/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/leveling/leveling_sink.h"

#include "cloud_topics/level_one/maintenance/leveling/leveling_source.h"
#include "cloud_topics/level_one/metastore/retry.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

leveling_sink::leveling_sink(
  model::topic_id_partition tp,
  metastore::compaction_epoch epoch,
  io* io,
  metastore* metastore,
  ss::abort_source& as,
  config::binding<size_t> max_object_size,
  size_t upload_part_size,
  prefix_logger& ctxlog,
  object_builder::options opts)
  : l1_object_sink(
      tp,
      io,
      metastore,
      as,
      std::move(max_object_size),
      upload_part_size,
      ctxlog,
      std::move(opts))
  , _expected_compaction_epoch(epoch) {}

ss::future<bool>
leveling_sink::initialize(compaction::sliding_window_reducer::source& src) {
    auto& lv_src = static_cast<leveling_source&>(src);

    if (lv_src._leveling_ranges.empty()) {
        co_return false;
    }

    co_await init_metadata_builder();

    vlog(
      _ctxlog.debug,
      "Initialized leveling job with {} leveling ranges",
      lv_src._leveling_ranges.size());

    co_return true;
}

ss::future<ss::stop_iteration>
leveling_sink::operator()(model::record_batch b) {
    auto next_offset = model::offset_cast(b.base_offset());
    auto prev_offset = kafka::prev_offset(next_offset);

    if (
      _inflight_object
      && _inflight_object->builder->file_size() >= _max_object_size()) {
        co_await flush(prev_offset);
    }

    if (!_inflight_object) {
        co_await initialize_builder(next_offset);
    }

    co_await _inflight_object->builder->add_batch(std::move(b));

    co_return ss::stop_iteration::no;
}

ss::future<> leveling_sink::finalize(bool success) {
    auto should_commit = co_await finalize_inflight(success);
    if (!should_commit) {
        co_return;
    }

    metastore::replace_epoch_map_t epoch_map;
    epoch_map.emplace(_tp, _expected_compaction_epoch);
    auto replace_res = co_await l1::retry_metastore_op_with_default_rtc(
      [this, &epoch_map]() {
          return _metastore->replace_objects(*_metadata_builder, epoch_map);
      },
      _as);
    if (replace_res.has_value()) {
        vlog(_ctxlog.info, "Finalized leveling");
    } else {
        vlog(
          _ctxlog.warn,
          "Could not commit object replacement during leveling: {}.",
          replace_res.error());
    }
}

} // namespace cloud_topics::l1
