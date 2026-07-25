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

#include <numeric>

namespace cloud_topics::l1 {

leveling_sink::leveling_sink(
  model::topic_id_partition tp,
  metastore::compaction_epoch epoch,
  io* io,
  metastore* metastore,
  ss::abort_source& as,
  config::binding<size_t> max_object_size,
  config::binding<size_t> commit_interval_bytes,
  size_t upload_part_size,
  compaction_worker_probe& probe,
  prefix_logger& ctxlog,
  object_builder::options opts)
  : l1_object_sink(
      tp,
      io,
      metastore,
      as,
      std::move(max_object_size),
      std::move(commit_interval_bytes),
      upload_part_size,
      ctxlog,
      std::move(opts))
  , _expected_compaction_epoch(epoch)
  , _probe(probe) {}

ss::future<bool>
leveling_sink::initialize(compaction::sliding_window_reducer::source& src) {
    auto& lv_src = static_cast<leveling_source&>(src);

    if (lv_src._leveling_ranges.empty()) {
        co_return false;
    }

    _input_extents = std::accumulate(
      lv_src._leveling_ranges.begin(),
      lv_src._leveling_ranges.end(),
      size_t{0},
      [](size_t acc, const auto& range) { return acc + range.extent_count; });

    vlog(
      _ctxlog.debug,
      "Initialized leveling job with {} leveling ranges",
      lv_src._leveling_ranges.size());

    co_return true;
}

ss::future<> leveling_sink::commit_objects(
  std::unique_ptr<metastore::object_metadata_builder> builder) {
    metastore::replace_epoch_map_t epoch_map;
    epoch_map.emplace(_tp, _expected_compaction_epoch);
    auto replace_res = co_await l1::retry_metastore_op_with_default_rtc(
      [this, &builder, &epoch_map]() {
          return _metastore->replace_objects(*builder, epoch_map);
      },
      _as);
    if (!replace_res.has_value()) {
        _probe.add_leveling_objects_rejected(_pending_objects);
        _probe.add_leveling_bytes_rejected(_pending_bytes);
        auto err = replace_res.error();
        throw std::runtime_error(
          fmt::format(
            "[{}] aborting leveling: object replacement at epoch {} failed: "
            "{}",
            _tp,
            _expected_compaction_epoch,
            err));
    }
    _probe.add_leveling_objects_committed(_pending_objects);
    _probe.add_leveling_bytes_committed(_pending_bytes);
    _committed_objects += _pending_objects;
    ++_partial_commits;
    vlog(
      _ctxlog.debug,
      "Partial leveling commit of {} object(s) landed at epoch {}",
      _pending_objects,
      _expected_compaction_epoch);
}

ss::future<> leveling_sink::finalize(bool success) {
    co_await finalize_inflight(success);

    if (!success) {
        vlog(
          _ctxlog.warn,
          "Skipping leveling finalization; {}",
          _committed_objects == 0
            ? fmt::format("no objects had been committed")
            : fmt::format(
                "{} object(s) already committed", _committed_objects));
        co_return;
    }

    if (_committed_objects == 0) {
        vlog(
          _ctxlog.debug, "Finalized leveling without any committed objects.");
        co_return;
    }

    auto reclaimed = _input_extents > _committed_objects
                       ? _input_extents - _committed_objects
                       : size_t{0};
    _probe.add_leveling_extents_reclaimed(reclaimed);
    vlog(
      _ctxlog.info,
      "Finalized leveling with {} extents reclaimed ({}->{}) in {} partial "
      "commit(s)",
      reclaimed,
      _input_extents,
      _committed_objects,
      _partial_commits);
}

} // namespace cloud_topics::l1
