/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/compaction/compaction_sink.h"

#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/maintenance/compaction/compaction_source.h"
#include "cloud_topics/level_one/maintenance/l1_object_sink.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "cloud_topics/level_one/metastore/retry.h"
#include "compaction/reducer.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "utils/prefix_logger.h"

#include <stdexcept>

namespace cloud_topics::l1 {

namespace {

// Computes the ranges that may be marked as having all their tombstones
// removed, based on the `metastore`'s initial `removable_tombstone_ranges`
// response, and the extents that were processed by the `sink`. The returned
// `offset_interval_set` will be used for the compaction update to the
// `metastore`.
offset_interval_set get_removed_tombstone_ranges(
  const offset_interval_set& removable_tombstone_ranges,
  const offset_interval_set& processed_extents) {
    offset_interval_set removed_tombstone_ranges;
    auto stream = removable_tombstone_ranges.make_stream();
    while (stream.has_next()) {
        auto i = stream.next();
        if (processed_extents.covers(i.base_offset, i.last_offset)) {
            removed_tombstone_ranges.insert(i.base_offset, i.last_offset);
        }
    }
    return removed_tombstone_ranges;
}

// Computes the ranges that may be marked as clean, based on the dirty ranges
// that were processed by the `compaction_source`, and the extents that were
// processed by the `sink`. The returned `chunked_vector` of `cleaned_ranges`
// will be used for the compaction update to the `metastore`.
chunked_vector<metastore::compaction_update::cleaned_range>
get_new_cleaned_ranges(
  const chunked_vector<metastore::compaction_update::cleaned_range>&
    maybe_cleaned_ranges,
  const offset_interval_set& processed_extents,
  kafka::offset start_offset) {
    chunked_vector<metastore::compaction_update::cleaned_range>
      new_cleaned_ranges;
    new_cleaned_ranges.reserve(maybe_cleaned_ranges.size());
    for (const auto& cleaned_range : maybe_cleaned_ranges) {
        if (processed_extents.covers(start_offset, cleaned_range.last_offset)) {
            new_cleaned_ranges.push_back(cleaned_range);
        }
    }

    new_cleaned_ranges.shrink_to_fit();
    return new_cleaned_ranges;
}

// Computes the new min_allowed_local_threshold floor from the ranges compaction
// just cleaned: the exclusive lower bound for local reads, i.e. one past the
// highest cleaned offset. Returns nullopt when nothing was cleaned.
// `new_cleaned_ranges` is ordered by descending offset (compaction indexes the
// head of the log first), so the front range carries the max last_offset.
std::optional<kafka::offset> get_max_cleaned_offset(
  const chunked_vector<metastore::compaction_update::cleaned_range>&
    new_cleaned_ranges) {
    if (new_cleaned_ranges.empty()) {
        return std::nullopt;
    }
    return kafka::next_offset(new_cleaned_ranges.front().last_offset);
}

} // namespace

compaction_sink::compaction_sink(
  model::topic_id_partition tp,
  const chunked_vector<offset_interval_set::interval>& dirty_range_intervals,
  const offset_interval_set& removable_tombstone_ranges,
  metastore::compaction_epoch expected_compaction_epoch,
  kafka::offset start_offset,
  io* io,
  metastore* metastore,
  ss::abort_source& as,
  config::binding<size_t> max_object_size,
  config::binding<size_t> commit_interval_bytes,
  size_t upload_part_size,
  compaction_worker_probe& probe,
  prefix_logger& ctxlog,
  object_builder::options opts,
  cloud_topics::level_zero_notifier* notifier)
  : l1_object_sink(
      std::move(tp),
      io,
      metastore,
      as,
      std::move(max_object_size),
      std::move(commit_interval_bytes),
      upload_part_size,
      ctxlog,
      std::move(opts))
  , _dirty_range_intervals(dirty_range_intervals)
  , _removable_tombstone_ranges(removable_tombstone_ranges)
  , _current_epoch(expected_compaction_epoch)
  , _start_offset(start_offset)
  , _probe(probe)
  , _notifier(notifier) {}

ss::future<bool>
compaction_sink::initialize(compaction::sliding_window_reducer::source& src) {
    auto& ct_src = static_cast<compaction_source&>(src);

    bool has_removable_tombstones = !_removable_tombstone_ranges.empty();
    bool has_dirty_ranges = !_dirty_range_intervals.empty();
    bool should_compact = has_removable_tombstones || has_dirty_ranges;

    if (!should_compact) {
        co_return false;
    }

    auto& new_cleaned_ranges = ct_src._new_cleaned_ranges;
    new_cleaned_ranges.shrink_to_fit();
    _new_cleaned_ranges = std::move(new_cleaned_ranges);

    vlog(
      _ctxlog.debug,
      "Built compaction map with {} keys (max allowed {})",
      ct_src._map->size(),
      ct_src._map->capacity());

    co_return true;
}

ss::future<std::expected<void, metastore::errc>>
compaction_sink::do_compact_objects(
  const metastore::object_metadata_builder& builder,
  metastore::compaction_map_t compact_map) {
    co_return co_await l1::retry_metastore_op_with_default_rtc(
      [this, &builder, &compact_map]() {
          return _metastore->compact_objects(builder, compact_map);
      },
      _as);
}

metastore::compaction_update compaction_sink::make_compaction_update(
  chunked_vector<metastore::compaction_update::cleaned_range>
    new_cleaned_ranges,
  offset_interval_set removed_tombstone_ranges,
  metastore::compaction_epoch expected_epoch) {
    auto cleaned_at = new_cleaned_ranges.empty()
                          && removed_tombstone_ranges.empty()
                        ? model::timestamp::missing()
                        : model::timestamp::now();
    return metastore::compaction_update{
      .new_cleaned_ranges = std::move(new_cleaned_ranges),
      .removed_tombstones_ranges = std::move(removed_tombstone_ranges),
      .cleaned_at = cleaned_at,
      .expected_compaction_epoch = expected_epoch};
}

metastore::compaction_map_t compaction_sink::make_compaction_map(
  metastore::compaction_update update) const {
    metastore::compaction_map_t compact_map;
    compact_map.emplace(_tp, std::move(update));
    return compact_map;
}

ss::future<bool>
compaction_sink::advance_local_threshold_floor(kafka::offset new_floor) {
    // _notifier is null only in tests, where the notification is a no-op.
    if (_notifier == nullptr) {
        co_return true;
    }
    vlog(
      _ctxlog.debug,
      "Compaction advancing min_allowed_local_threshold to {}",
      new_floor);
    auto res = co_await _notifier->set_min_allowed_local_threshold(
      _tp, new_floor);
    if (!res.has_value()) {
        vlog(
          _ctxlog.warn,
          "Failed to advance min_allowed_local_threshold to {} ({})",
          new_floor,
          res.error());
        co_return false;
    }
    co_return true;
}

ss::future<> compaction_sink::commit_objects(
  std::unique_ptr<metastore::object_metadata_builder> builder) {
    // Data is about to be durably replaced. Advance the partition's
    // min_allowed_local_threshold floor before the first partial commit so
    // that local reads cannot serve records this job removes, mirroring the
    // pre-commit floor advance of the single-request path. The floor is
    // advanced once, to the top of the job's candidate cleaned ranges (the
    // final cleaned ranges are a subset).
    if (!_floor_advanced) {
        if (
          auto new_floor = get_max_cleaned_offset(_new_cleaned_ranges);
          new_floor.has_value()) {
            if (!co_await advance_local_threshold_floor(*new_floor)) {
                throw std::runtime_error(
                  fmt::format(
                    "[{}] aborting compaction: could not advance "
                    "min_allowed_local_threshold",
                    _tp));
            }
        }
        _floor_advanced = true;
    }

    // Partial compaction commit: the finished object(s) with an empty
    // compaction update, which validates and bumps the compaction epoch.
    auto commit_res = co_await do_compact_objects(
      *builder,
      make_compaction_map(make_compaction_update({}, {}, _current_epoch)));
    if (!commit_res.has_value()) {
        _probe.add_compaction_objects_rejected(_pending_objects);
        _probe.add_compaction_bytes_rejected(_pending_bytes);
        auto err = commit_res.error();
        throw std::runtime_error(
          fmt::format(
            "[{}] aborting compaction: partial commit at epoch {} failed: "
            "{}",
            _tp,
            _current_epoch,
            err));
    }
    _probe.add_compaction_objects_committed(_pending_objects);
    _probe.add_compaction_bytes_committed(_pending_bytes);
    _committed_objects += _pending_objects;
    ++_partial_commits;
    _current_epoch = metastore::compaction_epoch{_current_epoch() + 1};
    vlog(
      _ctxlog.debug,
      "Partial compaction commit of {} object(s) landed; compaction epoch "
      "advanced to {}",
      _pending_objects,
      _current_epoch);
}

ss::future<> compaction_sink::finalize(bool success) {
    co_await finalize_inflight(success);

    if (!success) {
        vlog(
          _ctxlog.warn,
          "Skipping compaction metadata commit; {}",
          _partial_commits == 0
            ? fmt::format("no partial commits had landed")
            : fmt::format(
                "{} object(s) already committed in {} partial commit(s)",
                _committed_objects,
                _partial_commits));
        co_return;
    }
    auto removed_tombstone_ranges = get_removed_tombstone_ranges(
      _removable_tombstone_ranges, _processed_extents);
    auto new_cleaned_ranges = get_new_cleaned_ranges(
      _new_cleaned_ranges, _processed_extents, _start_offset);
    if (new_cleaned_ranges.empty() && removed_tombstone_ranges.empty()) {
        vlog(
          _ctxlog.info,
          "Finalized job without a compaction metadata update; {} extent(s) "
          "compacted into {} object(s) in {} partial commit(s)",
          _processed_extent_count,
          _committed_objects,
          _partial_commits);
        co_return;
    }

    // The min_allowed_local_threshold floor was already advanced before the
    // first partial commit. Record the job's compaction metadata against
    // the epoch its own partial commits advanced.
    auto compaction_update = make_compaction_update(
      std::move(new_cleaned_ranges),
      std::move(removed_tombstone_ranges),
      _current_epoch);
    auto compaction_update_str = fmt::format("{}", compaction_update);
    auto compact_map = make_compaction_map(std::move(compaction_update));
    auto commit_res = co_await l1::retry_metastore_op_with_default_rtc(
      [this, &compact_map]() {
          return _metastore->commit_compaction_metadata(compact_map);
      },
      _as);
    if (commit_res.has_value()) {
        vlog(
          _ctxlog.info,
          "Finalized job with compaction metadata update: {} ({} extent(s) "
          "compacted into {} object(s) in {} partial commit(s))",
          compaction_update_str,
          _processed_extent_count,
          _committed_objects,
          _partial_commits);
    } else {
        vlog(
          _ctxlog.warn,
          "Could not commit compaction metadata update {}: {}; {} extent(s) "
          "compacted into {} object(s) in {} partial commit(s)",
          compaction_update_str,
          commit_res.error(),
          _processed_extent_count,
          _committed_objects,
          _partial_commits);
    }
}

} // namespace cloud_topics::l1
