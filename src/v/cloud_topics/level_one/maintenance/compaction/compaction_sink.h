/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/maintenance/l1_object_sink.h"
#include "cloud_topics/level_one/maintenance/worker_probe.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_zero/notifier/level_zero_notifier.h"
#include "config/property.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "utils/prefix_logger.h"

namespace cloud_topics::l1 {

class compaction_sink : public l1_object_sink {
public:
    compaction_sink(
      model::topic_id_partition,
      const chunked_vector<offset_interval_set::interval>&,
      const offset_interval_set&,
      metastore::compaction_epoch,
      kafka::offset,
      l1::io*,
      l1::metastore*,
      ss::abort_source&,
      config::binding<size_t>,
      config::binding<size_t>,
      size_t,
      compaction_worker_probe&,
      prefix_logger&,
      object_builder::options = {},
      cloud_topics::level_zero_notifier* = nullptr);

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source&) final;

    ss::future<> finalize(bool success) final;

protected:
    // Commits the builder's finished objects as a partial compaction
    // commit: a compact_objects() carrying the objects and an empty
    // compaction update, which validates and bumps the compaction epoch.
    // Bumping the epoch on every batch keeps the job's progress durable and
    // fences stale rewriters (straggler compaction jobs, leveling jobs with
    // old read snapshots) for the remainder of the run. Throws on failure,
    // aborting the run.
    ss::future<>
      commit_objects(std::unique_ptr<metastore::object_metadata_builder>) final;

private:
    // Makes a `compact_objects()` request to the `metastore`, using the
    // provided (potentially empty) `compaction_map_t` as the metastore
    // compaction update.
    ss::future<std::expected<void, metastore::errc>> do_compact_objects(
      const metastore::object_metadata_builder&, metastore::compaction_map_t);

    // Advances the partition's min_allowed_local_threshold floor via the
    // notifier. Returns false if the floor could not be advanced, in which
    // case no compaction data or metadata may be committed.
    ss::future<bool> advance_local_threshold_floor(kafka::offset new_floor);

    // Builds a compaction update carrying the given ranges at the given
    // expected epoch.
    static metastore::compaction_update make_compaction_update(
      chunked_vector<metastore::compaction_update::cleaned_range>,
      offset_interval_set,
      metastore::compaction_epoch);

    // Wraps the given update into a single-partition compaction map.
    metastore::compaction_map_t
      make_compaction_map(metastore::compaction_update) const;

private:
    // Offset ranges for the contained `topic_id_partition` obtained from the
    // metastore.
    using interval_vec = chunked_vector<offset_interval_set::interval>;
    const interval_vec& _dirty_range_intervals;
    const offset_interval_set& _removable_tombstone_ranges;

    // The compaction epoch the job's own partial commits have advanced the
    // partition to. Each successful partial commit bumps this by one; the
    // final metadata-only commit validates against (and bumps) it.
    metastore::compaction_epoch _current_epoch;

    // Number of partial commits this job has landed, and the output
    // objects they committed. Reported against the processed extent count
    // at finalize to show the job's extent-count reduction.
    uint64_t _partial_commits{0};
    uint64_t _committed_objects{0};

    // Whether the min_allowed_local_threshold floor was advanced for this
    // job (advanced once, before the first partial commit removes any
    // data).
    bool _floor_advanced{false};

    // The start offset of the log.
    kafka::offset _start_offset;

    compaction_worker_probe& _probe;

    // Dirty ranges returned by the `metastore` that were indexed during
    // `map_deduplication_iteration`.
    chunked_vector<metastore::compaction_update::cleaned_range>
      _new_cleaned_ranges;

    // Receives the new min_allowed_local_threshold floor (keyed by the
    // partition's topic_id_partition) after a successful finalize(). May be
    // null. The notification is not sent if this is the case (tests).
    cloud_topics::level_zero_notifier* _notifier;

private:
    friend class throwing_compaction_sink;
};

} // namespace cloud_topics::l1
