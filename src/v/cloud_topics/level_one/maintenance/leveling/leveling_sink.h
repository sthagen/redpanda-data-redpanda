/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/maintenance/l1_object_sink.h"
#include "cloud_topics/level_one/maintenance/worker_probe.h"
#include "utils/prefix_logger.h"

namespace cloud_topics::l1 {

/// Sink for leveling jobs. Reuses L1 object building from l1_object_sink, but
/// commits via metastore::replace_objects() so that the rewrite does not touch
/// compaction state (cleaned ranges, tombstones).
class leveling_sink : public l1_object_sink {
public:
    leveling_sink(
      model::topic_id_partition,
      metastore::compaction_epoch,
      l1::io*,
      l1::metastore*,
      ss::abort_source&,
      config::binding<size_t> max_object_size,
      config::binding<size_t> commit_interval_bytes,
      size_t upload_part_size,
      compaction_worker_probe&,
      prefix_logger&,
      object_builder::options = {});

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source&) final;

    ss::future<> finalize(bool success) final;

protected:
    // Commits the builder's finished objects via replace_objects() at the
    // job's pinned compaction epoch. Unlike compaction's partial commits,
    // this does not bump the epoch: leveling must never fence a concurrent
    // compaction job, while compaction's epoch bumps fence this job's
    // remaining commits. Throws on failure, aborting the run; extents
    // replaced by earlier commits remain durable.
    ss::future<>
      commit_objects(std::unique_ptr<metastore::object_metadata_builder>) final;

private:
    // The expected compaction epoch for the log.
    const metastore::compaction_epoch _expected_compaction_epoch;

    compaction_worker_probe& _probe;

    // Total undersized input extents across this job's leveling ranges,
    // summed in `initialize()`. Compared against _committed_objects at
    // finalize to report the net extent-count reduction.
    size_t _input_extents{0};

    // Output objects committed over the job's lifetime, across all commits.
    uint64_t _committed_objects{0};

    // Number of partial commits this job has landed.
    uint64_t _partial_commits{0};
};

} // namespace cloud_topics::l1
