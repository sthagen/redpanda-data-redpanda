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
      size_t upload_part_size,
      prefix_logger&,
      object_builder::options = {});

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source&) final;

    ss::future<ss::stop_iteration> operator()(model::record_batch) final;

    ss::future<> finalize(bool success) final;

private:
    // The expected compaction epoch for the log.
    const metastore::compaction_epoch _expected_compaction_epoch;
};

} // namespace cloud_topics::l1
