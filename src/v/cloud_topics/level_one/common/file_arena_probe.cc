/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/file_arena_probe.h"

#include "cloud_topics/level_one/common/file_arena.h"
#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

namespace cloud_topics::l1 {

void file_arena_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      prometheus_sanitize::metrics_name(
        fmt::format("cloud_topics::file_arena:{}", _ctx)),
      {
        sm::make_gauge(
          "shard_local_available",
          [this] {
              return _arena->_local_disk_bytes_reservable.available_units();
          },
          sm::description(
            "Number of bytes available in the shard-local pool, including "
            "outstanding reservations")),
        sm::make_gauge(
          "global_pool_available",
          [this] {
              if (ss::this_shard_id() == file_arena::manager_shard) {
                  if (_arena->_core0_arena_manager) {
                      return _arena->_core0_arena_manager
                        ->_disk_bytes_reservable.available_units();
                  }
              }
              return ssize_t{0};
          },
          sm::description(
            "Number of bytes available in the global pool, including "
            "outstanding reservations (expected to be empty on all shards "
            "except shard 0).")),
      });
}

} // namespace cloud_topics::l1
