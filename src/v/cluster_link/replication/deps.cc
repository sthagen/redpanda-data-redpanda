/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/deps.h"

namespace cluster_link::replication {
fmt::iterator data_source::source_partition_offsets_report::format_to(
  fmt::iterator it) const {
    return fmt::format_to(
      it,
      "start_offset: {}, hwm: {}, lso: {}, update_time: {}",
      source_start_offset,
      source_hwm,
      source_lso,
      update_time.time_since_epoch().count());
}
} // namespace cluster_link::replication
