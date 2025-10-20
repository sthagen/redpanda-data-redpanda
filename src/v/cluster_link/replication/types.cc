/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/types.h"

namespace cluster_link::replication {
fmt::iterator partition_offsets_report::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{source_hwm: {}, source_lso: {}, update_time (ms since epoch): {}ms, "
      "shadow_hwm: {}}}",
      source_hwm,
      source_lso,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        update_time.time_since_epoch()),
      shadow_hwm);
}
} // namespace cluster_link::replication
