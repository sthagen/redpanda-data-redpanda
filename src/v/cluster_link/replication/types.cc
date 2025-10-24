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

#include <algorithm>
#include <functional>
#include <ranges>

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

fetch_counters fetch_counters::from_fetch_data(const fetch_data& data) {
    fetch_counters ret;
    ret.n_bytes = data.units.count();
    ret.n_records = std::ranges::fold_left(
      data.batches | std::views::transform(&model::record_batch::record_count),
      int64_t{},
      std::plus<int64_t>{});
    return ret;
}
} // namespace cluster_link::replication
