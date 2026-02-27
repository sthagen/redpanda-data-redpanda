/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "utils/log_hist.h"

#include <cstdint>

namespace lsm {

struct probe {
    uint64_t throttled_writes = 0;
    uint64_t stalled_writes = 0;
    uint64_t table_cache_hit = 0;
    uint64_t table_cache_miss = 0;
    uint64_t block_cache_hit = 0;
    uint64_t block_cache_miss = 0;
    log_hist_internal compaction_latency;
    log_hist_internal flush_latency;
    log_hist_internal manifest_write_latency;
};

} // namespace lsm
