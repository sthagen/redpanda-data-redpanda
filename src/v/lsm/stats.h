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

#include "container/chunked_vector.h"

#include <cstdint>
#include <string>

namespace lsm {

/// \brief Summary information about a single SST file.
struct file_info {
    uint64_t epoch;
    uint64_t id;
    uint64_t size_bytes;

    // NOTE: these are formatted, human-readable representations of the keys,
    // including additional metadata like sequence number and key type.
    std::string smallest_key_info;
    std::string largest_key_info;
};

/// \brief Per-level SST file information for an LSM tree database.
struct level_info {
    int32_t level_number;
    chunked_vector<file_info> files;
};

/// \brief Data statistics for an LSM tree database.
struct data_stats {
    size_t active_memtable_bytes;
    size_t immutable_memtable_bytes;
    size_t total_size_bytes;
    std::vector<level_info> levels;
};

} // namespace lsm
