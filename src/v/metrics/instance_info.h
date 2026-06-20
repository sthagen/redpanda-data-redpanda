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

#include "base/format_to.h"

#include <cstdint>
#include <string_view>

namespace instance_info {

enum class cloud_provider : uint8_t { aws = 0, gcp = 1, azure = 2 };

constexpr std::string_view to_string_view(cloud_provider p) {
    switch (p) {
    case cloud_provider::aws:
        return "aws";
    case cloud_provider::gcp:
        return "gcp";
    case cloud_provider::azure:
        return "azure";
    }
    // The switch is exhaustive over the enum; guard a corrupt/out-of-range
    // value with a sentinel rather than risking UB.
    return "unknown";
}

inline fmt::iterator format_to(cloud_provider p, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(p));
}

/// Static information about a cloud host, derived from its instance type via a
/// generated lookup table (see instance_info_table.cc).
///
/// For the most part these are nominal vendor advertised values, but in general
/// we have confirmed that the hosts do reach these values in the right
/// conditions, but there are some exceptions.
struct capacity_info {
    /// Number of virtual CPUs (hardware threads) on the host.
    uint32_t vcpus;
    /// Physical memory in bytes.
    uint64_t memory_bytes;
    /// Total local (instance-attached) SSD capacity in bytes.
    uint64_t disk_bytes;
    /// Advertised network bandwidth in bytes/sec.
    /// Generally speaking network is full-duplex so in theory
    /// this figure can be achieved in both directions simultaneously.
    uint64_t network_bytes_per_sec;
    /// Local-SSD read IOPS.
    uint64_t read_iops;
    /// Local-SSD write IOPS.
    uint64_t write_iops;
};

} // namespace instance_info
