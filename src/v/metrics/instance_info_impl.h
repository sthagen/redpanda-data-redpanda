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

// Implementation detail of the instance-info table, split out of the public
// interface (instance_info.h, which holds just the shared types). Declares the
// generated table and the lookup helpers over it, used by the generated table
// definition, the detector's name resolution, and the structural tests.

#include "metrics/instance_info.h"

#include <optional>
#include <span>
#include <string_view>

namespace instance_info {

/// One row of the generated table: an instance type and its capacity info.
struct table_entry {
    cloud_provider provider;
    std::string_view name;
    capacity_info capacity;
};

/// The full generated table, sorted by (provider, family, size).
std::span<const table_entry> instance_table();

/// Look up capacity info for a cloud instance type (e.g. "m6id.4xlarge",
/// "n2d-standard-16", "Standard_D16ds_v5"). Returns nullopt when the
/// (provider, name) pair is not in the generated table.
std::optional<capacity_info>
lookup(cloud_provider provider, std::string_view instance_type);

/// Look up capacity info by instance-type name alone, across all providers.
/// Returns the first match, or nullopt if none.
std::optional<capacity_info> lookup_by_name(std::string_view instance_type);

} // namespace instance_info
