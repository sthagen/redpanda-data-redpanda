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

#include "metrics/instance_info.h"

#include "metrics/instance_info_impl.h"

#include <algorithm>
#include <optional>
#include <string_view>

namespace instance_info {

std::optional<capacity_info>
lookup(cloud_provider provider, std::string_view instance_type) {
    // Linear scan: the table is only consulted once (at startup), so there is
    // no need to keep it in a search-friendly order, leaving it free to be
    // sorted for readability instead.
    auto table = instance_table();
    const auto it = std::ranges::find_if(table, [&](const table_entry& e) {
        return e.provider == provider && e.name == instance_type;
    });
    if (it != table.end()) {
        return it->capacity;
    }
    return std::nullopt;
}

std::optional<capacity_info> lookup_by_name(std::string_view instance_type) {
    auto table = instance_table();
    const auto it = std::ranges::find(table, instance_type, &table_entry::name);
    if (it != table.end()) {
        return it->capacity;
    }
    return std::nullopt;
}

} // namespace instance_info
