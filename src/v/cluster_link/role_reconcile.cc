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

#include "cluster_link/role_reconcile.h"

#include "container/chunked_hash_map.h"

namespace cluster_link {

role_changes reconcile_roles(
  chunked_vector<security::role_with_members> source_selected,
  chunked_vector<security::role_with_members> shadow_selected) {
    auto to_map = [](chunked_vector<security::role_with_members> roles) {
        chunked_hash_map<security::role_name, security::role> m;
        m.reserve(roles.size());
        for (auto& r : roles) {
            m.emplace(std::move(r.name), std::move(r.role));
        }
        return m;
    };
    auto source = to_map(std::move(source_selected));
    auto shadow = to_map(std::move(shadow_selected));

    role_changes changes;
    for (auto& [src_name, src_role] : source) {
        auto it = shadow.find(src_name);
        if (it == shadow.end()) {
            changes.to_create.push_back(
              {.name = src_name, .role = std::move(src_role)});
        } else if (src_role.members() != it->second.members()) {
            changes.to_update.push_back(
              {.name = src_name, .role = std::move(src_role)});
        }
    }

    for (const auto& [shadow_name, _] : shadow) {
        if (!source.contains(shadow_name)) {
            changes.to_delete.push_back(shadow_name);
        }
    }

    return changes;
}

} // namespace cluster_link
