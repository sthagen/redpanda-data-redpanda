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
#include "kafka/protocol/schemata/describe_redpanda_roles_response.h"
#include "security/role.h"
#include "security/types.h"

#include <fmt/format.h>

#include <stdexcept>

namespace kafka::details {

// Wire contract (DescribeRedpandaRoles role member): 0 = user, 1 = group.
inline security::role_member_type to_role_member_type(int8_t type) {
    switch (type) {
    case 0:
        return security::role_member_type::user;
    case 1:
        return security::role_member_type::group;
    default:
        throw std::runtime_error(
          fmt::format("Invalid role member type: {}", static_cast<int>(type)));
    }
}

inline int8_t to_wire_role_member_type(security::role_member_type type) {
    return static_cast<int8_t>(type);
}

inline security::role_with_members
to_role_with_members(const redpanda_role& role) {
    security::role::container_type members;
    for (const auto& m : role.members) {
        members.insert(
          security::role_member{to_role_member_type(m.member_type), m.name});
    }
    return security::role_with_members{
      .name = security::role_name{role.name},
      .role = security::role{std::move(members)}};
}

inline redpanda_role
to_wire_redpanda_role(const security::role_with_members& rwm) {
    redpanda_role out;
    out.name = rwm.name();
    for (const auto& m : rwm.role.members()) {
        out.members.push_back(
          redpanda_role_member{
            .member_type = to_wire_role_member_type(m.type()),
            .name = m.name()});
    }
    return out;
}

} // namespace kafka::details
