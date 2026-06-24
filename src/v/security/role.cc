/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/role.h"

#include "container/chunked_hash_map.h"
#include "security/role_store.h"

#include <algorithm>
#include <ranges>
#include <utility>

namespace security {

namespace {
role_member_type member_type_for_principal_type(security::principal_type p) {
    switch (p) {
    case security::principal_type::user:
        return role_member_type::user;
    case security::principal_type::group:
        return role_member_type::group;
    case security::principal_type::ephemeral_user:
    case security::principal_type::role:
        vunreachable("Invalid principal_type {{{}}} for role membership", p);
    }
    __builtin_unreachable();
}
} // namespace

role_member_view::role_member_view(const role_member& m)
  : _type(m.type())
  , _name(m.name()) {}

role_member_view
role_member_view::from_principal(const security::acl_principal& p) {
    return {member_type_for_principal_type(p.type()), p.name_view()};
}

role_member role_member::from_principal(const security::acl_principal& p) {
    return role_member{role_member_view::from_principal(p)};
}

security::acl_principal role::to_principal(std::string_view role_name) {
    return {
      security::principal_type::role, {role_name.data(), role_name.size()}};
}

security::acl_principal_view
role::to_principal_view(std::string_view role_name) {
    return {security::principal_type::role, role_name};
}

std::optional<role> role_store::get(const role_name& name) const {
    if (!_roles.contains(name)) {
        return std::nullopt;
    }
    auto member_rng = _members_store
                      | std::views::filter(
                        [&name](const members_store_type::value_type& e) {
                            return e.second.contains(role_name_view{name});
                        })
                      | std::views::keys;
    return role{{member_rng.begin(), member_rng.end()}};
}

bool role_store::remove(const role_name& name) {
    std::ranges::for_each(
      _members_store, [&name](members_store_type::value_type& e) {
          e.second.erase(role_name_view{name});
      });
    return _roles.erase(name) > 0;
}

auto role_store::range(std::function<bool(const role_accessor&)>&& pred) const
  -> range_query_container_type {
    auto rng = _roles | std::views::transform([this](const auto& e) {
                   return std::make_pair(
                     role_name_view{e}, [this]() -> const members_store_type& {
                         return _members_store;
                     });
               })
               | std::views::filter(std::move(pred)) | std::views::keys;
    return {rng.begin(), rng.end()};
}

chunked_vector<role_with_members> role_store::roles_with_members(
  const std::function<bool(const role_name&)>& pred) const {
    // Seed with every matching role so roles with no members are still
    // returned (they don't appear in the member store).
    chunked_hash_map<role_name_view, role::container_type> by_role;
    by_role.reserve(_roles.size());
    for (const auto& rn : _roles) {
        if (pred(rn)) {
            by_role.try_emplace(role_name_view{rn});
        }
    }

    for (const auto& [member, role_names] : _members_store) {
        for (const auto& role_name : role_names) {
            if (auto it = by_role.find(role_name); it != by_role.end()) {
                it->second.insert(member);
            }
        }
    }

    chunked_vector<role_with_members> result;
    result.reserve(by_role.size());
    for (auto& [name_view, members] : by_role) {
        result.push_back(
          role_with_members{
            .name = role_name{ss::sstring{name_view()}},
            .role = role{std::move(members)}});
    }
    return result;
}

} // namespace security
