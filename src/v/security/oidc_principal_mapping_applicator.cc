/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/oidc_principal_mapping_applicator.h"

#include "security/logger.h"
#include "security/oidc_principal_mapping.h"

#include <boost/algorithm/string.hpp>
#include <rapidjson/pointer.h>

namespace security::oidc {

namespace {
std::optional<chunked_vector<std::string_view>>
get_group_claim(const json::Pointer& p, const jwt& jwt) {
    auto list_claim = jwt.claim<chunked_vector<std::string_view>>(p);
    if (list_claim) {
        vlog(
          seclog.trace,
          "Group claim found as string list: {}",
          list_claim.value());
        return std::move(list_claim).value();
    }

    auto string_claim = jwt.claim<std::string_view>(p);
    if (!string_claim) {
        return std::nullopt;
    }

    vlog(seclog.trace, "Group claim found as string: {}", string_claim.value());

    chunked_vector<std::string_view> string_claim_parsed;
    boost::split(
      string_claim_parsed, string_claim.value(), boost::is_any_of(","));
    return string_claim_parsed;
}

acl_principal
apply_nested_group_policy(std::string_view user, nested_group_behavior b) {
    switch (b) {
    case security::oidc::nested_group_behavior::none:
        return acl_principal{principal_type::group, ss::sstring{user}};
    case security::oidc::nested_group_behavior::suffix: {
        auto pos = user.find_last_of('/');
        if (pos == std::string_view::npos) {
            return acl_principal{principal_type::group, ss::sstring{user}};
        }
        return acl_principal{
          principal_type::group, ss::sstring{user.substr(pos + 1)}};
    }
    }
}
} // namespace

result<acl_principal> principal_mapping_rule_apply(
  const principal_mapping_rule& mapping, const jwt& jwt) {
    auto claim = jwt.claim<std::string_view>(mapping.claim());
    if (claim.value_or("").empty()) {
        return errc::jwt_invalid_principal;
    }

    auto principal = mapping.mapping().apply(claim.value());
    if (principal.value_or("").empty()) {
        return errc::jwt_invalid_principal;
    }

    return {principal_type::user, std::move(principal).value()};
}

result<chunked_vector<acl_principal>>
group_policy_apply(const group_claim_policy& policy, const jwt& jwt) {
    auto group_claim = get_group_claim(policy.group_pointer(), jwt);
    if (!group_claim) {
        return chunked_vector<acl_principal>{};
    }
    return chunked_vector<acl_principal>(
      std::from_range,
      std::views::transform(
        *group_claim,
        [behavior = policy.nested_behavior()](std::string_view g) {
            return apply_nested_group_policy(g, behavior);
        }));
}

} // namespace security::oidc
