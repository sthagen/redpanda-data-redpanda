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
#include "kafka/server/handlers/describe_redpanda_roles.h"

#include "container/chunked_hash_map.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "security/acl.h"
#include "security/role_store.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <ranges>

namespace kafka {

namespace {

redpanda_role to_wire(const security::role_with_members& rwm) {
    redpanda_role out;
    out.name = rwm.name();
    for (const auto& m : rwm.role.members()) {
        out.members.push_back(
          redpanda_role_member{
            .member_type = static_cast<int8_t>(m.type()), .name = m.name()});
    }
    return out;
}

} // namespace

template<>
ss::future<response_ptr> describe_redpanda_roles_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    describe_redpanda_roles_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    auto authz = ctx.authorized(
      security::acl_operation::describe, security::default_cluster_name);

    describe_redpanda_roles_response resp;

    if (!ctx.audit()) {
        resp.data.error_code = error_code::broker_not_available;
        resp.data.error_message = "Broker not available - audit system failure";
        co_return co_await ctx.respond(std::move(resp));
    }

    if (!authz) {
        resp.data.error_code = error_code::cluster_authorization_failed;
        co_return co_await ctx.respond(std::move(resp));
    }

    auto& roles = ctx.role_store();

    resp.data.error_code = error_code::none;

    // Null or empty filters mean "all roles"; otherwise return only the named
    // roles. Names that do not exist simply do not match and are skipped.
    const auto& filters = request.data.role_name_filters;
    const bool all_roles = !filters.has_value() || filters->empty();
    chunked_hash_set<security::role_name> wanted;
    if (!all_roles) {
        wanted = filters.value() | std::views::transform([](const auto& f) {
                     return security::role_name{f.name};
                 })
                 | std::ranges::to<chunked_hash_set<security::role_name>>();
    }
    const auto matches = [&](const security::role_name& name) {
        return all_roles || wanted.contains(name);
    };

    resp.data.roles = roles.roles_with_members(matches)
                      | std::views::transform(to_wire)
                      | std::ranges::to<chunked_vector<redpanda_role>>();

    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
