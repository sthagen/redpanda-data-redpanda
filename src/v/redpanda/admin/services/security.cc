/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/admin/services/security.h"

#include "cluster/controller.h"
#include "cluster/security_frontend.h"
#include "kafka/server/server.h"
#include "redpanda/admin/proxy/context.h"
#include "redpanda/admin/services/utils.h"
#include "security/oidc_authenticator.h"
#include "security/oidc_service.h"
#include "security/request_auth.h"
#include "security/role.h"
#include "security/role_store.h"
#include "security/scram_algorithm.h"

#include <algorithm>

namespace admin {

namespace {

// Timeout for role operations
constexpr std::chrono::seconds security_operation_timeout{5};

// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger securitylog{"admin_api_server/security_service"};

} // namespace

namespace internal {

void validate_role_name(const ss::sstring& role_name) {
    try {
        validate_no_control(role_name);
    } catch (const control_character_present_exception& e) {
        vlog(securitylog.warn, "Role name contains invalid characters");
        throw serde::pb::rpc::invalid_argument_exception(
          "Role name contains invalid characters");
    }

    if (!security::validate_scram_username(role_name)) {
        vlog(securitylog.warn, "Invalid role name {{{}}}", role_name);
        throw serde::pb::rpc::invalid_argument_exception(
          ssx::sformat("Invalid role name {{{}}}", role_name));
    }
}

void validate_pb_role_member(const proto::admin::role_member& pb_member) {
    auto validate_member = [&](const auto& member) {
        try {
            validate_no_control(member.get_name());
        } catch (const control_character_present_exception& e) {
            vlog(securitylog.warn, "Role Member contains invalid characters");
            throw serde::pb::rpc::invalid_argument_exception(
              "Role Member contains invalid characters");
        }
    };

    pb_member.visit_member(
      [&](const proto::admin::role_user& user) { validate_member(user); },
      [&](const proto::admin::role_group& group) { validate_member(group); },
      [&](const auto&) {
          vlog(
            securitylog.warn,
            "Unhandled role member type for member: {}",
            pb_member);
          throw serde::pb::rpc::invalid_argument_exception(
            ssx::sformat("Unknown role member type for member: {}", pb_member));
      });
}

security::role_member
convert_to_security_role_member(const proto::admin::role_member& pb_member) {
    return pb_member.visit_member(
      [&](const proto::admin::role_user& user) {
          return security::role_member{
            security::role_member_type::user, user.get_name()};
      },
      [&](const proto::admin::role_group& group) {
          return security::role_member{
            security::role_member_type::group, group.get_name()};
      },
      [&](const auto&) -> security::role_member {
          vlog(
            securitylog.warn,
            "Unhandled role member type for member: {}",
            pb_member);
          throw serde::pb::rpc::unknown_exception(
            ssx::sformat("Unknown role member type for member: {}", pb_member));
      });
}

security::role convert_to_security_role(const proto::admin::role& pb_role) {
    auto role_members = pb_role.get_members()
                        | std::views::transform(convert_to_security_role_member)
                        | std::ranges::to<security::role::container_type>();

    return security::role{std::move(role_members)};
}

proto::admin::role_member
convert_to_pb_role_member(const security::role_member& role_member) {
    const auto& member_name = role_member.name();
    const auto& member_type = role_member.type();

    proto::admin::role_member pb_role_member;
    switch (member_type) {
    case security::role_member_type::user: {
        proto::admin::role_user pb_role_user;
        pb_role_user.set_name(ss::sstring{member_name});
        pb_role_member.set_user(std::move(pb_role_user));
        break;
    }
    case security::role_member_type::group: {
        proto::admin::role_group pb_role_group;
        pb_role_group.set_name(ss::sstring{member_name});
        pb_role_member.set_group(std::move(pb_role_group));
        break;
    }
    default:
        vlog(
          securitylog.warn,
          "Unhandled role member type for member '{}'.",
          member_name);
        throw serde::pb::rpc::internal_exception(
          ssx::sformat(
            "Unknown role member type for member '{}'.", member_name));
    }
    return pb_role_member;
}

proto::admin::role
convert_to_pb_role(ss::sstring role_name, const security::role& role) {
    proto::admin::role pb_role;
    pb_role.set_name(std::move(role_name));

    auto pb_role_members
      = role.members() | std::views::transform(convert_to_pb_role_member)
        | std::ranges::to<chunked_vector<proto::admin::role_member>>();

    pb_role.set_members(std::move(pb_role_members));
    return pb_role;
}

} // namespace internal

using namespace internal;

security_service_impl::security_service_impl(
  admin::proxy::client proxy_client,
  cluster::controller* controller,
  ss::sharded<kafka::server>& kafka_server,
  ss::sharded<cluster::metadata_cache>& md_cache)
  : _proxy_client(std::move(proxy_client))
  , _controller(controller)
  , _kafka_server(kafka_server)
  , _md_cache(md_cache) {}

seastar::future<proto::admin::create_role_response>
security_service_impl::create_role(
  serde::pb::rpc::context ctx, proto::admin::create_role_request req) {
    vlog(securitylog.trace, "create_role: {}", req);

    const auto redirect_node = utils::redirect_to_leader(
      _md_cache.local(), model::controller_ntp, _proxy_client.self_node_id());

    if (redirect_node) {
        vlog(
          securitylog.debug,
          "Redirecting to leader of {}: {}",
          model::controller_ntp,
          *redirect_node);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::security_service_client>(
            *redirect_node)
          .create_role(ctx, std::move(req));
    }

    const auto& req_role = req.get_role();
    validate_role_name(req_role.get_name());
    std::ranges::for_each(req_role.get_members(), validate_pb_role_member);

    const security::role_name role_name{req_role.get_name()};
    const security::role role = convert_to_security_role(req_role);

    const auto err
      = co_await _controller->get_security_frontend().local().create_role(
        role_name,
        role,
        model::timeout_clock::now() + security_operation_timeout);

    if (err == cluster::errc::role_exists) {
        // Idempotency: if the role already exists, return an error unless
        // it is identical to the requested role.
        if (_controller->get_role_store().local().get(role_name) != role) {
            throw serde::pb::rpc::already_exists_exception(
              "Role already exists");
        }
    } else if (err != cluster::errc::success) {
        vlog(
          securitylog.error, "Failed to create role '{}': {}", role_name, err);
        throw serde::pb::rpc::unknown_exception(
          ssx::sformat("Failed to create role '{}'", role_name));
    }

    proto::admin::create_role_response res;
    res.set_role(std::move(req.get_role()));
    co_return res;
}

seastar::future<proto::admin::get_role_response>
security_service_impl::get_role(
  serde::pb::rpc::context, proto::admin::get_role_request req) {
    vlog(securitylog.trace, "get_role: {}", req);

    validate_role_name(req.get_name());
    const security::role_name role_name{req.get_name()};
    auto role_opt = _controller->get_role_store().local().get(role_name);
    if (!role_opt) {
        vlog(securitylog.debug, "Role '{}' does not exist", role_name);
        throw serde::pb::rpc::not_found_exception(
          ssx::sformat("Role '{}' does not exist", role_name));
    }

    const auto& role = role_opt.value();

    proto::admin::get_role_response res;
    res.set_role(convert_to_pb_role(role_name, role));
    co_return res;
}

seastar::future<proto::admin::list_roles_response>
security_service_impl::list_roles(
  serde::pb::rpc::context, proto::admin::list_roles_request req) {
    vlog(securitylog.trace, "list_roles: {}", req);

    // TODO: implement filtering based on request parameters
    auto pred = [](const auto&) { return true; };

    const auto& local_role_store = _controller->get_role_store().local();
    const auto role_name_views = local_role_store.range(pred);

    proto::admin::list_roles_response res;
    auto& pb_roles = res.get_roles();

    for (const auto& role_name_view : role_name_views) {
        const security::role_name role_name{role_name_view};
        vlog(securitylog.debug, "Found role: {}", role_name);
        const auto role = local_role_store.get(role_name);
        if (role) {
            pb_roles.push_back(convert_to_pb_role(role_name, *role));
        } else {
            vlog(
              securitylog.error,
              "Role '{}' listed in store but could not be retrieved",
              role_name);
        }
    }

    co_return res;
}

seastar::future<proto::admin::add_role_members_response>
security_service_impl::add_role_members(
  serde::pb::rpc::context ctx, proto::admin::add_role_members_request req) {
    vlog(securitylog.trace, "add_role_members: {}", req);

    const auto redirect_node = utils::redirect_to_leader(
      _md_cache.local(), model::controller_ntp, _proxy_client.self_node_id());

    if (redirect_node) {
        vlog(
          securitylog.debug,
          "Redirecting to leader of {}: {}",
          model::controller_ntp,
          *redirect_node);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::security_service_client>(
            *redirect_node)
          .add_role_members(ctx, std::move(req));
    }

    validate_role_name(req.get_role_name());
    std::ranges::for_each(req.get_members(), validate_pb_role_member);

    const security::role_name role_name{req.get_role_name()};
    const auto members_to_add
      = req.get_members()
        | std::views::transform(convert_to_security_role_member)
        | std::ranges::to<std::vector<security::role_member>>();

    const auto role_opt = _controller->get_role_store().local().get(role_name);

    if (!role_opt) {
        vlog(securitylog.debug, "Role '{}' does not exist", role_name);
        throw serde::pb::rpc::not_found_exception(
          ssx::sformat("Role '{}' does not exist", role_name));
    }

    auto curr_members = role_opt->members();
    curr_members.insert(members_to_add.begin(), members_to_add.end());

    const security::role role{curr_members};

    const auto err
      = co_await _controller->get_security_frontend().local().update_role(
        role_name,
        role,
        model::timeout_clock::now() + security_operation_timeout);

    if (err == cluster::errc::role_does_not_exist) {
        vlog(
          securitylog.debug,
          "Role '{}' disappeared during member addition",
          role_name);
        throw serde::pb::rpc::not_found_exception(
          ssx::sformat("Role '{}' does not exist", role_name));
    } else if (err != cluster::errc::success) {
        vlog(
          securitylog.error,
          "Failed to add members to role '{}': {}",
          role_name,
          err);
        throw serde::pb::rpc::internal_exception(
          ssx::sformat("Failed to add members to role '{}'", role_name));
    }

    proto::admin::add_role_members_response res{};
    res.set_role(convert_to_pb_role(role_name, role));
    co_return res;
}

seastar::future<proto::admin::remove_role_members_response>
security_service_impl::remove_role_members(
  serde::pb::rpc::context ctx, proto::admin::remove_role_members_request req) {
    vlog(securitylog.trace, "remove_role_members: {}", req);

    const auto redirect_node = utils::redirect_to_leader(
      _md_cache.local(), model::controller_ntp, _proxy_client.self_node_id());

    if (redirect_node) {
        vlog(
          securitylog.debug,
          "Redirecting to leader of {}: {}",
          model::controller_ntp,
          *redirect_node);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::security_service_client>(
            *redirect_node)
          .remove_role_members(ctx, std::move(req));
    }

    validate_role_name(req.get_role_name());
    std::ranges::for_each(req.get_members(), validate_pb_role_member);

    const security::role_name role_name{req.get_role_name()};
    const auto members_to_remove
      = req.get_members()
        | std::views::transform(convert_to_security_role_member)
        | std::ranges::to<std::vector<security::role_member>>();

    const auto role_opt = _controller->get_role_store().local().get(role_name);

    if (!role_opt) {
        vlog(securitylog.debug, "Role '{}' does not exist", role_name);
        throw serde::pb::rpc::not_found_exception(
          ssx::sformat("Role '{}' does not exist", role_name));
    }

    auto curr_members = role_opt->members();
    absl::erase_if(curr_members, [&members_to_remove](const auto& member) {
        return std::ranges::contains(members_to_remove, member);
    });

    const security::role role{curr_members};

    const auto err
      = co_await _controller->get_security_frontend().local().update_role(
        role_name,
        role,
        model::timeout_clock::now() + security_operation_timeout);

    if (err == cluster::errc::role_does_not_exist) {
        vlog(
          securitylog.debug,
          "Role '{}' disappeared during member removal",
          role_name);
        throw serde::pb::rpc::not_found_exception(
          ssx::sformat("Role '{}' does not exist", role_name));
    } else if (err != cluster::errc::success) {
        vlog(
          securitylog.error,
          "Failed to remove members from role '{}': {}",
          role_name,
          err);
        throw serde::pb::rpc::internal_exception(
          ssx::sformat("Failed to remove members from role '{}'", role_name));
    }

    proto::admin::remove_role_members_response res{};
    res.set_role(convert_to_pb_role(role_name, role));
    co_return res;
}

seastar::future<proto::admin::delete_role_response>
security_service_impl::delete_role(
  serde::pb::rpc::context ctx, proto::admin::delete_role_request req) {
    vlog(securitylog.trace, "delete_role: {}", req);

    const auto redirect_node = utils::redirect_to_leader(
      _md_cache.local(), model::controller_ntp, _proxy_client.self_node_id());

    if (redirect_node) {
        vlog(
          securitylog.debug,
          "Redirecting to leader of {}: {}",
          model::controller_ntp,
          *redirect_node);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::security_service_client>(
            *redirect_node)
          .delete_role(ctx, std::move(req));
    }

    validate_role_name(req.get_name());
    const security::role_name role_name{req.get_name()};

    const auto err
      = co_await _controller->get_security_frontend().local().delete_role(
        role_name, model::timeout_clock::now() + security_operation_timeout);

    if (err == cluster::errc::role_does_not_exist) {
        // Idempotency: removing a non-existent role is considered successful.
        vlog(
          securitylog.debug,
          "Role '{}' already gone during deletion",
          role_name);
        co_return proto::admin::delete_role_response{};
    } else if (err != cluster::errc::success) {
        vlog(
          securitylog.error, "Failed to delete role '{}': {}", role_name, err);
        throw serde::pb::rpc::internal_exception(
          ssx::sformat("Failed to delete role '{}'", role_name));
    }

    if (req.get_delete_acls()) {
        vlog(
          securitylog.debug,
          "Deleting ACLs associated with role '{}'",
          role_name);

        security::acl_binding_filter role_binding_filter{
          security::resource_pattern_filter::any(),
          security::acl_entry_filter{
            security::role::to_principal(role_name()),
            std::nullopt,
            std::nullopt,
            std::nullopt}};

        auto results
          = co_await _controller->get_security_frontend().local().delete_acls(
            {std::move(role_binding_filter)}, security_operation_timeout);

        size_t n_deleted = 0;
        size_t n_failed = 0;
        for (const auto& r : results) {
            if (r.error == cluster::errc::success) {
                n_deleted += 1;
            } else {
                n_failed += 1;
                auto ec = make_error_code(r.error);
                vlog(
                  securitylog.warn,
                  "Error while deleting ACLs for {} - {}:{}",
                  role_name,
                  ec,
                  ec.message());
            }
        }

        vlog(
          securitylog.debug,
          "Deleted {} ACL bindings for role {} ({} failed)",
          n_deleted,
          role_name,
          n_failed);
    }

    co_return proto::admin::delete_role_response{};
}

seastar::future<proto::admin::list_current_user_roles_response>
security_service_impl::list_current_user_roles(
  serde::pb::rpc::context ctx,
  proto::admin::list_current_user_roles_request req) {
    vlog(securitylog.trace, "list_current_user_roles: {}", req);

    const auto* auth_result = ctx.get_optional_value<request_auth_result>();

    if (auth_result == nullptr) {
        vlog(securitylog.warn, "No request_auth_result found in context");
        throw serde::pb::rpc::failed_precondition_exception(
          "No authentication result found");
    }

    const security::role_member member{
      security::role_member_type::user, auth_result->get_username()};

    const auto role_names_for_member
      = _controller->get_role_store().local().roles_for_member(member);

    proto::admin::list_current_user_roles_response res;
    std::ranges::transform(
      role_names_for_member,
      std::back_inserter(res.get_roles()),
      [](const auto& role_name) { return ss::sstring{role_name}; });

    co_return res;
}

seastar::future<proto::admin::resolve_oidc_identity_response>
security_service_impl::resolve_oidc_identity(
  serde::pb::rpc::context ctx, proto::admin::resolve_oidc_identity_request) {
    const auto* auth_result = ctx.get_optional_value<request_auth_result>();

    if (auth_result == nullptr) {
        vlog(securitylog.warn, "No request_auth_result found in context");
        throw serde::pb::rpc::failed_precondition_exception(
          "No authentication result found");
    }

    auto& sasl_mechanism = auth_result->get_sasl_mechanism();
    if (sasl_mechanism != security::oidc::sasl_authenticator::name) {
        vlog(
          securitylog.warn, "SASL mechanism is not OIDC: {}", sasl_mechanism);
        throw serde::pb::rpc::failed_precondition_exception(
          "SASL mechanism is not OIDC");
    }

    proto::admin::resolve_oidc_identity_response resp;
    resp.set_principal(ss::sstring(auth_result->get_username()));

    const auto& bearer = auth_result->get_password();
    if (!bearer.starts_with(authz_bearer_prefix)) {
        vlog(securitylog.warn, "Invalid OIDC bearer token format: {}", bearer);
        throw serde::pb::rpc::unauthenticated_exception(
          "Invalid OIDC bearer token format");
    }

    security::oidc::authenticator auth{_controller->get_oidc_service().local()};
    auto res = auth.authenticate(bearer.substr(authz_bearer_prefix.length()));

    if (res.has_error() || !res.has_value()) {
        vlog(
          securitylog.warn,
          "Failed to authenticate OIDC token: {}",
          res.has_error() ? res.error().message() : "unknown");

        throw serde::pb::rpc::unauthenticated_exception(
          "Failed to authenticate OIDC token");
    }

    // Convert ss::lowres_system_clock::time_point to absl::Time
    resp.set_expire(
      absl::FromChrono(
        std::chrono::system_clock::time_point{
          res.assume_value().expiry.time_since_epoch()}));

    resp.set_groups(
      {std::from_range,
       auth_result->get_groups()
         | std::views::transform(&security::acl_principal::name)});

    co_return resp;
}

seastar::future<proto::admin::refresh_oidc_keys_response>
security_service_impl::refresh_oidc_keys(
  serde::pb::rpc::context ctx, proto::admin::refresh_oidc_keys_request) {
    vlog(securitylog.debug, "Refreshing OIDC keys.");

    co_await _controller->get_oidc_service().invoke_on_all(
      [](security::oidc::service& s) { return s.refresh_keys(); });

    if (!proxy::is_proxied(ctx)) {
        vlog(securitylog.debug, "Broadcasting request to other nodes");

        auto clients = _proxy_client.make_clients_for_other_nodes<
          proto::admin::security_service_client>();

        for (auto& client_pair : clients) {
            auto& [node_id, client] = client_pair;
            vlog(
              securitylog.trace, "Proxying refresh_oidc_keys to {}", node_id);
            co_await client.refresh_oidc_keys(ctx, {});
        }
    }

    co_return proto::admin::refresh_oidc_keys_response{};
}

seastar::future<proto::admin::revoke_oidc_sessions_response>
security_service_impl::revoke_oidc_sessions(
  serde::pb::rpc::context ctx, proto::admin::revoke_oidc_sessions_request) {
    vlog(securitylog.debug, "Refreshing OIDC keys and revoking OIDC sessions");

    co_await _controller->get_oidc_service().invoke_on_all(
      [](security::oidc::service& s) { return s.refresh_keys(); });

    co_await _kafka_server.invoke_on_all([](kafka::server& ks) {
        return ks.revoke_credentials(security::oidc::sasl_authenticator::name);
    });

    if (!proxy::is_proxied(ctx)) {
        vlog(securitylog.debug, "Broadcasting request to other nodes");

        auto clients = _proxy_client.make_clients_for_other_nodes<
          proto::admin::security_service_client>();

        for (auto& client_pair : clients) {
            auto& [node_id, client] = client_pair;
            vlog(
              securitylog.trace,
              "Proxying revoke_oidc_sessions to {}",
              node_id);
            co_await client.revoke_oidc_sessions(ctx, {});
        }
    }

    co_return proto::admin::revoke_oidc_sessions_response{};
}

} // namespace admin
