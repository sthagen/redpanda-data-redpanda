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

#include "kafka/server/server.h"
#include "redpanda/admin/proxy/context.h"
#include "security/oidc_authenticator.h"
#include "security/oidc_service.h"
#include "security/request_auth.h"

#include <seastar/core/coroutine.hh>

namespace admin {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger securitylog{"admin_api_server/security_service"};
} // namespace

security_service_impl::security_service_impl(
  admin::proxy::client proxy_client,
  cluster::controller* controller,
  ss::sharded<kafka::server>& kafka_server)
  : _proxy_client(std::move(proxy_client))
  , _controller(controller)
  , _kafka_server(kafka_server) {}

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
