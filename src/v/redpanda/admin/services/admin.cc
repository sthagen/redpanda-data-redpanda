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

#include "redpanda/admin/services/admin.h"

#include "version/version.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin {

admin_service_impl::admin_service_impl(
  admin::proxy::client client,
  std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* services)
  : _proxy_client(std::move(client))
  , _services(services) {}

ss::future<proto::list_build_info_response>
admin::admin_service_impl::list_build_info(
  serde::pb::rpc::context ctx, proto::list_build_info_request) {
    proto::list_build_info_response resp;
    auto& build_info = resp.get_build_infos().emplace_back();
    build_info.set_node_id(_proxy_client.self_node_id());
    build_info.set_version(ss::sstring(redpanda_git_version()));
    build_info.set_build_sha(ss::sstring(redpanda_git_revision()));
    if (ctx.is_proxied()) {
        co_return resp;
    }
    auto clients
      = _proxy_client
          .make_clients_for_other_nodes<proto::admin::admin_service_client>();
    for (auto& [node_id, client] : clients) {
        auto proxy_resp = co_await client.list_build_info(
          ctx, proto::list_build_info_request{});
        std::ranges::move(
          proxy_resp.get_build_infos(),
          std::back_inserter(resp.get_build_infos()));
    }
    co_return resp;
}

ss::future<proto::list_rpc_routes_response>
admin::admin_service_impl::list_rpc_routes(
  serde::pb::rpc::context ctx, proto::list_rpc_routes_request req) {
    auto target = model::node_id(req.get_node_id());
    if (
      target != model::node_id(-1) && target != _proxy_client.self_node_id()) {
        auto client
          = _proxy_client
              .make_client_for_node<proto::admin::admin_service_client>(target);
        co_return co_await client.list_rpc_routes(ctx, std::move(req));
    }
    proto::list_rpc_routes_response resp;
    for (auto& service : *_services) {
        for (auto& route : service->all_routes()) {
            proto::rpc_route r;
            r.set_name(
              fmt::format("{}.{}", route.service_name, route.method_name));
            r.set_http_route(fmt::format("/v2{}", route.path));
            resp.get_routes().push_back(std::move(r));
        }
    }
    co_return resp;
}

} // namespace admin
