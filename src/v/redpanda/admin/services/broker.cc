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

#include "redpanda/admin/services/broker.h"

#include "features/feature_table.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/server.h"
#include "proto/redpanda/core/admin/v2/kafka_connections.proto.h"
#include "redpanda/admin/aip_filter.h"
#include "serde/protobuf/rpc.h"
#include "ssx/async_algorithm.h"
#include "version/version.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger brlog{"admin_api_server/broker_service"};

} // namespace

broker_service_impl::broker_service_impl(
  admin::proxy::client client,
  std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* services,
  ss::sharded<kafka::server>& kafka_server,
  ss::sharded<features::feature_table>& feature_table)
  : _proxy_client(std::move(client))
  , _services(services)
  , _kafka_server(kafka_server)
  , _feature_table(feature_table) {}

ss::future<proto::admin::get_broker_response> broker_service_impl::get_broker(
  serde::pb::rpc::context ctx, proto::admin::get_broker_request req) {
    auto target = model::node_id(req.get_node_id());
    if (target != -1 && target != _proxy_client.self_node_id()) {
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::broker_service_client>(target)
          .get_broker(ctx, std::move(req));
    }
    proto::admin::get_broker_response resp;
    resp.set_broker(self_broker());
    co_return resp;
}

ss::future<proto::admin::list_brokers_response>
broker_service_impl::list_brokers(
  serde::pb::rpc::context ctx, proto::admin::list_brokers_request) {
    proto::admin::list_brokers_response list_resp;
    list_resp.get_brokers().push_back(self_broker());
    auto clients
      = _proxy_client
          .make_clients_for_other_nodes<proto::admin::broker_service_client>();
    for (auto& [node_id, client] : clients) {
        proto::admin::get_broker_request req;
        req.set_node_id(node_id);
        auto get_resp = co_await client.get_broker(ctx, std::move(req));
        list_resp.get_brokers().push_back(std::move(get_resp.get_broker()));
    }
    co_return list_resp;
}

proto::admin::broker broker_service_impl::self_broker() const {
    proto::admin::broker b;
    b.set_node_id(_proxy_client.self_node_id());
    b.get_build_info().set_version(ss::sstring(redpanda_git_version()));
    b.get_build_info().set_build_sha(ss::sstring(redpanda_git_revision()));
    for (auto& service : *_services) {
        for (auto& route : service->all_routes()) {
            proto::rpc_route r;
            r.set_name(
              fmt::format("{}.{}", route.service_name, route.method_name));
            r.set_http_route(ss::sstring(route.path));
            b.get_admin_server().get_routes().push_back(std::move(r));
        }
    }
    return b;
}

namespace {

struct connection_gather_result {
    chunked_vector<proto::kafka_connection> connections;
    size_t total_matching_count;
};

ss::future<connection_gather_result> gather_connections(
  const kafka::server& server, size_t limit, const filter_predicate& filter) {
    auto result = connection_gather_result{};

    auto conn_ptrs = server.list_connections();
    co_await ss::coroutine::maybe_yield();

    co_await ssx::async_for_each(
      conn_ptrs, [&result, limit, &filter](const auto& conn_ptr) {
          auto conn_proto = conn_ptr->to_proto();

          bool matches_filter = filter(conn_proto);

          if (matches_filter) {
              result.total_matching_count++;

              if (result.connections.size() < limit) {
                  result.connections.emplace_back(std::move(conn_proto));
              }
          }
      });

    co_return result;
}

void check_license(const features::feature_table& ft) {
    if (ft.should_sanction()) {
        const auto& license = ft.get_license();
        auto status = [&license]() {
            return !license.has_value()    ? "not present"
                   : license->is_expired() ? "expired"
                                           : "unknown error";
        };
        throw serde::pb::rpc::failed_precondition_exception(
          fmt::format("Invalid license: {}", status()));
    }
}

} // namespace

ss::future<proto::admin::list_kafka_connections_response>
broker_service_impl::list_kafka_connections(
  serde::pb::rpc::context ctx,
  proto::admin::list_kafka_connections_request req) {
    vlog(brlog.trace, "list_kafka_connections: {}", req);

    check_license(_feature_table.local());

    // Proxy to the target node id specified in the request
    auto target = model::node_id{req.get_node_id()};
    if (target != -1 && target != _proxy_client.self_node_id()) {
        vlog(brlog.debug, "Redirecting to target node id {}", target);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::broker_service_client>(target)
          .list_kafka_connections(ctx, std::move(req));
    }

    auto resp = proto::admin::list_kafka_connections_response{};

    constexpr size_t default_limit = 1000;
    auto limit = (req.get_page_size() == 0) ? default_limit
                                            : req.get_page_size();

    auto filter_cfg = make_aip_filter_config<proto::kafka_connection>(
      req.get_filter());
    auto filter = aip_filter_parser::create_aip_filter(std::move(filter_cfg));

    // Iterate across shards sequentially to bound memory usage while
    // calculating total size
    auto result_connections = chunked_vector<proto::kafka_connection>{};
    auto total_matching_connections = size_t{0};

    for (ss::shard_id shard = 0; shard < ss::smp::count; ++shard) {
        // Get connections from this shard with the remaining limit
        auto remaining_limit = limit - result_connections.size();
        auto shard_result = co_await _kafka_server.invoke_on(
          shard,
          [remaining_limit, &filter](
            kafka::server& server) -> ss::future<connection_gather_result> {
              return gather_connections(server, remaining_limit, filter);
          });

        total_matching_connections += shard_result.total_matching_count;
        std::ranges::move(
          shard_result.connections, std::back_inserter(result_connections));

        co_await ss::coroutine::maybe_yield();
    }

    resp.set_total_size(total_matching_connections);
    resp.set_connections(std::move(result_connections));

    vlog(
      brlog.trace,
      "list_kafka_connections: response connections: {} ({}b), total matching: "
      "{}",
      resp.get_connections().size(),
      resp.get_connections().memory_size(),
      resp.get_total_size());

    co_return resp;
}

} // namespace admin
