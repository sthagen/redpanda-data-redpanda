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

#include "container/priority_queue.h"
#include "features/feature_table.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/server.h"
#include "proto/redpanda/core/admin/v2/kafka_connections.proto.h"
#include "redpanda/admin/aip_filter.h"
#include "redpanda/admin/aip_ordering.h"
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

struct connection_collector {
    virtual ~connection_collector() = default;
    virtual void add(proto::kafka_connection conn) = 0;
    virtual chunked_vector<proto::kafka_connection> extract() && = 0;
    virtual size_t size() const = 0;
};

class unordered_collector : public connection_collector {
    chunked_vector<proto::kafka_connection> _connections;
    size_t _limit;

public:
    explicit unordered_collector(size_t limit)
      : _limit(limit) {}

    void add(proto::kafka_connection conn) final {
        if (_connections.size() < _limit) {
            _connections.emplace_back(std::move(conn));
        }
    }

    chunked_vector<proto::kafka_connection> extract() && final {
        return std::move(_connections);
    }

    size_t size() const final { return _connections.size(); }
};

template<typename Comparator>
class ordered_collector : public connection_collector {
    // Invert the order here to get the min-k instead of the max-k
    chunked_bounded_priority_queue<
      proto::kafka_connection,
      detail::invert_comparator<Comparator>>
      _pq;

public:
    ordered_collector(size_t limit, Comparator comp)
      : _pq(limit, detail::invert_comparator<Comparator>(std::move(comp))) {}

    void add(proto::kafka_connection conn) final { _pq.push(std::move(conn)); }

    chunked_vector<proto::kafka_connection> extract() && final {
        return std::move(_pq).extract_heap();
    }

    size_t size() const final { return _pq.size(); }

    ss::future<chunked_vector<proto::kafka_connection>> extract_sorted() && {
        return std::move(_pq).async_extract_sorted();
    }
};

struct connection_gather_result {
    chunked_vector<proto::kafka_connection> connections;
    size_t total_matching_count;
};

ss::future<connection_gather_result> gather_connections(
  const kafka::server& server,
  const filter_predicate& filter,
  ss::shared_ptr<connection_collector> collector) {
    auto result = connection_gather_result{};

    auto conn_ptrs = server.list_connections();
    co_await ss::coroutine::maybe_yield();

    auto process_conn = [&result, &collector, &filter](
                          proto::admin::kafka_connection&& conn_proto) {
        bool matches_filter = filter(conn_proto);

        if (matches_filter) {
            result.total_matching_count++;
            collector->add(std::move(conn_proto));
        }
    };

    co_await ssx::async_for_each(
      conn_ptrs, [&process_conn](const auto& conn_ptr) {
          process_conn(conn_ptr->to_proto());
      });

    auto closed_conns = server.list_closed_connections();
    for (auto& elem : closed_conns) {
        auto elem_copy = co_await proto::admin::kafka_connection::from_proto(
          co_await elem->to_proto());
        process_conn(std::move(elem_copy));
    }

    result.connections = std::move(*collector).extract();
    co_return result;
}

ss::future<size_t> gather_all_shards(
  ss::sharded<kafka::server>& kafka_server,
  const filter_predicate& filter,
  const auto& make_local_collector,
  connection_collector& global_collector) {
    size_t total_matching_connections = 0;

    for (ss::shard_id shard = 0; shard < ss::smp::count; ++shard) {
        auto accumulated_count = global_collector.size();
        auto shard_result = co_await kafka_server.invoke_on(
          shard,
          [accumulated_count, &filter, &make_local_collector](
            kafka::server& server) {
              return gather_connections(
                server, filter, make_local_collector(accumulated_count));
          });

        total_matching_connections += shard_result.total_matching_count;

        for (auto& conn : shard_result.connections) {
            global_collector.add(std::move(conn));
        }

        co_await ss::coroutine::maybe_yield();
    }

    co_return total_matching_connections;
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

    if (req.get_order_by().empty()) {
        auto global_collector = unordered_collector{limit};

        auto make_local_collector = [limit](size_t accumulated_count) {
            return ss::make_shared<unordered_collector>(
              limit - accumulated_count);
        };

        auto total_matching_connections = co_await gather_all_shards(
          _kafka_server, filter, make_local_collector, global_collector);

        resp.set_connections(std::move(global_collector).extract());
        resp.set_total_size(total_matching_connections);
    } else {
        auto ordering_conf
          = make_ordering_config<proto::admin::kafka_connection>(
            req.get_order_by());
        auto comp = sort_order::parse(ordering_conf);

        auto global_collector = ordered_collector{limit, comp};

        auto make_local_collector = [limit, &comp](size_t) {
            return ss::make_shared<ordered_collector<decltype(comp)>>(
              limit, comp);
        };

        auto total_matching_connections = co_await gather_all_shards(
          _kafka_server, filter, make_local_collector, global_collector);

        resp.set_connections(
          co_await std::move(global_collector).extract_sorted());
        resp.set_total_size(total_matching_connections);
    }

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
