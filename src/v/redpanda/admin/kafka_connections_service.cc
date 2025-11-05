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

#include "redpanda/admin/kafka_connections_service.h"

#include "kafka/server/server.h"
#include "proto/redpanda/core/admin/v2/cluster.proto.h"
#include "proto/redpanda/core/admin/v2/kafka_connections.proto.h"
#include "random/generators.h"
#include "redpanda/admin/aip_filter.h"
#include "redpanda/admin/aip_ordering.h"
#include "redpanda/admin/kafka_connections_service_impl.h"
#include "ssx/async_algorithm.h"
#include "ssx/semaphore.h"

#include <seastar/core/coroutine.hh>

#include <chrono>
#include <ranges>

namespace proto {
using namespace proto::admin;
} // namespace proto

namespace admin {

namespace {

using namespace admin::detail;

// Estimated memory needed per kafka connection based on testing
constexpr auto memory_per_connection = 3_KiB;

ss::future<connection_gather_result> gather_connections(
  const kafka::server& server,
  const filter_predicate& filter,
  std::unique_ptr<connection_collector> collector) {
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

    result.connections = std::move(*collector).extract_unordered();
    co_return result;
}

ss::future<size_t> gather_all_shards(
  ss::sharded<kafka::server>& kafka_server,
  const filter_predicate& filter,
  const make_local_collector_t& make_local_collector,
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
        co_await global_collector.add_all(std::move(shard_result.connections));

        co_await ss::coroutine::maybe_yield();
    }

    co_return total_matching_connections;
}

} // namespace

kafka_connections_service::kafka_connections_service(
  ss::sharded<kafka::server>& kafka_server)
  : _kafka_server(kafka_server) {
    if (ss::this_shard_id() == rate_limiter_shard) {
        constexpr static auto max_requests_in_flight = size_t{1};
        _rate_limiter = std::make_unique<ssx::semaphore>(
          max_requests_in_flight, "kafka_connections_service/rate_limiter");
    }
}

ss::future<>
kafka_connections_service::start(ssx::semaphore& admin_memory_semaphore) {
    _admin_memory_semaphore = &admin_memory_semaphore;
    _admin_memory_semaphore_max = admin_memory_semaphore.current();
    co_return;
}

ss::future<kafka_connections_service::remote_units>
kafka_connections_service::rate_limit() {
    // TODO: this could be optimized later by reserving memory on-demand, while
    // iterating through shards.
    co_return co_await container().invoke_on(
      rate_limiter_shard,
      [](kafka_connections_service& self) { return self.do_rate_limit(); });
}

ss::future<kafka_connections_service::remote_units>
kafka_connections_service::do_rate_limit() {
    vassert(
      ss::this_shard_id() == rate_limiter_shard,
      "do_rate_limit() must be called on shard {}",
      rate_limiter_shard);
    const auto timeout = std::chrono::milliseconds(
      random_generators::get_int(3000, 5000));
    try {
        auto units = co_await ss::get_units(*_rate_limiter, 1, timeout);
        co_return ss::make_foreign(
          std::make_unique<ssx::semaphore_units>(std::move(units)));
    } catch (const ss::semaphore_timed_out&) {
        throw serde::pb::rpc::resource_exhausted_exception(
          "Timeout acquiring rate limiter for list_kafka_connections");
    }
}

ss::future<std::vector<kafka_connections_service::remote_units>>
kafka_connections_service::memory_limit(size_t connection_count) {
    co_return co_await container().map_reduce0(
      [connection_count](kafka_connections_service& self) {
          return self.do_memory_limit(connection_count);
      },
      std::vector<remote_units>{},
      [](std::vector<remote_units> acc, remote_units&& units) {
          acc.emplace_back(std::move(units));
          return acc;
      });
}

ss::future<kafka_connections_service::remote_units>
kafka_connections_service::do_memory_limit(size_t connection_count) {
    try {
        vassert(
          _admin_memory_semaphore != nullptr,
          "Admin memory semaphore not initialized");
        const auto timeout = std::chrono::milliseconds(
          random_generators::get_int(3000, 5000));
        auto units = co_await ss::get_units(
          *_admin_memory_semaphore,
          connection_count * memory_per_connection,
          timeout);
        co_return ss::make_foreign(
          std::make_unique<ssx::semaphore_units>(std::move(units)));
    } catch (const ss::semaphore_timed_out&) {
        throw serde::pb::rpc::resource_exhausted_exception(
          fmt::format(
            "Timeout waiting for memory for list_kafka_connections. Consider "
            "filtering for a single broker or reducing page_size."));
    }
}

ss::future<proto::admin::list_kafka_connections_response>
kafka_connections_service::list_kafka_connections_local(
  proto::admin::list_kafka_connections_request req) {
    auto resp = proto::admin::list_kafka_connections_response{};

    auto limit = get_effective_limit(req.get_page_size());

    auto filter_cfg = make_aip_filter_config<proto::kafka_connection>(
      req.get_filter());
    auto filter = aip_filter_parser::create_aip_filter(std::move(filter_cfg));

    auto [global_collector, make_local_collector] =
      [&req, limit]() -> std::pair<
                        std::unique_ptr<connection_collector>,
                        make_local_collector_t> {
        if (req.get_order_by().empty()) {
            auto global_collector = std::make_unique<unordered_collector>(
              limit);

            auto make_local_collector = [limit](size_t accumulated_count) {
                return std::make_unique<unordered_collector>(
                  limit - accumulated_count);
            };

            return std::make_pair(
              std::move(global_collector), std::move(make_local_collector));
        } else {
            auto ordering_conf
              = make_ordering_config<proto::admin::kafka_connection>(
                req.get_order_by());
            auto comp = sort_order::parse(ordering_conf);

            auto global_collector
              = std::make_unique<ordered_collector<sort_order>>(limit, comp);

            auto make_local_collector = [limit, comp](size_t) {
                return std::make_unique<ordered_collector<decltype(comp)>>(
                  limit, comp);
            };

            return std::make_pair(
              std::move(global_collector), std::move(make_local_collector));
        }
    }();

    auto total_matching_connections = co_await gather_all_shards(
      _kafka_server, filter, make_local_collector, *global_collector);

    resp.set_connections(co_await std::move(*global_collector).extract());
    resp.set_total_size(total_matching_connections);
    co_return resp;
}

ss::future<proto::admin::list_kafka_connections_response>
kafka_connections_service::list_kafka_connections_cluster_wide(
  admin::proxy::client& proxy_client,
  const serde::pb::rpc::context& ctx,
  proto::admin::list_kafka_connections_request req) {
    auto limit = get_effective_limit(req.get_page_size());

    auto collector = [&req, limit]() -> std::unique_ptr<connection_collector> {
        if (req.get_order_by().empty()) {
            return std::make_unique<unordered_collector>(limit);
        } else {
            auto ordering_conf
              = make_ordering_config<proto::admin::kafka_connection>(
                req.get_order_by());
            auto comp = sort_order::parse(ordering_conf);

            return std::make_unique<ordered_collector<sort_order>>(limit, comp);
        }
    }();

    auto total_count = size_t{0};

    auto add_to_response =
      [&collector, &total_count](
        proto::admin::list_kafka_connections_response client_resp) {
          total_count += client_resp.get_total_size();
          return collector->add_all(std::move(client_resp.get_connections()));
      };

    // TODO: we could optimize here further by inspecting the filter and if we
    // can detect that it is for a single broker by parsing the filtering AST
    // (e.g.; "node_id = X AND ..."), then we could avoid querying nodes other
    // than X.

    auto make_broker_req = [&req]() {
        auto client_req = proto::admin::list_kafka_connections_request{};
        client_req.set_filter(ss::sstring{req.get_filter()});
        client_req.set_order_by(ss::sstring{req.get_order_by()});
        client_req.set_page_size(req.get_page_size());
        return client_req;
    };

    // Iterate one by one for now to limit memory usage to be approximately in
    // the order of 2 x page_size. We could optimize here to issue requests in
    // parallel when page_size is small.
    auto other_node_clients
      = proxy_client
          .make_clients_for_other_nodes<proto::admin::cluster_service_client>();
    for (auto& [node_id, client] : other_node_clients) {
        auto client_resp = co_await client.list_kafka_connections(
          ctx, make_broker_req());

        co_await add_to_response(std::move(client_resp));
    }

    auto local_resp = co_await list_kafka_connections_local(make_broker_req());
    co_await add_to_response(std::move(local_resp));

    auto resp = proto::admin::list_kafka_connections_response{};
    resp.set_connections(co_await std::move(*collector).extract());
    resp.set_total_size(total_count);
    co_return resp;
}

size_t kafka_connections_service::get_effective_limit(size_t page_size) {
    constexpr size_t default_limit = 1000;
    auto requested_page_size = page_size == 0 ? default_limit : page_size;

    // Calculate maximum connections we can handle per request based on
    // available memory
    const auto max_connections_by_memory = _admin_memory_semaphore_max
                                           / memory_per_connection;
    const auto capped_page_size = std::min(
      requested_page_size, max_connections_by_memory);

    return capped_page_size;
}

} // namespace admin
