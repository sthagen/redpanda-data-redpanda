// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/chunked_vector.h"
#include "kafka/protocol/types.h"
#include "proto/redpanda/core/admin/v2/cluster.proto.h"
#include "proto/redpanda/core/admin/v2/kafka_connections.proto.h"
#include "random/generators.h"
#include "redpanda/admin/aip_ordering.h"
#include "redpanda/admin/kafka_connections_service_impl.h"

#include <seastar/net/inet_address.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/defer.hh>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <cstddef>
#include <memory>
#include <ranges>

struct ListKafkaConnectionsTest {
    using value_type = proto::admin::kafka_connection;

    struct random_connection_data {
        int32_t node_id;
        int32_t shard_id;
        ss::sstring uid;
        proto::admin::kafka_connection_state state;
        absl::Time open_time;
        proto::admin::authentication_state auth_state;
        proto::admin::authentication_mechanism auth_mechanism;
        ss::sstring user_principal;
        ss::sstring listener_name;
        bool tls_enabled;
        ss::sstring ip_address;
        uint16_t port;
        ss::sstring client_id;
        ss::sstring client_software_name;
        ss::sstring client_software_version;
        std::optional<ss::sstring> group_id;
        std::optional<kafka::member_id> group_member_id;

        random_connection_data()
          : node_id(random_generators::get_int<int32_t>(8))
          , shard_id(random_generators::get_int<int32_t>(8))
          , uid(ssx::sformat("{}", uuid_t::create()))
          , state(
              static_cast<proto::admin::kafka_connection_state>(
                random_generators::get_int<int32_t>(0, 3)))
          , open_time(absl::Time(absl::Now()))
          , auth_state(
              static_cast<proto::admin::authentication_state>(
                random_generators::get_int<int32_t>(0, 3)))
          , auth_mechanism(
              static_cast<proto::admin::authentication_mechanism>(
                random_generators::get_int<int32_t>(0, 4)))
          , user_principal(random_generators::gen_alphanum_max_distinct(16))
          , listener_name("kafka")
          , tls_enabled(random_generators::get_int<int32_t>(0, 1) == 1)
          , ip_address(ssx::sformat("{}", ss::net::inet_address{}))
          , port(random_generators::get_int<uint16_t>(1024, 65535))
          , client_id(random_generators::gen_alphanum_max_distinct(16))
          , client_software_name(
              random_generators::gen_alphanum_max_distinct(4))
          , client_software_version(
              random_generators::gen_alphanum_max_distinct(4)) {
            if (random_generators::get_int<int32_t>(0, 1) == 1) {
                group_id = random_generators::gen_alphanum_max_distinct(64);
                boost::uuids::uuid uuid = boost::uuids::random_generator()();
                group_member_id = kafka::member_id(
                  ssx::sformat("{}-{}", client_id, to_string(uuid)));
            }
        }

        auto to_proto() const {
            proto::admin::kafka_connection conn;
            conn.set_node_id(node_id);
            conn.set_shard_id(shard_id);
            conn.set_uid(ss::sstring{uid});
            conn.set_state(state);
            conn.set_open_time(absl::Time{open_time});
            proto::admin::authentication_info auth_info;
            auth_info.set_state(auth_state);
            auth_info.set_mechanism(auth_mechanism);
            auth_info.set_user_principal(ss::sstring{user_principal});
            conn.set_authentication_info(std::move(auth_info));
            conn.set_listener_name(ss::sstring{listener_name});
            proto::admin::tls_info tls_info;
            tls_info.set_enabled(tls_enabled);
            conn.set_tls_info(std::move(tls_info));
            proto::admin::source src;
            src.set_ip_address(ss::sstring{ip_address});
            src.set_port(port);
            conn.set_source(std::move(src));
            conn.set_client_id(ss::sstring{client_id});
            conn.set_client_software_name(ss::sstring{client_software_name});
            conn.set_client_software_version(
              ss::sstring{client_software_version});
            if (group_id.has_value()) {
                conn.set_group_id(ss::sstring{group_id.value()});
            }
            if (group_member_id.has_value()) {
                conn.set_group_member_id(ss::sstring{group_member_id.value()});
            }
            return conn;
        }
    };

    static auto make_random_data(size_t size) {
        return chunked_vector<random_connection_data>(
          std::from_range,
          std::views::iota(size_t{0}, size) | std::views::transform([](auto) {
              return random_connection_data{};
          }));
    }

    static auto
    make_test_proto(const chunked_vector<random_connection_data>& data) {
        return chunked_vector<value_type>(
          std::from_range,
          data | std::views::transform(&random_connection_data::to_proto));
    }

    using serde_fun = ss::future<iobuf> (
      proto::admin::list_kafka_connections_response::*)() const;

    static constexpr serde_fun to_proto
      = &proto::admin::list_kafka_connections_response::to_proto;
    static constexpr serde_fun to_json
      = &proto::admin::list_kafka_connections_response::to_json;

    template<typename Fn>
    constexpr auto measure(bool measure, Fn&& fn) {
        if (measure) {
            perf_tests::start_measuring_time();
        }
        return ss::futurize_invoke(std::forward<Fn>(fn)).finally([measure]() {
            if (measure) {
                perf_tests::stop_measuring_time();
            }
        });
    }

    ss::future<> run_test_impl(
      bool ordered,
      bool measure_to_proto,
      bool measure_insert,
      bool measure_extract,
      std::optional<serde_fun> serialize,
      size_t size,
      size_t limit) {
        auto random_data = make_random_data(size);

        std::unique_ptr<admin::detail::connection_collector> collector;
        if (ordered) {
            auto cmp = admin::sort_order::parse(
              admin::make_ordering_config<proto::admin::kafka_connection>(
                "uid"));
            collector = std::make_unique<
              ::admin::detail::ordered_collector<decltype(cmp)>>(limit, cmp);
        } else {
            collector = std::make_unique<::admin::detail::unordered_collector>(
              limit);
        }

        constexpr auto end = [](auto obj) { perf_tests::do_not_optimize(obj); };

        auto test_data = co_await measure(
          measure_to_proto, [&]() { return make_test_proto(random_data); });
        if (!measure_insert && !measure_extract && !serialize) {
            co_return end(test_data.size());
        }

        co_await measure(measure_insert, [&]() {
            return collector->add_all(std::move(test_data));
        });
        if (!measure_extract && !serialize) {
            co_return end(collector->size());
        }

        auto result = co_await measure(
          measure_extract, [&]() { return std::move(*collector).extract(); });
        if (!serialize) {
            co_return end(result.size());
        }

        proto::admin::list_kafka_connections_response resp;
        resp.set_connections(std::move(result));
        auto buf = co_await measure(
          serialize.has_value(), [&]() { return (resp.*serialize.value())(); });

        end(buf.size_bytes());
    }
};

constexpr size_t c1 = 100;
constexpr size_t k10 = 10000;
constexpr size_t k100 = 100000;
constexpr size_t k250 = 250000;

/// Unordered Collect and Insert are both O(n)
PERF_TEST_F(ListKafkaConnectionsTest, CollectProto100k) {
    return run_test_impl(false, true, false, false, std::nullopt, k100, k100);
}
PERF_TEST_F(ListKafkaConnectionsTest, CollectProto250k) {
    return run_test_impl(false, true, false, false, std::nullopt, k250, k250);
}

// Combine insert and extract, since extract is just a std::move()
PERF_TEST_F(ListKafkaConnectionsTest, UnorderedInsertExtract100k) {
    return run_test_impl(false, false, true, true, std::nullopt, k100, k100);
}
PERF_TEST_F(ListKafkaConnectionsTest, UnorderedInsertExtract250k) {
    return run_test_impl(false, false, true, true, std::nullopt, k250, k250);
}

// Ordered Insert and Extract are both O(n log n)
PERF_TEST_F(ListKafkaConnectionsTest, OrderedInsert10k) {
    return run_test_impl(true, false, true, false, std::nullopt, k10, k10);
}
PERF_TEST_F(ListKafkaConnectionsTest, OrderedInsert100k) {
    return run_test_impl(true, false, true, false, std::nullopt, k100, k100);
}
PERF_TEST_F(ListKafkaConnectionsTest, OrderedInsert250k) {
    return run_test_impl(true, false, true, false, std::nullopt, k250, k250);
}
PERF_TEST_F(ListKafkaConnectionsTest, OrderedExtract10k) {
    return run_test_impl(true, false, false, true, std::nullopt, k10, k10);
}
PERF_TEST_F(ListKafkaConnectionsTest, OrderedExtract100k) {
    return run_test_impl(true, false, false, true, std::nullopt, k100, k100);
}
PERF_TEST_F(ListKafkaConnectionsTest, OrderedExtract250k) {
    return run_test_impl(true, false, false, true, std::nullopt, k250, k250);
}

// ToProto and ToJson should both be O(n)
PERF_TEST_F(ListKafkaConnectionsTest, ToProto10k) {
    return run_test_impl(false, false, false, false, to_proto, k10, k10);
}
PERF_TEST_F(ListKafkaConnectionsTest, ToProto100k) {
    return run_test_impl(false, false, false, false, to_proto, k100, k100);
}
PERF_TEST_F(ListKafkaConnectionsTest, ToJson10k) {
    return run_test_impl(false, false, false, false, to_json, k10, k10);
}
PERF_TEST_F(ListKafkaConnectionsTest, ToJson100k) {
    return run_test_impl(false, false, false, false, to_json, k100, k100);
}

// Test select 100 from 100k
PERF_TEST_F(ListKafkaConnectionsTest, UnorderedInsertProto100of100k) {
    return run_test_impl(false, false, true, false, std::nullopt, k100, c1);
}
PERF_TEST_F(ListKafkaConnectionsTest, OrderedInsertProto100of100k) {
    return run_test_impl(true, false, true, false, std::nullopt, k100, c1);
}

// E2E Unordered is O(n)
PERF_TEST_F(ListKafkaConnectionsTest, E2EUnorderedProto10k) {
    return run_test_impl(false, true, true, true, to_proto, k10, k10);
}
PERF_TEST_F(ListKafkaConnectionsTest, E2EUnorderedProto100k) {
    return run_test_impl(false, true, true, true, to_proto, k100, k100);
}
PERF_TEST_F(ListKafkaConnectionsTest, E2EUnorderedJson10k) {
    return run_test_impl(false, true, true, true, to_json, k10, k10);
}
PERF_TEST_F(ListKafkaConnectionsTest, E2EUnorderedJson100k) {
    return run_test_impl(false, true, true, true, to_json, k100, k100);
}

// E2E Ordered tests are harder to quantify
PERF_TEST_F(ListKafkaConnectionsTest, E2EOrderedProto10k) {
    return run_test_impl(true, true, true, true, to_proto, k10, k10);
}
PERF_TEST_F(ListKafkaConnectionsTest, E2EOrderedProto100k) {
    return run_test_impl(true, true, true, true, to_proto, k100, k100);
}
PERF_TEST_F(ListKafkaConnectionsTest, E2EOrderedJson10k) {
    return run_test_impl(true, true, true, true, to_json, k10, k10);
}
PERF_TEST_F(ListKafkaConnectionsTest, E2EOrderedJson100k) {
    return run_test_impl(true, true, true, true, to_json, k100, k100);
}

// E2E tests with limit
PERF_TEST_F(ListKafkaConnectionsTest, E2EUnorderedProto100of100k) {
    return run_test_impl(false, true, true, true, to_proto, k100, c1);
}
PERF_TEST_F(ListKafkaConnectionsTest, E2EOrderedProto100of100k) {
    return run_test_impl(true, true, true, true, to_proto, k100, c1);
}
PERF_TEST_F(ListKafkaConnectionsTest, E2EUnorderedJson100of100k) {
    return run_test_impl(false, true, true, true, to_json, k100, c1);
}
PERF_TEST_F(ListKafkaConnectionsTest, E2EOrderedJson100of100k) {
    return run_test_impl(true, true, true, true, to_json, k100, c1);
}
