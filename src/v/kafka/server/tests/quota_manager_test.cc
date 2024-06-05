// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/client_quota_store.h"
#include "config/configuration.h"
#include "kafka/server/client_quota_translator.h"
#include "kafka/server/quota_manager.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

using namespace std::chrono_literals;

const static auto client_id = "franz-go";

struct fixture {
    kafka::quota_manager::client_quotas_t buckets_map;
    ss::sharded<cluster::client_quota::store> quota_store;
    ss::sharded<kafka::quota_manager> sqm;

    ss::future<> start() {
        co_await quota_store.start();
        co_await sqm.start(std::ref(buckets_map), std::ref(quota_store));
        co_await sqm.invoke_on_all(&kafka::quota_manager::start);
    }

    ss::future<> stop() {
        co_await sqm.stop();
        co_await quota_store.stop();
    }
};

template<typename F>
ss::future<> run_quota_manager_test(F test_core) {
    fixture f;
    co_await f.start();

    // Run the core of the test now that everything's set up
    co_await ss::futurize_invoke(test_core, f.sqm);

    co_await f.stop();
}

template<typename F>
ss::future<> set_config(F update) {
    co_await ss::smp::invoke_on_all(
      [update{std::move(update)}]() { update(config::shard_local_cfg()); });
    co_await ss::sleep(std::chrono::milliseconds(1));
}

SEASTAR_THREAD_TEST_CASE(quota_manager_fetch_no_throttling) {
    set_config([](auto& conf) {
        conf.target_fetch_quota_byte_rate.set_value(std::nullopt);
    }).get();

    run_quota_manager_test(
      ss::coroutine::lambda(
        [](ss::sharded<kafka::quota_manager>& sqm) -> ss::future<> {
            auto& qm = sqm.local();

            // Test that if fetch throttling is disabled, we don't throttle
            co_await qm.record_fetch_tp(client_id, 10000000000000);
            auto delay = co_await qm.throttle_fetch_tp(client_id);

            BOOST_CHECK_EQUAL(0ms, delay);
        }))
      .get();
}

SEASTAR_THREAD_TEST_CASE(quota_manager_fetch_throttling) {
    set_config([](auto& conf) {
        conf.target_fetch_quota_byte_rate.set_value(100);
        conf.max_kafka_throttle_delay_ms.set_value(
          std::chrono::milliseconds::max());
    }).get();

    run_quota_manager_test(
      ss::coroutine::lambda(
        [](ss::sharded<kafka::quota_manager>& sqm) -> ss::future<> {
            auto& qm = sqm.local();

            // Test that below the fetch quota we don't throttle
            co_await qm.record_fetch_tp(client_id, 99);
            auto delay = co_await qm.throttle_fetch_tp(client_id);

            BOOST_CHECK_EQUAL(delay, 0ms);

            // Test that above the fetch quota we throttle
            co_await qm.record_fetch_tp(client_id, 10);
            delay = co_await qm.throttle_fetch_tp(client_id);

            BOOST_CHECK_GT(delay, 0ms);

            // Test that once we wait out the throttling delay, we don't
            // throttle again (as long as we stay under the limit)
            co_await seastar::sleep_abortable(delay + 1s);
            co_await qm.record_fetch_tp(client_id, 10);
            delay = co_await qm.throttle_fetch_tp(client_id);

            BOOST_CHECK_EQUAL(delay, 0ms);
        }))
      .get();
}

SEASTAR_THREAD_TEST_CASE(quota_manager_fetch_stress_test) {
    set_config([](config::configuration& conf) {
        conf.target_fetch_quota_byte_rate.set_value(100);
        conf.max_kafka_throttle_delay_ms.set_value(
          std::chrono::milliseconds::max());
    }).get();

    run_quota_manager_test(
      ss::coroutine::lambda(
        [](ss::sharded<kafka::quota_manager>& sqm) -> ss::future<> {
            // Exercise the quota manager from multiple cores to attempt to
            // discover segfaults caused by data races/use-after-free
            co_await sqm.invoke_on_all(ss::coroutine::lambda(
              [](kafka::quota_manager& qm) -> ss::future<> {
                  for (size_t i = 0; i < 1000; ++i) {
                      co_await qm.record_fetch_tp(client_id, 1);
                      auto delay [[maybe_unused]]
                      = co_await qm.throttle_fetch_tp(client_id);
                      co_await ss::maybe_yield();
                  }
              }));
        }))
      .get();
}

constexpr std::string_view raw_basic_produce_config = R"([
  {
    "group_name": "not-franz-go-group",
    "clients_prefix": "not-franz-go",
    "quota": 2048
  },
  {
    "group_name": "franz-go-group",
    "clients_prefix": "franz-go",
    "quota": 4096
  }
])";

constexpr std::string_view raw_basic_fetch_config = R"([
  {
    "group_name": "not-franz-go-group",
    "clients_prefix": "not-franz-go",
    "quota": 2049
  },
  {
    "group_name": "franz-go-group",
    "clients_prefix": "franz-go",
    "quota": 4097
  }
])";

constexpr auto basic_config = [](config::configuration& conf) {
    // produce
    conf.target_quota_byte_rate.set_value(1024);
    conf.kafka_client_group_byte_rate_quota.set_value(
      YAML::Load(std::string(raw_basic_produce_config)));
    // fetch
    conf.target_fetch_quota_byte_rate.set_value(1025);
    conf.kafka_client_group_fetch_byte_rate_quota.set_value(
      YAML::Load(std::string(raw_basic_fetch_config)));
    // partition mutation rate
    conf.kafka_admin_topic_api_rate.set_value(1026);
};

SEASTAR_THREAD_TEST_CASE(static_config_test) {
    using k_client_id = kafka::k_client_id;
    using k_group_name = kafka::k_group_name;
    fixture f;
    f.start().get();
    auto stop = ss::defer([&] { f.stop().get(); });

    set_config(basic_config).get();

    BOOST_REQUIRE_EQUAL(f.buckets_map.local()->size(), 0);

    {
        ss::sstring client_id = "franz-go";
        f.sqm.local().record_fetch_tp(client_id, 1).get();
        f.sqm.local().record_produce_tp_and_throttle(client_id, 1).get();
        f.sqm.local().record_partition_mutations(client_id, 1).get();
        auto it = f.buckets_map.local()->find(
          k_group_name{client_id + "-group"});
        BOOST_REQUIRE(it != f.buckets_map.local()->end());
        BOOST_REQUIRE(it->second->tp_produce_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_produce_rate->rate(), 4096);
        BOOST_REQUIRE(it->second->tp_fetch_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_fetch_rate->rate(), 4097);
        BOOST_REQUIRE(!it->second->pm_rate.has_value());
    }
    {
        ss::sstring client_id = "not-franz-go";
        f.sqm.local().record_fetch_tp(client_id, 1).get();
        f.sqm.local().record_produce_tp_and_throttle(client_id, 1).get();
        f.sqm.local().record_partition_mutations(client_id, 1).get();
        auto it = f.buckets_map.local()->find(
          k_group_name{client_id + "-group"});
        BOOST_REQUIRE(it != f.buckets_map.local()->end());
        BOOST_REQUIRE(it->second->tp_produce_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_produce_rate->rate(), 2048);
        BOOST_REQUIRE(it->second->tp_fetch_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_fetch_rate->rate(), 2049);
        BOOST_REQUIRE(!it->second->pm_rate.has_value());
    }
    {
        ss::sstring client_id = "unconfigured";
        f.sqm.local().record_fetch_tp(client_id, 1).get();
        f.sqm.local().record_produce_tp_and_throttle(client_id, 1).get();
        f.sqm.local().record_partition_mutations(client_id, 1).get();
        auto it = f.buckets_map.local()->find(k_client_id{client_id});
        BOOST_REQUIRE(it != f.buckets_map.local()->end());
        BOOST_REQUIRE(it->second->tp_produce_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_produce_rate->rate(), 1024);
        BOOST_REQUIRE(it->second->tp_fetch_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_fetch_rate->rate(), 1025);
        BOOST_REQUIRE(it->second->pm_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->pm_rate->rate(), 1026);
    }
}

SEASTAR_THREAD_TEST_CASE(update_test) {
    using clock = kafka::quota_manager::clock;
    using k_group_name = kafka::k_group_name;
    using k_client_id = kafka::k_client_id;
    fixture f;
    f.start().get();
    auto stop = ss::defer([&] { f.stop().get(); });

    set_config(basic_config).get();

    auto now = clock::now();
    {
        // Update fetch config
        ss::sstring client_id = "franz-go";
        f.sqm.local().record_fetch_tp(client_id, 8194, now).get();
        f.sqm.local()
          .record_produce_tp_and_throttle(client_id, 8192, now)
          .get();

        set_config([](config::configuration& conf) {
            auto fetch_config = YAML::Load(std::string(raw_basic_fetch_config));
            for (auto n : fetch_config) {
                n["quota"] = n["quota"].as<uint32_t>() + 1;
            }
            conf.kafka_client_group_fetch_byte_rate_quota.set_value(
              fetch_config);
        }).get();

        // Check the rate has been updated
        auto it = f.buckets_map.local()->find(
          k_group_name{client_id + "-group"});
        BOOST_REQUIRE(it != f.buckets_map.local()->end());
        BOOST_REQUIRE(it->second->tp_fetch_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_fetch_rate->rate(), 4098);

        // Check produce is the same bucket
        BOOST_REQUIRE(it->second->tp_produce_rate.has_value());
        auto delay = f.sqm.local()
                       .record_produce_tp_and_throttle(client_id, 1, now)
                       .get();
        BOOST_CHECK_EQUAL(delay / 1ms, 1000);
    }

    {
        // Remove produce config
        ss::sstring client_id = "franz-go";
        f.sqm.local().record_fetch_tp(client_id, 8196, now).get();
        f.sqm.local()
          .record_produce_tp_and_throttle(client_id, 8192, now)
          .get();

        set_config([&](config::configuration& conf) {
            auto produce_config = YAML::Load(
              std::string(raw_basic_produce_config));
            for (auto i = 0; i < produce_config.size(); ++i) {
                if (
                  produce_config[i]["clients_prefix"].as<std::string>()
                  == client_id) {
                    produce_config.remove(i);
                    break;
                }
            }
            conf.kafka_client_group_byte_rate_quota.set_value(produce_config);
        }).get();

        // Check the produce rate has been updated on the group
        auto it = f.buckets_map.local()->find(
          k_group_name{client_id + "-group"});
        BOOST_REQUIRE(it != f.buckets_map.local()->end());
        BOOST_CHECK(!it->second->tp_produce_rate.has_value());

        // Check fetch is the same bucket
        BOOST_REQUIRE(it->second->tp_fetch_rate.has_value());
        auto delay = f.sqm.local().throttle_fetch_tp(client_id, now).get();
        BOOST_CHECK_EQUAL(delay / 1ms, 1000);

        // Check the new produce rate now applies
        f.sqm.local()
          .record_produce_tp_and_throttle(client_id, 8192, now)
          .get();
        auto client_it = f.buckets_map.local()->find(k_client_id{client_id});
        BOOST_REQUIRE(client_it != f.buckets_map.local()->end());
        BOOST_REQUIRE(client_it->second->tp_produce_rate.has_value());
        BOOST_CHECK_EQUAL(client_it->second->tp_produce_rate->rate(), 1024);
    }
}
