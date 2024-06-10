// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "cluster/client_quota_serde.h"
#include "cluster/client_quota_store.h"
#include "config/configuration.h"
#include "kafka/server/client_quota_translator.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/auto_unit_test.hpp>
#include <boost/test/test_tools.hpp>
#include <boost/test/unit_test.hpp>

#include <variant>

using namespace kafka;

const ss::sstring test_client_id = "franz-go";
const tracker_key test_client_id_key = k_client_id{test_client_id};

constexpr auto P_DEF = 1111;
constexpr auto F_DEF = 2222;
constexpr auto PM_DEF = 3333;

constexpr std::string_view raw_basic_produce_config = R"([
  {
    "group_name": "not-franz-go",
    "clients_prefix": "not-franz-go",
    "quota": 2048
  },
  {
    "group_name": "franz-go",
    "clients_prefix": "franz-go",
    "quota": 4096
  }
])";

constexpr std::string_view raw_basic_fetch_config = R"([
  {
    "group_name": "not-franz-go",
    "clients_prefix": "not-franz-go",
    "quota": 2049
  },
  {
    "group_name": "franz-go",
    "clients_prefix": "franz-go",
    "quota": 4097
  }
])";

// Helper for checking std::variant types for equality
const auto CHECK_VARIANT_EQ = [](auto expected, auto got) {
    BOOST_CHECK_EQUAL(expected, get<decltype(expected)>(got));
};

void reset_configs() {
    config::shard_local_cfg().target_quota_byte_rate.reset();
    config::shard_local_cfg().target_fetch_quota_byte_rate.reset();
    config::shard_local_cfg().kafka_admin_topic_api_rate.reset();
    config::shard_local_cfg().kafka_client_group_byte_rate_quota.reset();
    config::shard_local_cfg().kafka_client_group_fetch_byte_rate_quota.reset();
}

struct fixture {
    ss::sharded<cluster::client_quota::store> quota_store;
    kafka::client_quota_translator tr;

    fixture()
      : tr(std::ref(quota_store)) {
        quota_store.start().get();
    }

    ~fixture() { quota_store.stop().get(); }
};

SEASTAR_THREAD_TEST_CASE(quota_translator_default_test) {
    reset_configs();
    fixture f;

    auto default_limits = client_quota_limits{
      .produce_limit = 2147483648,
      .fetch_limit = std::nullopt,
      .partition_mutation_limit = std::nullopt,
    };
    auto [key, limits] = f.tr.find_quota(
      {client_quota_type::produce_quota, test_client_id});
    BOOST_CHECK_EQUAL(test_client_id_key, key);
    BOOST_CHECK_EQUAL(default_limits, limits);
}

SEASTAR_THREAD_TEST_CASE(quota_translator_modified_default_test) {
    reset_configs();
    config::shard_local_cfg().target_quota_byte_rate.set_value(1111);
    config::shard_local_cfg().target_fetch_quota_byte_rate.set_value(2222);
    config::shard_local_cfg().kafka_admin_topic_api_rate.set_value(3333);

    fixture f;

    auto expected_limits = client_quota_limits{
      .produce_limit = 1111,
      .fetch_limit = 2222,
      .partition_mutation_limit = 3333,
    };
    auto [key, limits] = f.tr.find_quota(
      {client_quota_type::produce_quota, test_client_id});
    BOOST_CHECK_EQUAL(test_client_id_key, key);
    BOOST_CHECK_EQUAL(expected_limits, limits);
}

void run_quota_translator_client_group_test(fixture& f) {
    // Stage 1 - Start by checking that tracker_key's are correctly detected
    // for various client ids
    auto get_produce_key = [&f](auto client_id) {
        return f.tr.find_quota_key(
          {client_quota_type::produce_quota, client_id});
    };
    auto get_fetch_key = [&f](auto client_id) {
        return f.tr.find_quota_key({client_quota_type::fetch_quota, client_id});
    };
    auto get_mutation_key = [&f](auto client_id) {
        return f.tr.find_quota_key(
          {client_quota_type::partition_mutation_quota, client_id});
    };

    // Check keys for produce
    CHECK_VARIANT_EQ(k_group_name{"franz-go"}, get_produce_key("franz-go"));
    CHECK_VARIANT_EQ(
      k_group_name{"not-franz-go"}, get_produce_key("not-franz-go"));
    CHECK_VARIANT_EQ(k_client_id{"unknown"}, get_produce_key("unknown"));
    CHECK_VARIANT_EQ(k_client_id{""}, get_produce_key(std::nullopt));

    // Check keys for fetch
    CHECK_VARIANT_EQ(k_group_name{"franz-go"}, get_fetch_key("franz-go"));
    CHECK_VARIANT_EQ(
      k_group_name{"not-franz-go"}, get_fetch_key("not-franz-go"));
    CHECK_VARIANT_EQ(k_client_id{"unknown"}, get_fetch_key("unknown"));
    CHECK_VARIANT_EQ(k_client_id{""}, get_fetch_key(std::nullopt));

    // Check keys for partition mutations
    CHECK_VARIANT_EQ(k_client_id{"franz-go"}, get_mutation_key("franz-go"));
    CHECK_VARIANT_EQ(
      k_client_id{"not-franz-go"}, get_mutation_key("not-franz-go"));
    CHECK_VARIANT_EQ(k_client_id{"unknown"}, get_mutation_key("unknown"));
    CHECK_VARIANT_EQ(k_client_id{""}, get_mutation_key(std::nullopt));

    // Stage 2 - Next verify that the correct quota limits apply to the
    // various tracker_key's being tested
    // Check limits for the franz-go groups
    auto franz_go_limits = client_quota_limits{
      .produce_limit = 4096,
      .fetch_limit = 4097,
      .partition_mutation_limit = {},
    };
    BOOST_CHECK_EQUAL(
      franz_go_limits, f.tr.find_quota_value(k_group_name{"franz-go"}));

    // Check limits for the not-franz-go groups
    auto not_franz_go_limits = client_quota_limits{
      .produce_limit = 2048,
      .fetch_limit = 2049,
      .partition_mutation_limit = {},
    };
    BOOST_CHECK_EQUAL(
      not_franz_go_limits, f.tr.find_quota_value(k_group_name{"not-franz-go"}));

    // Check limits for the non-client-group keys
    auto default_limits = client_quota_limits{
      .produce_limit = P_DEF,
      .fetch_limit = F_DEF,
      .partition_mutation_limit = PM_DEF,
    };
    BOOST_CHECK_EQUAL(
      default_limits, f.tr.find_quota_value(k_client_id{"unknown"}));
    BOOST_CHECK_EQUAL(default_limits, f.tr.find_quota_value(k_client_id{""}));
    BOOST_CHECK_EQUAL(
      default_limits, f.tr.find_quota_value(k_client_id{"franz-go"}));
    BOOST_CHECK_EQUAL(
      default_limits, f.tr.find_quota_value(k_client_id{"not-franz-go"}));
}

SEASTAR_THREAD_TEST_CASE(quota_translator_config_client_group_test) {
    reset_configs();
    config::shard_local_cfg().target_quota_byte_rate.set_value(P_DEF);
    config::shard_local_cfg().target_fetch_quota_byte_rate.set_value(F_DEF);
    config::shard_local_cfg().kafka_admin_topic_api_rate.set_value(PM_DEF);

    config::shard_local_cfg().kafka_client_group_byte_rate_quota.set_value(
      YAML::Load(std::string(raw_basic_produce_config)));
    config::shard_local_cfg()
      .kafka_client_group_fetch_byte_rate_quota.set_value(
        YAML::Load(std::string(raw_basic_fetch_config)));

    fixture f;
    run_quota_translator_client_group_test(f);
}

SEASTAR_THREAD_TEST_CASE(quota_translator_store_client_group_test) {
    reset_configs();
    fixture f;

    using cluster::client_quota::entity_key;
    using cluster::client_quota::entity_value;

    auto default_key = entity_key{
      .parts = {entity_key::part{
        .part = entity_key::part::client_id_default_match{},
      }},
    };
    auto default_values = entity_value{
      .producer_byte_rate = P_DEF,
      .consumer_byte_rate = F_DEF,
      .controller_mutation_rate = PM_DEF,
    };

    auto franz_go_key = entity_key{
      .parts = {entity_key::part{
        .part = entity_key::part::client_id_prefix_match{.value = "franz-go"},
      }},
    };
    auto franz_go_values = entity_value{
      .producer_byte_rate = 4096,
      .consumer_byte_rate = 4097,
    };

    auto not_franz_go_key = entity_key{
      .parts = {entity_key::part{
        .part
        = entity_key::part::client_id_prefix_match{.value = "not-franz-go"},
      }},
    };
    auto not_franz_go_values = entity_value{
      .producer_byte_rate = 2048,
      .consumer_byte_rate = 2049,
    };

    f.quota_store.local().set_quota(default_key, default_values);
    f.quota_store.local().set_quota(franz_go_key, franz_go_values);
    f.quota_store.local().set_quota(not_franz_go_key, not_franz_go_values);

    run_quota_translator_client_group_test(f);
}

SEASTAR_THREAD_TEST_CASE(quota_translator_priority_order) {
    reset_configs();
    fixture f;

    using cluster::client_quota::entity_key;
    using cluster::client_quota::entity_value;

    auto check_produce =
      [&f](auto client_id, auto expected_key, auto expected_value) {
          auto [k, v] = f.tr.find_quota(
            {.q_type = kafka::client_quota_type::produce_quota,
             .client_id = client_id});
          CHECK_VARIANT_EQ(expected_key, k);
          BOOST_CHECK_EQUAL(expected_value, v.produce_limit);
      };
    auto check_fetch =
      [&f](auto client_id, auto expected_key, auto expected_value) {
          auto [k, v] = f.tr.find_quota(
            {.q_type = kafka::client_quota_type::fetch_quota,
             .client_id = client_id});
          CHECK_VARIANT_EQ(expected_key, k);
          BOOST_CHECK_EQUAL(expected_value, v.fetch_limit);
      };
    auto check_pm = [&f](
                      auto client_id, auto expected_key, auto expected_value) {
        auto [k, v] = f.tr.find_quota(
          {.q_type = kafka::client_quota_type::partition_mutation_quota,
           .client_id = client_id});
        CHECK_VARIANT_EQ(expected_key, k);
        BOOST_CHECK_EQUAL(expected_value, v.partition_mutation_limit);
    };

    // This test walks through the priority levels of the various ways of
    // configuring quotas in increasing order and asserts that each successive
    // priority level overwrites the previous one. The quota values XY mean
    // priority level X and Y = {1, 2, 3} for produce/fetch/partition mutation
    // quotas respectively to check that their values are independent.

    // 1. Lowest priority: default cluster config
    config::shard_local_cfg().target_quota_byte_rate.set_value(11);
    config::shard_local_cfg().target_fetch_quota_byte_rate.set_value(12);
    config::shard_local_cfg().kafka_admin_topic_api_rate.set_value(13);

    check_produce("franz-go", k_client_id{"franz-go"}, 11);
    check_fetch("franz-go", k_client_id{"franz-go"}, 12);
    check_pm("franz-go", k_client_id{"franz-go"}, 13);

    // 2. Next: default client quota
    auto default_key = entity_key{
      .parts = {entity_key::part{
        .part = entity_key::part::client_id_default_match{},
      }},
    };
    auto default_values = entity_value{
      .producer_byte_rate = 21,
      .consumer_byte_rate = 22,
      .controller_mutation_rate = 23,
    };
    f.quota_store.local().set_quota(default_key, default_values);

    check_produce("franz-go", k_client_id{"franz-go"}, 21);
    check_fetch("franz-go", k_client_id{"franz-go"}, 22);
    check_pm("franz-go", k_client_id{"franz-go"}, 23);

    // 3. Next: client id prefix cluster configs
    const auto produce_prefix_config = YAML::Load(std::string(R"([
  {
    "group_name": "franz-go",
    "clients_prefix": "franz-go",
    "quota": 31
  }
])"));
    const auto fetch_prefix_config = YAML::Load(std::string(R"([
  {
    "group_name": "franz-go",
    "clients_prefix": "franz-go",
    "quota": 32
  }
])"));
    config::shard_local_cfg().kafka_client_group_byte_rate_quota.set_value(
      produce_prefix_config);
    config::shard_local_cfg()
      .kafka_client_group_fetch_byte_rate_quota.set_value(fetch_prefix_config);

    check_produce("franz-go", k_group_name{"franz-go"}, 31);
    check_fetch("franz-go", k_group_name{"franz-go"}, 32);
    // there's no cluster config for partition mutation quotas based on client
    // prefix, so this fall backs to the previous priority level
    check_pm("franz-go", k_client_id{"franz-go"}, 23);

    // 4. Next: client id prefix quota store
    auto franz_go_prefix_key = entity_key{
      .parts = {entity_key::part{
        .part = entity_key::part::client_id_prefix_match{.value = "franz-go"},
      }},
    };
    auto franz_go_prefix_values = entity_value{
      .producer_byte_rate = 41,
      .consumer_byte_rate = 42,
      .controller_mutation_rate = 43,
    };
    f.quota_store.local().set_quota(
      franz_go_prefix_key, franz_go_prefix_values);

    check_produce("franz-go", k_group_name{"franz-go"}, 41);
    check_fetch("franz-go", k_group_name{"franz-go"}, 42);
    check_pm("franz-go", k_group_name{"franz-go"}, 43);

    // 5. Finally: client id exact match quota store
    auto franz_go_exact_key = entity_key{
      .parts = {entity_key::part{
        .part = entity_key::part::client_id_match{.value = "franz-go"},
      }},
    };
    auto franz_go_exact_values = entity_value{
      .producer_byte_rate = 51,
      .consumer_byte_rate = 52,
      .controller_mutation_rate = 53,
    };
    f.quota_store.local().set_quota(franz_go_exact_key, franz_go_exact_values);

    check_produce("franz-go", k_client_id{"franz-go"}, 51);
    check_fetch("franz-go", k_client_id{"franz-go"}, 52);
    check_pm("franz-go", k_client_id{"franz-go"}, 53);
}
