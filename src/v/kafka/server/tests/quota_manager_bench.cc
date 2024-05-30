/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "config/configuration.h"
#include "kafka/server/quota_manager.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

#include <iostream>
#include <limits>
#include <optional>
#include <vector>

const static auto fixed_client_id = "shared-client-id";
const static size_t total_requests{10 << 20};
const static size_t unique_client_id_count = 1000;

std::vector<ss::sstring> initialize_client_ids() {
    std::vector<ss::sstring> client_ids;
    client_ids.reserve(unique_client_id_count);
    for (int i = 0; i < unique_client_id_count; ++i) {
        client_ids.push_back("client-id-" + std::to_string(i));
    }
    return client_ids;
}

std::vector<ss::sstring> unique_client_ids = initialize_client_ids();

ss::future<>
send_requests(kafka::quota_manager& qm, size_t count, bool use_unique) {
    auto offset = ss::this_shard_id() * count;
    for (size_t i = 0; i < count; ++i) {
        auto cid_idx = (offset + i) % unique_client_id_count;
        auto client_id = use_unique ? unique_client_ids[cid_idx]
                                    : fixed_client_id;

        // Have a mixed workload of produce and fetch to highlight any cache
        // contention on produce/fetch token buckets for the same client id
        if (ss::this_shard_id() % 2 == 0) {
            co_await qm.record_fetch_tp(client_id, 1);
            auto delay = co_await qm.throttle_fetch_tp(client_id);
            perf_tests::do_not_optimize(delay);
        } else {
            auto delay = co_await qm.record_produce_tp_and_throttle(
              client_id, 1);
            perf_tests::do_not_optimize(delay);
        }
        co_await maybe_yield();
    }
    co_return;
}

ss::future<> test_quota_manager(size_t count, bool use_unique) {
    kafka::quota_manager::client_quotas_t buckets_map;
    ss::sharded<kafka::quota_manager> sqm;
    co_await sqm.start(std::ref(buckets_map));
    co_await sqm.invoke_on_all(&kafka::quota_manager::start);

    perf_tests::start_measuring_time();
    co_await sqm.invoke_on_all([count, use_unique](kafka::quota_manager& qm) {
        return send_requests(qm, count, use_unique);
    });
    perf_tests::stop_measuring_time();
    co_await sqm.stop();
}

struct test_case {
    std::optional<uint32_t> fetch_tp;
    bool use_unique;
};

future<> run_tc(test_case tc) {
    co_await ss::smp::invoke_on_all([fetch_tp{tc.fetch_tp}]() {
        config::shard_local_cfg().target_fetch_quota_byte_rate.set_value(
          fetch_tp);
    });
    co_await test_quota_manager(total_requests / ss::smp::count, tc.use_unique);
}

struct quota_group {};

PERF_TEST_C(quota_group, test_quota_manager_on_unlimited_shared) {
    co_await run_tc(test_case{
      .fetch_tp = std::numeric_limits<uint32_t>::max(),
      .use_unique = false,
    });
}

PERF_TEST_C(quota_group, test_quota_manager_on_unlimited_unique) {
    co_await run_tc(test_case{
      .fetch_tp = std::numeric_limits<uint32_t>::max(),
      .use_unique = true,
    });
}

PERF_TEST_C(quota_group, test_quota_manager_on_limited_shared) {
    co_await run_tc(test_case{
      .fetch_tp = 1000,
      .use_unique = false,
    });
}

PERF_TEST_C(quota_group, test_quota_manager_on_limited_unique) {
    co_await run_tc(test_case{
      .fetch_tp = 1000,
      .use_unique = true,
    });
}

PERF_TEST_C(quota_group, test_quota_manager_off_shared) {
    co_await run_tc(test_case{
      .fetch_tp = std::nullopt,
      .use_unique = false,
    });
}

PERF_TEST_C(quota_group, test_quota_manager_off_unique) {
    co_await run_tc(test_case{
      .fetch_tp = std::nullopt,
      .use_unique = true,
    });
}
