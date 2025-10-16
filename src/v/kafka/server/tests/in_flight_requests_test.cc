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

#include "kafka/server/connection_context.h"
#include "test_utils/test.h"

#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

TEST_CORO(in_flight_requests_tests, test_usage) {
    using tracker_t = kafka::connection_attributes::in_flight_request_tracker;

    auto now = tracker_t::clock::now();
    auto tracker = tracker_t{now};
    EXPECT_LE(tracker.get_idle_duration(now), std::chrono::seconds(1));

    auto test_req_count = 10;

    for (int i = 0; i < test_req_count; ++i) {
        tracker.record_begin_request(kafka::api_key(1));
    }
    auto res = tracker.to_proto(now);
    EXPECT_TRUE(res.get_has_more_requests());
    EXPECT_EQ(
      res.get_sampled_in_flight_requests().size(), tracker_t::max_count);

    for (int i = 0; i < test_req_count; ++i) {
        tracker.record_end_request();
    }
    now = tracker_t::clock::now();

    res = tracker.to_proto(now);
    EXPECT_FALSE(res.get_has_more_requests());
    EXPECT_EQ(res.get_sampled_in_flight_requests().size(), 0);

    constexpr auto sleep_time = 50ms;
    co_await ss::sleep(sleep_time);
    now += sleep_time;

    EXPECT_GE(tracker.get_idle_duration(now), 0s);
}
