// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "net/dial.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

// Compile-check the coroutine body.
template ss::future<ss::connected_socket> net::dial_serially(
  std::vector<ss::socket_address>,
  net::clock_type::time_point,
  net::fixed_timeout_dial_policy,
  seastar::logger*,
  void (*)());

namespace {
const auto t0 = net::clock_type::time_point{} + 1000s;
const auto fixed_policy = net::fixed_timeout_dial_policy{.attempt_timeout = 1s};
} // namespace

TEST(fixed_timeout_dial_policy, fixed_timeout_per_attempt) {
    auto d = fixed_policy.attempt_deadline(t0, t0 + 9s, 3);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(*d, t0 + 1s);
}

TEST(fixed_timeout_dial_policy, no_overall_deadline) {
    auto d = fixed_policy.attempt_deadline(
      t0, net::clock_type::time_point::max(), 1);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(*d, t0 + 1s);
}

TEST(fixed_timeout_dial_policy, full_timeout_when_deadline_is_closer) {
    auto d = fixed_policy.attempt_deadline(t0, t0 + 500ms, 3);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(*d, t0 + 1s);
}

TEST(fixed_timeout_dial_policy, expired_deadline) {
    EXPECT_FALSE(fixed_policy.attempt_deadline(t0, t0, 1).has_value());
    EXPECT_FALSE(fixed_policy.attempt_deadline(t0, t0 - 1s, 1).has_value());
}
