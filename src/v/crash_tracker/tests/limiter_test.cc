/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "crash_tracker/limiter.h"
#include "crash_tracker/recorder.h"
#include "crash_tracker/types.h"
#include "model/timestamp.h"
#include "version/version.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/core.h>
#include <gtest/gtest.h>

namespace crash_tracker {

struct LimiterTest : public testing::Test {};

TEST_F(LimiterTest, TestDescribeCrashes) {
    auto crashes = std::vector<recorder::recorded_crash>{};
    EXPECT_EQ(
      crash_tracker::impl::describe_crashes(crashes),
      "(No crash files have been recorded.)");

    using with_additional_info
      = ss::bool_class<struct with_additional_info_tag>;
    auto make_crash = [](with_additional_info wai) {
        auto res = crash_description{};
        res.crash_message = crash_description::reserved_string_t{
          "Assertion error"};
        res.stacktrace = crash_description::reserved_string_t{
          "0xaaaaaaaa 0xbbbbbbbb"};
        if (wai) {
            res.addition_info = crash_description::reserved_string_t{
              "false != true at example.cc:123"};
        }
        res.app_version = ss::sstring{redpanda_version()};
        return recorder::recorded_crash{"", std::move(res), {}};
    };

    crashes.emplace_back(make_crash(with_additional_info::no));

    EXPECT_EQ(
      crash_tracker::impl::describe_crashes(crashes),
      fmt::format(
        // clang-format off
        "The following crashes have been recorded:"
        "\nCrash #1 at 1970-01-01 00:00:00 UTC - Redpanda version: {}. Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb.",
        // clang-format on
        redpanda_version()));

    for (int i = 0; i < 10; i++) {
        crashes.emplace_back(make_crash(with_additional_info::yes));
    }

    auto expected = fmt::format(
      R"(The following crashes have been recorded:
Crash #1 at 1970-01-01 00:00:00 UTC - Redpanda version: {version}. Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb.
Crash #2 at 1970-01-01 00:00:00 UTC - Redpanda version: {version}. Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123
Crash #3 at 1970-01-01 00:00:00 UTC - Redpanda version: {version}. Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123
Crash #4 at 1970-01-01 00:00:00 UTC - Redpanda version: {version}. Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123
Crash #5 at 1970-01-01 00:00:00 UTC - Redpanda version: {version}. Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123
    ...
Crash #7 at 1970-01-01 00:00:00 UTC - Redpanda version: {version}. Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123
Crash #8 at 1970-01-01 00:00:00 UTC - Redpanda version: {version}. Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123
Crash #9 at 1970-01-01 00:00:00 UTC - Redpanda version: {version}. Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123
Crash #10 at 1970-01-01 00:00:00 UTC - Redpanda version: {version}. Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123
Crash #11 at 1970-01-01 00:00:00 UTC - Redpanda version: {version}. Assertion error Backtrace: 0xaaaaaaaa 0xbbbbbbbb. false != true at example.cc:123)",
      fmt::arg("version", redpanda_version()));

    EXPECT_EQ(crash_tracker::impl::describe_crashes(crashes), expected);
}

} // namespace crash_tracker
