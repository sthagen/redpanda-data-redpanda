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

#include "base/vassert-register.h"

#include <seastar/util/backtrace.hh>

#include <gtest/gtest.h>

struct AssertLogHolderTest : public testing::Test {};

TEST_F(AssertLogHolderTest, ValidateAssertLogHolder) {
    static ss::sstring message;
    static ss::sstring unused_message;
    const auto cb = [](std::string_view msg) { message = ss::sstring{msg}; };
    const auto unused_cb = [](std::string_view msg) {
        unused_message = ss::sstring{msg};
    };

    auto bt = ss::current_backtrace();
    base::register_event(bt, "This is a test event: test");
    EXPECT_TRUE(message.empty());

    base::register_cb(cb);

    // Verify that a second call to `register_cb` does not replace the current
    // callback
    base::register_cb(unused_cb);

    base::register_event(bt, "This is a second test event: test");
    EXPECT_EQ(message, fmt::format("This is a second test event: test"));
    EXPECT_TRUE(unused_message.empty());
}
