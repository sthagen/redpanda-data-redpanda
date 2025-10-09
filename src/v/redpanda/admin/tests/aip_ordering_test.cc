/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed by the
 * Apache License, Version 2.0
 */
#include "redpanda/admin/aip_ordering.h"
#include "redpanda/admin/tests/aip_test_message_helpers.h"
#include "serde/protobuf/rpc.h"
#include "src/v/redpanda/admin/tests/aip_test_message.proto.h"

#include <fmt/ranges.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <string>
#include <vector>

namespace admin {

namespace rpc = serde::pb::rpc;

class AIPOrderingTest : public ::testing::Test {
protected:
    auto parse(std::string_view ordering_expression) {
        auto config = make_ordering_config<aip_test::test_message>(
          ordering_expression);
        return admin::sort_order::parse(config);
    }
};

TEST_F(AIPOrderingTest, SimpleAscendingSort) {
    constexpr auto msg_count = 10;
    auto msgs = std::vector<aip_test::test_message>{};
    for (int i = msg_count; i >= 1; --i) {
        msgs.push_back(create_test_message(i));
    }

    auto order = parse("int_field");
    std::ranges::sort(msgs, order);
    for (int i = 0; i < msg_count; ++i) {
        EXPECT_EQ(msgs[i].get_int_field(), i + 1);
    }
}

TEST_F(AIPOrderingTest, SimpleDescendingSort) {
    constexpr auto msg_count = 10;
    auto msgs = std::vector<aip_test::test_message>{};
    for (int i = 1; i <= msg_count; ++i) {
        msgs.push_back(create_test_message(i));
    }

    auto order = parse("int_field desc");
    std::ranges::sort(msgs, order);
    for (int i = 0; i < msg_count; ++i) {
        EXPECT_EQ(msgs[i].get_int_field(), msg_count - i);
    }
}

TEST_F(AIPOrderingTest, StringAscendingAndDescending) {
    auto msgs = std::vector<aip_test::test_message>{};
    msgs.push_back(create_test_message(1, "ccc"));
    msgs.push_back(create_test_message(2, "aaa"));
    msgs.push_back(create_test_message(3, "bbb"));

    auto asc_order = parse("string_field");
    std::ranges::sort(msgs, asc_order);
    EXPECT_EQ(msgs[0].get_string_field(), "aaa");
    EXPECT_EQ(msgs[1].get_string_field(), "bbb");
    EXPECT_EQ(msgs[2].get_string_field(), "ccc");

    auto desc_order = parse("string_field desc");
    std::ranges::sort(msgs, desc_order);
    EXPECT_EQ(msgs[0].get_string_field(), "ccc");
    EXPECT_EQ(msgs[1].get_string_field(), "bbb");
    EXPECT_EQ(msgs[2].get_string_field(), "aaa");
}

TEST_F(AIPOrderingTest, MultiFieldOrdering) {
    auto msgs = std::vector<aip_test::test_message>{};
    msgs.push_back(create_test_message(2, "b"));
    msgs.push_back(create_test_message(1, "b"));
    msgs.push_back(create_test_message(1, "a"));
    msgs.push_back(create_test_message(2, "a"));

    auto order = parse("int_field asc, string_field desc");
    std::ranges::sort(msgs, order);

    // Should be: (1,"b"), (1,"a"), (2,"b"), (2,"a")
    EXPECT_EQ(msgs[0].get_int_field(), 1);
    EXPECT_EQ(msgs[0].get_string_field(), "b");
    EXPECT_EQ(msgs[1].get_int_field(), 1);
    EXPECT_EQ(msgs[1].get_string_field(), "a");
    EXPECT_EQ(msgs[2].get_int_field(), 2);
    EXPECT_EQ(msgs[2].get_string_field(), "b");
    EXPECT_EQ(msgs[3].get_int_field(), 2);
    EXPECT_EQ(msgs[3].get_string_field(), "a");
}

TEST_F(AIPOrderingTest, NestedFieldOrdering) {
    auto msgs = std::vector<aip_test::test_message>{};
    msgs.push_back(create_test_message(1, "a", true, "zzz", 10));
    msgs.push_back(create_test_message(2, "b", true, "abc", 5));
    msgs.push_back(create_test_message(3, "c", true, "abc", 20));

    auto order = parse("nested.name asc, nested.value desc");
    std::ranges::sort(msgs, order);

    // Expect "abc", 20 first, then "abc", 5, then "zzz", 10
    EXPECT_EQ(msgs[0].get_nested().get_name(), "abc");
    EXPECT_EQ(msgs[0].get_nested().get_value(), 20);
    EXPECT_EQ(msgs[1].get_nested().get_name(), "abc");
    EXPECT_EQ(msgs[1].get_nested().get_value(), 5);
    EXPECT_EQ(msgs[2].get_nested().get_name(), "zzz");
    EXPECT_EQ(msgs[2].get_nested().get_value(), 10);
}

TEST_F(AIPOrderingTest, BoolFieldOrdering) {
    auto msgs = std::vector<aip_test::test_message>{};
    msgs.push_back(create_test_message(1, "a", true));
    msgs.push_back(create_test_message(2, "b", false));
    msgs.push_back(create_test_message(3, "c", true));

    auto order = parse("bool_field desc, int_field asc");
    std::ranges::sort(msgs, order);

    // Expect all 'true' before 'false', then by int_field
    EXPECT_TRUE(msgs[0].get_bool_field());
    EXPECT_TRUE(msgs[1].get_bool_field());
    EXPECT_FALSE(msgs[2].get_bool_field());
}

TEST_F(AIPOrderingTest, EnumFieldOrdering) {
    auto msgs = std::vector<aip_test::test_message>{};
    msgs.push_back(create_test_message(1, "a"));
    msgs.push_back(create_test_message(2, "b"));
    msgs[0].set_status(aip_test::test_message_status::status_inactive);
    msgs[1].set_status(aip_test::test_message_status::status_active);

    std::ranges::sort(msgs, parse("status"));
    EXPECT_EQ(
      msgs[0].get_status(), aip_test::test_message_status::status_active);
    EXPECT_EQ(
      msgs[1].get_status(), aip_test::test_message_status::status_inactive);

    std::ranges::sort(msgs, parse("status desc"));
    EXPECT_EQ(
      msgs[0].get_status(), aip_test::test_message_status::status_inactive);
    EXPECT_EQ(
      msgs[1].get_status(), aip_test::test_message_status::status_active);
}

TEST_F(AIPOrderingTest, DurationFieldOrdering) {
    auto msgs = std::vector<aip_test::test_message>{};

    auto& m1 = msgs.emplace_back();
    m1.set_duration_field(absl::Seconds(20));

    auto& m2 = msgs.emplace_back();
    m2.set_duration_field(absl::Seconds(10));

    auto order = parse("duration_field");

    std::ranges::sort(msgs, order);
    EXPECT_EQ(msgs[0].get_duration_field(), absl::Seconds(10));
    EXPECT_EQ(msgs[1].get_duration_field(), absl::Seconds(20));
}

TEST_F(AIPOrderingTest, TimestampFieldOrdering) {
    auto msgs = std::vector<aip_test::test_message>{};

    auto& m1 = msgs.emplace_back();
    m1.set_timestamp_field(absl::FromUnixMillis(20000));

    auto& m2 = msgs.emplace_back();
    m2.set_timestamp_field(absl::FromUnixMillis(10000));

    auto order = parse("timestamp_field");

    std::ranges::sort(msgs, order);
    EXPECT_EQ(msgs[0].get_timestamp_field(), absl::FromUnixMillis(10000));
    EXPECT_EQ(msgs[1].get_timestamp_field(), absl::FromUnixMillis(20000));
}

TEST_F(AIPOrderingTest, FloatFieldOrdering) {
    auto msgs = std::vector<aip_test::test_message>{};

    auto& m1 = msgs.emplace_back();
    m1.set_float_field(2.5f);

    auto& m2 = msgs.emplace_back();
    m2.set_float_field(1.5f);

    auto& m3 = msgs.emplace_back();
    m3.set_float_field(std::numeric_limits<float>::quiet_NaN());

    auto& m4 = msgs.emplace_back();
    m4.set_float_field(std::numeric_limits<float>::signaling_NaN());

    auto& m5 = msgs.emplace_back();
    m5.set_float_field(0.0f);

    auto& m6 = msgs.emplace_back();
    m6.set_float_field(-0.0f);

    auto& m7 = msgs.emplace_back();
    m7.set_float_field(std::numeric_limits<float>::infinity());

    auto& m8 = msgs.emplace_back();
    m8.set_float_field(-std::numeric_limits<float>::infinity());

    auto order = parse("float_field");
    std::ranges::sort(msgs, order);
    EXPECT_EQ(
      msgs[0].get_float_field(), -std::numeric_limits<float>::infinity());
    EXPECT_EQ(msgs[1].get_float_field(), -0.0f);
    EXPECT_TRUE(std::signbit(msgs[1].get_float_field()));
    EXPECT_EQ(msgs[2].get_float_field(), 0.0f);
    EXPECT_FALSE(std::signbit(msgs[2].get_float_field()));
    EXPECT_EQ(msgs[3].get_float_field(), 1.5f);
    EXPECT_EQ(msgs[4].get_float_field(), 2.5f);
    EXPECT_EQ(
      msgs[5].get_float_field(), std::numeric_limits<float>::infinity());
    EXPECT_TRUE(std::isnan(msgs[6].get_float_field()));
    EXPECT_TRUE(std::isnan(msgs[7].get_float_field()));
}

TEST_F(AIPOrderingTest, WhitespaceHandling) {
    auto msgs = std::vector<aip_test::test_message>{};
    msgs.push_back(create_test_message(3, "c"));
    msgs.push_back(create_test_message(1, "a"));
    msgs.push_back(create_test_message(2, "b"));

    auto order = parse(" int_field   ASC ,  string_field DESC ");
    std::ranges::sort(msgs, order);
    EXPECT_EQ(msgs[0].get_int_field(), 1);
    EXPECT_EQ(msgs[1].get_int_field(), 2);
    EXPECT_EQ(msgs[2].get_int_field(), 3);
}

TEST_F(AIPOrderingTest, InvalidArguments) {
    // Invalid field name
    EXPECT_THROW(parse("nonexistent_field"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("int_field, badfield"), rpc::invalid_argument_exception);

    // Invalid order specified
    EXPECT_THROW(
      parse("int_field upside_down"), rpc::invalid_argument_exception);
    EXPECT_THROW(
      parse("int_field, string_field foo"), rpc::invalid_argument_exception);

    // Empty ordering throws
    EXPECT_THROW(parse(""), rpc::invalid_argument_exception);
    EXPECT_THROW(parse(" , , "), rpc::invalid_argument_exception);

    // Unsupported field types
    EXPECT_THROW(parse("repeated_field"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("map_field"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("nested"), rpc::invalid_argument_exception);
}

} // namespace admin
