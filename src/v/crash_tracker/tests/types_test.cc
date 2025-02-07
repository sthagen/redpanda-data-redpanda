// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "crash_tracker/types.h"

#include <fmt/core.h>
#include <gtest/gtest.h>

#include <string_view>

namespace crash_tracker {

class TestReservedStringSerde : public testing::Test {};

auto run_test_case(std::string_view print_str, std::string_view limited_str) {
    // Write print_str into the reserved_string, it may be more or less than the
    // capacity of the reserved_string, so limited_str is what fits into the
    // reserved_string
    auto rs_in = reserved_string<5>{};
    auto res = fmt::format_to_n(
      rs_in.begin(), rs_in.capacity(), "{}", print_str);
    ASSERT_EQ(res.size, print_str.size());
    ASSERT_EQ(rs_in.c_str(), limited_str);

    // Round trip serde (expect no exception)
    iobuf buf;
    serde::write(buf, std::move(rs_in));
    auto parser = iobuf_parser{std::move(buf)};
    auto rs_out = serde::read<decltype(rs_in)>(parser);

    // Assert that the result is the same as rs_in was
    ASSERT_EQ(rs_out.c_str(), limited_str);
}

TEST_F(TestReservedStringSerde, AtCapacity) {
    constexpr auto five_char_string = "12345";
    run_test_case(five_char_string, five_char_string);
}

TEST_F(TestReservedStringSerde, BelowCapacity) {
    constexpr auto three_char_string = "123";
    run_test_case(three_char_string, three_char_string);
}

TEST_F(TestReservedStringSerde, AboveCapacity) {
    run_test_case("123456", "12345");
}

} // namespace crash_tracker
