/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/transform.h"
#include "iceberg/transform_utils.h"
#include "iceberg/values.h"
#include "model/timestamp.h"

#include <fmt/chrono.h>
#include <gtest/gtest.h>

#include <variant>

using namespace iceberg;
using namespace std::chrono_literals;
using t_clock_t = std::chrono::system_clock;
namespace {
value make_timestamp_val(std::chrono::time_point<t_clock_t> tp) {
    return timestamp_value{tp.time_since_epoch() / 1us};
}
value make_timestamp_val(std::chrono::microseconds time_shift) {
    return timestamp_value{time_shift / 1us};
}

value make_date_val(std::chrono::microseconds time_shift) {
    auto days = std::chrono::floor<std::chrono::days>(time_shift);
    return date_value{static_cast<int32_t>(days.count())};
}
} // namespace

TEST(TestTransforms, TestHourlyTransform) {
    auto start_time = std::chrono::system_clock::now();

    auto start_transformed = apply_transform(
      make_timestamp_val(start_time), hour_transform{});

    ASSERT_TRUE(std::holds_alternative<primitive_value>(start_transformed));
    ASSERT_TRUE(std::holds_alternative<int_value>(
      std::get<primitive_value>(start_transformed)));
    auto start_val = std::get<int_value>(
      std::get<primitive_value>(start_transformed));

    ASSERT_EQ(
      start_val.val,
      std::chrono::floor<std::chrono::hours>(start_time)
        .time_since_epoch()
        .count());

    auto plus_1hr = start_time + 1h;
    auto plus_1hr_transformed = apply_transform(
      make_timestamp_val(plus_1hr), hour_transform{});
    ASSERT_NE(start_transformed, plus_1hr_transformed);
    ASSERT_TRUE(std::holds_alternative<primitive_value>(plus_1hr_transformed));
    ASSERT_TRUE(std::holds_alternative<int_value>(
      std::get<primitive_value>(plus_1hr_transformed)));
    auto plus_1hr_val = std::get<int_value>(
      std::get<primitive_value>(plus_1hr_transformed));
    ASSERT_EQ(start_val.val + 1, plus_1hr_val.val);

    auto minus_1hr = start_time - 1h;
    auto minus_1hr_transformed = apply_transform(
      make_timestamp_val(minus_1hr), hour_transform{});
    ASSERT_NE(start_transformed, minus_1hr_transformed);
    ASSERT_TRUE(std::holds_alternative<primitive_value>(minus_1hr_transformed));
    ASSERT_TRUE(std::holds_alternative<int_value>(
      std::get<primitive_value>(minus_1hr_transformed)));
    auto minus_1hr_val = std::get<int_value>(
      std::get<primitive_value>(minus_1hr_transformed));
    ASSERT_EQ(start_val.val - 1, minus_1hr_val.val);
}

struct time_transform_test_case {
    std::chrono::microseconds time_shift;
    transform tr;
    int32_t expected_result;
};

const std::vector<time_transform_test_case> time_transform_test_cases{
  time_transform_test_case{
    .time_shift = 0s, .tr = hour_transform{}, .expected_result = 0},
  time_transform_test_case{
    .time_shift = 0s, .tr = day_transform{}, .expected_result = 0},
  time_transform_test_case{
    .time_shift = 0s, .tr = month_transform{}, .expected_result = 0},
  time_transform_test_case{
    .time_shift = 0s, .tr = year_transform{}, .expected_result = 0},
  time_transform_test_case{
    .time_shift = 10s, .tr = hour_transform{}, .expected_result = 0},
  time_transform_test_case{
    .time_shift = -10s, .tr = hour_transform{}, .expected_result = -1},
  time_transform_test_case{
    .time_shift = -10h, .tr = hour_transform{}, .expected_result = -10},
  time_transform_test_case{
    .time_shift = -(10h + 1us), .tr = hour_transform{}, .expected_result = -11},
  time_transform_test_case{
    .time_shift = std::chrono::days(-1) - 1us,
    .tr = hour_transform{},
    .expected_result = -25},
  time_transform_test_case{
    .time_shift = std::chrono::days(-1) - 1us,
    .tr = day_transform{},
    .expected_result = -2},
  time_transform_test_case{
    .time_shift = std::chrono::days(-1) - 1us,
    .tr = month_transform{},
    .expected_result = -1},
  time_transform_test_case{
    .time_shift = std::chrono::days(-1) - 1us,
    .tr = year_transform{},
    .expected_result = -1},
  time_transform_test_case{
    .time_shift = std::chrono::years(100),
    .tr = month_transform{},
    .expected_result = 1199},
  time_transform_test_case{
    .time_shift = std::chrono::years(100),
    .tr = year_transform{},
    .expected_result = 99},
  time_transform_test_case{
    .time_shift = std::chrono::years(100),
    .tr = day_transform{},
    .expected_result = 36524,
  },
  time_transform_test_case{
    .time_shift = std::chrono::years(100),
    .tr = hour_transform{},
    .expected_result = 876582},
};

std::vector<time_transform_test_case> date_transform_test_cases() {
    std::vector<time_transform_test_case> ret;

    for (const auto& tc : time_transform_test_cases) {
        if (std::holds_alternative<hour_transform>(tc.tr)) {
            continue;
        }
        ret.push_back(tc);
    }
    return ret;
};

struct TestTimeTransforms
  : public testing::TestWithParam<time_transform_test_case> {};
struct TestDateTransforms
  : public testing::TestWithParam<time_transform_test_case> {};

TEST_P(TestTimeTransforms, TestConversion) {
    auto test_case = GetParam();

    auto transformed = apply_transform(
      make_timestamp_val(test_case.time_shift), test_case.tr);
    ASSERT_TRUE(std::holds_alternative<primitive_value>(transformed));
    ASSERT_TRUE(std::holds_alternative<int_value>(
      std::get<primitive_value>(transformed)));
    auto transformed_val = std::get<int_value>(
      std::get<primitive_value>(transformed));

    ASSERT_EQ(transformed_val.val, test_case.expected_result);
}

TEST_P(TestDateTransforms, TestConversion) {
    auto test_case = GetParam();

    auto transformed = apply_transform(
      make_date_val(test_case.time_shift), test_case.tr);
    ASSERT_TRUE(std::holds_alternative<primitive_value>(transformed));
    ASSERT_TRUE(std::holds_alternative<int_value>(
      std::get<primitive_value>(transformed)));
    auto transformed_val = std::get<int_value>(
      std::get<primitive_value>(transformed));

    ASSERT_EQ(transformed_val.val, test_case.expected_result);
}

INSTANTIATE_TEST_SUITE_P(
  TestAllTimeTimeTransforms,
  TestTimeTransforms,
  ::testing::ValuesIn(time_transform_test_cases));
INSTANTIATE_TEST_SUITE_P(
  TestAllTimeTimeTransforms,
  TestDateTransforms,
  ::testing::ValuesIn(date_transform_test_cases()));

struct primitive_test_values {
    value boolean_val = primitive_value{boolean_value{true}};
    value int_val = primitive_value{int_value{321123}};
    value long_val = primitive_value{long_value{321123321123}};
    value float_val = primitive_value{float_value{3.14}};
    value double_val = primitive_value{double_value{6.23}};
    value date_val = primitive_value{date_value{1000}};
    value time_val = primitive_value{time_value{std::chrono::hours(10) / 1us}};
    value timestamp_val = primitive_value{timestamp_value{1741177530000000}};
    value timestamptz_val = primitive_value{
      timestamptz_value{1741177530000000}};
    value string_val = primitive_value{
      string_value{iobuf::from("hello Iceberg")}};
    value uuid_val = primitive_value{
      uuid_value{uuid_t::from_string("ab4ed576-b638-424f-89d8-4ea602393772")}};
    value fixed_val = primitive_value{
      fixed_value{iobuf::from("fixed Iceberg")}};
    value binary_val = primitive_value{binary_value{iobuf::from("bytes ?")}};
    value decimal_val = primitive_value{
      decimal_value{absl::MakeInt128(0, 123)}};
    value decimal_val_2 = primitive_value{
      decimal_value{absl::MakeInt128(1, 123)}};
    value decimal_val_3 = primitive_value{
      decimal_value{absl::MakeInt128(6321412421, 53441242)}};
    value decimal_val_4 = primitive_value{
      decimal_value{absl::MakeInt128(0, 123)}};
};

TEST(TestTransformApplication, IdentityTransform) {
    primitive_test_values test_values;

    auto test_transform = [](const value& val) {
        auto transformed = apply_transform(val, identity_transform{});
        ASSERT_EQ(val, transformed);
    };

    test_transform(test_values.boolean_val);
    test_transform(test_values.int_val);
    test_transform(test_values.long_val);
    test_transform(test_values.float_val);
    test_transform(test_values.double_val);
    test_transform(test_values.date_val);
    test_transform(test_values.time_val);
    test_transform(test_values.timestamp_val);
    test_transform(test_values.timestamptz_val);
    test_transform(test_values.string_val);
    test_transform(test_values.uuid_val);
    test_transform(test_values.fixed_val);
    test_transform(test_values.binary_val);
    test_transform(test_values.decimal_val);
}
