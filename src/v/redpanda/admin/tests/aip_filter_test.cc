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

#include "redpanda/admin/aip_filter.h"
#include "redpanda/admin/tests/aip_test_message_helpers.h"
#include "serde/protobuf/rpc.h"
#include "src/v/redpanda/admin/tests/aip_test_message.proto.h"

#include <fmt/ranges.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>

namespace admin {

namespace rpc = serde::pb::rpc;

class AIPFilterTest : public ::testing::Test {
protected:
    auto parse(std::string_view filter_expression) {
        auto config = admin::make_aip_filter_config<aip_test::test_message>(
          filter_expression);
        return aip_filter_parser::create_aip_filter(std::move(config));
    }
};

TEST_F(AIPFilterTest, IntegerFieldcomparisons) {
    auto msg = create_test_message(42);

    // Test all comparison operators for int64
    EXPECT_TRUE(parse("int_field = 42")(msg));
    EXPECT_TRUE(parse("int_field != 41")(msg));
    EXPECT_TRUE(parse("int_field < 43")(msg));
    EXPECT_TRUE(parse("int_field <= 42")(msg));
    EXPECT_TRUE(parse("int_field > 41")(msg));
    EXPECT_TRUE(parse("int_field >= 42")(msg));

    // Test zero and negative values
    msg.set_int_field(0);
    EXPECT_TRUE(parse("int_field = 0")(msg));
    msg.set_int_field(-42);
    EXPECT_TRUE(parse("int_field = -42")(msg));

    // Test int32 field (smaller integer type)
    msg.set_int32_field(12345);
    EXPECT_TRUE(parse("int32_field = 12345")(msg));
    EXPECT_FALSE(parse("int32_field = 12346")(msg));
    EXPECT_TRUE(parse("int32_field > 12344")(msg));
    EXPECT_TRUE(parse("int32_field < 12346")(msg));
}

TEST_F(AIPFilterTest, StringFieldComparisons) {
    auto msg = create_test_message(1, "client-b");

    // Basic string operations
    EXPECT_TRUE(parse(R"(string_field = "client-b")")(msg));
    EXPECT_TRUE(parse(R"(string_field != "client-a")")(msg));
    EXPECT_TRUE(parse(R"(string_field > "client-a")")(msg));
    EXPECT_TRUE(parse(R"(string_field < "client-c")")(msg));

    // Test only quoted strings work, unquoted strings throw
    msg.set_string_field(ss::sstring("test"));
    EXPECT_THROW(parse("string_field = test"), rpc::invalid_argument_exception);
    EXPECT_TRUE(parse(R"(string_field = "test")")(msg));

    // Test escape sequences in quoted strings
    msg.set_string_field(ss::sstring(R"(client"with"quotes)"));
    EXPECT_TRUE(parse(R"(string_field = "client\"with\"quotes")")(msg));

    msg.set_string_field(ss::sstring(R"(\)"));
    EXPECT_TRUE(parse(R"(string_field = "\\")")(msg));

    constexpr auto chars = std::to_array(
      {'\t', '\n', '\r', '\\', '\"', '\xC3', '\xA9'});
    msg.set_string_field(ss::sstring{chars.data(), chars.size()});
    EXPECT_TRUE(parse(R"(string_field = "\t\n\r\\\"\xC3\xA9")")(msg));
    EXPECT_THROW(
      parse(R"(string_field = "invalid-utf8-\xA")"),
      rpc::invalid_argument_exception);

    // Empty string
    msg.set_string_field(ss::sstring(""));
    EXPECT_TRUE(parse(R"(string_field = "")")(msg));

    // String with spaces
    msg.set_string_field(ss::sstring("client with spaces"));
    EXPECT_TRUE(parse(R"(string_field = "client with spaces")")(msg));
}

TEST_F(AIPFilterTest, BooleanFieldOperations) {
    auto msg_true = create_test_message(1, "test", true);
    auto msg_false = create_test_message(1, "test", false);

    // Only equality and inequality supported for booleans
    EXPECT_TRUE(parse("bool_field = true")(msg_true));
    EXPECT_TRUE(parse("bool_field = false")(msg_false));
    EXPECT_TRUE(parse("bool_field != false")(msg_true));
    EXPECT_TRUE(parse("bool_field != true")(msg_false));

    // Case sensitivity of true/false
    EXPECT_THROW(parse("bool_field = TRUE"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("bool_field = True"), rpc::invalid_argument_exception);

    // Only = and != allowed for boolean fields
    EXPECT_THROW(parse("bool_field > true"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("bool_field < false"), rpc::invalid_argument_exception);
}

TEST_F(AIPFilterTest, FloatingPointFieldOperations) {
    auto msg = create_test_message();

    // Test double field operations
    msg.set_double_field(5.5);
    EXPECT_TRUE(parse("double_field = 5.5")(msg));
    EXPECT_TRUE(parse("double_field > 5.4")(msg));
    EXPECT_TRUE(parse("double_field < 5.6")(msg));
    EXPECT_TRUE(parse("double_field != 5.4")(msg));

    // Test scientific notation for double
    msg.set_double_field(1e5);
    EXPECT_TRUE(parse("double_field = 1e5")(msg));
    EXPECT_TRUE(parse("double_field = 1E5")(msg));
    EXPECT_TRUE(parse("double_field = 100000")(msg));
    msg.set_double_field(-1.5e-10);
    EXPECT_TRUE(parse("double_field = -1.5e-10")(msg));

    // Test zero values
    msg.set_double_field(0.0);
    EXPECT_TRUE(parse("double_field = 0")(msg));
    EXPECT_TRUE(parse("double_field = -0.0")(msg));

    // Test float field (smaller floating-point type)
    msg.set_float_field(2.5f);
    EXPECT_TRUE(parse("float_field = 2.5")(msg));
    EXPECT_FALSE(parse("float_field = 2.6")(msg));
    EXPECT_TRUE(parse("float_field > 2.4")(msg));
    EXPECT_TRUE(parse("float_field < 2.6")(msg));
    EXPECT_TRUE(parse("float_field = 2.5e0")(msg));
}

TEST_F(AIPFilterTest, NestedFieldAccess) {
    auto msg = create_test_message(1, "test", true, "admin", 200);

    EXPECT_TRUE(parse("nested.name = \"admin\"")(msg));
    EXPECT_TRUE(parse("nested.value = 200")(msg));
    EXPECT_FALSE(parse("nested.name = \"other\"")(msg));

    msg.get_nested().set_name(ss::sstring(""));
    msg.get_nested().set_value(0);
    EXPECT_TRUE(parse("nested.name = \"\"")(msg));
    EXPECT_TRUE(parse("nested.value = 0")(msg));
}

TEST_F(AIPFilterTest, LogicalAndOperations) {
    auto msg = create_test_message(1, "test", false);

    // Basic AND operations
    EXPECT_TRUE(parse("int_field = 1 AND bool_field = false")(msg));
    EXPECT_FALSE(parse("int_field = 1 AND bool_field = true")(msg));
    EXPECT_FALSE(parse("int_field = 2 AND bool_field = false")(msg));

    // Multiple conditions
    EXPECT_TRUE(parse(
      "int_field = 1 AND bool_field = false AND string_field = \"test\"")(msg));

    // Case sensitivity for AND keyword
    EXPECT_THROW(
      parse("int_field = 1 and bool_field = false"),
      rpc::invalid_argument_exception);
    EXPECT_THROW(
      parse("int_field = 1 And bool_field = false"),
      rpc::invalid_argument_exception);
}

TEST_F(AIPFilterTest, EnumFieldOperations) {
    auto msg = create_test_message();

    // Test enum equality
    msg.set_status(aip_test::test_message_status::status_active);
    EXPECT_TRUE(parse("status = STATUS_ACTIVE")(msg));
    EXPECT_FALSE(parse("status = STATUS_INACTIVE")(msg));
    EXPECT_TRUE(parse("status != STATUS_INACTIVE")(msg));

    // Test unspecified value
    msg.set_status(aip_test::test_message_status::status_unspecified);
    EXPECT_TRUE(parse("status = STATUS_UNSPECIFIED")(msg));

    // Test case sensitivity
    msg.set_status(aip_test::test_message_status::status_active);
    EXPECT_FALSE(parse("status = status_active")(msg));

    // Only equality operators should work for enums
    EXPECT_THROW(
      parse("status > STATUS_ACTIVE"), rpc::invalid_argument_exception);
    EXPECT_THROW(
      parse("status < STATUS_INACTIVE"), rpc::invalid_argument_exception);
}

TEST_F(AIPFilterTest, DurationFieldOperations) {
    auto msg = create_test_message();

    // Test basic duration formats
    msg.set_duration_field(absl::Seconds(30));
    EXPECT_TRUE(parse("duration_field = 30s")(msg));

    msg.set_duration_field(absl::Milliseconds(1500));
    EXPECT_TRUE(parse("duration_field = 1.5s")(msg));

    msg.set_duration_field(absl::ZeroDuration());
    EXPECT_TRUE(parse("duration_field = 0s")(msg));

    msg.set_duration_field(absl::Milliseconds(1));
    EXPECT_TRUE(parse("duration_field = 0.001s")(msg));

    msg.set_duration_field(absl::Hours(5));
    EXPECT_TRUE(parse("duration_field = 5h")(msg));

    // All comparison operators should work for durations
    msg.set_duration_field(absl::Seconds(30));
    EXPECT_TRUE(parse("duration_field > 29s")(msg));
    EXPECT_TRUE(parse("duration_field < 31s")(msg));
    EXPECT_TRUE(parse("duration_field >= 30s")(msg));
    EXPECT_TRUE(parse("duration_field <= 30s")(msg));
    EXPECT_TRUE(parse("duration_field != 29s")(msg));
}

TEST_F(AIPFilterTest, TimestampFieldOperations) {
    auto msg = create_test_message();
    absl::Time test_time;
    std::string error;

    // Test RFC-3339 format
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00Z", &test_time, &error));
    msg.set_timestamp_field(std::move(test_time));
    EXPECT_TRUE(parse("timestamp_field = 2012-04-21T11:30:00Z")(msg));

    // Test with fractional seconds
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00.123Z", &test_time, &error));
    msg.set_timestamp_field(std::move(test_time));
    EXPECT_TRUE(parse("timestamp_field = 2012-04-21T11:30:00.123Z")(msg));

    // Test with timezone offset
    ASSERT_TRUE(
      absl::ParseTime(
        absl::RFC3339_full, "2012-04-21T11:30:00-04:00", &test_time, &error));
    msg.set_timestamp_field(std::move(test_time));
    EXPECT_TRUE(parse("timestamp_field = 2012-04-21T11:30:00-04:00")(msg));

    // All comparison operators should work for timestamps
    EXPECT_TRUE(parse("timestamp_field > 2012-04-21T11:29:00-04:00")(msg));
    EXPECT_TRUE(parse("timestamp_field < 2012-04-21T11:31:00-04:00")(msg));
}

TEST_F(AIPFilterTest, FieldOnLeftRequirement) {
    // AIP-160 requires field names on the left side of comparisons
    EXPECT_NO_THROW(parse("int_field = 42"));
    EXPECT_NO_THROW(parse("string_field = \"test\""));
    EXPECT_THROW(parse("42 = int_field"), rpc::invalid_argument_exception);
    EXPECT_THROW(
      parse("\"test\" = string_field"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("true = bool_field"), rpc::invalid_argument_exception);
}

TEST_F(AIPFilterTest, GrammarErrorHandling) {
    // Empty filter should always match
    auto msg = create_test_message();
    EXPECT_TRUE(parse("")(msg));

    // Unknown field errors
    EXPECT_THROW(parse("unknown_field = 1"), rpc::invalid_argument_exception);
    EXPECT_THROW(
      parse("int_field = 1 AND unknown_field = 2"),
      rpc::invalid_argument_exception);

    // Type mismatches
    EXPECT_THROW(
      parse("int_field = \"not_a_number\""), rpc::invalid_argument_exception);
    EXPECT_THROW(
      parse("bool_field = \"not_a_boolean\""), rpc::invalid_argument_exception);

    // Malformed expressions
    EXPECT_THROW(parse("int_field ="), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("= 1"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("int_field 1"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("int_field = 1 AND"), rpc::invalid_argument_exception);
}

TEST_F(AIPFilterTest, FieldPathValidation) {
    // Test malformed nested field paths
    EXPECT_THROW(parse("nested. = 1"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse(".nested = 1"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("nested..value = 1"), rpc::invalid_argument_exception);
    EXPECT_THROW(
      parse("nested.nonexistent = 1"), rpc::invalid_argument_exception);

    // Test invalid field name formats
    EXPECT_THROW(parse("123field = 1"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("-field = 1"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("field- = 1"), rpc::invalid_argument_exception);

    // Case sensitivity for field names
    EXPECT_THROW(parse("INT_FIELD = 1"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("Int_Field = 1"), rpc::invalid_argument_exception);
}

TEST_F(AIPFilterTest, StringEscapingErrors) {
    // Test unterminated quoted strings
    EXPECT_THROW(
      parse(R"(string_field = "unclosed)"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse(R"(string_field = ")"), rpc::invalid_argument_exception);
    EXPECT_THROW(
      parse(R"(string_field = "\)"), rpc::invalid_argument_exception);

    // Test invalid escape sequences that should fail
    EXPECT_THROW(
      parse(R"(string_field = "\x")"), rpc::invalid_argument_exception);
}

TEST_F(AIPFilterTest, NumberFormatValidation) {
    // Test invalid number formats
    EXPECT_THROW(parse("int_field = 1.2.3"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("int_field = .123"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("int_field = 123."), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("int_field = --1"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("int_field = ++1"), rpc::invalid_argument_exception);

    // Test invalid scientific notation
    EXPECT_THROW(parse("double_field = 123e"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("double_field = e123"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("double_field = 1e+"), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("double_field = 1e-"), rpc::invalid_argument_exception);
}

TEST_F(AIPFilterTest, TimeFormatValidation) {
    // Test invalid duration formats
    EXPECT_THROW(
      parse("duration_field = invalid"), rpc::invalid_argument_exception);
    EXPECT_THROW(
      parse("duration_field = 120"),
      rpc::invalid_argument_exception); // Missing 's'
    EXPECT_THROW(
      parse("duration_field = s"),
      rpc::invalid_argument_exception); // No number

    // Test invalid timestamp formats
    EXPECT_THROW(
      parse("timestamp_field = \"invalid-timestamp\""),
      rpc::invalid_argument_exception);
    EXPECT_THROW(
      parse("timestamp_field = \"2012-04-21 11:30:00\""),
      rpc::invalid_argument_exception); // Missing T
    EXPECT_THROW(
      parse("timestamp_field = \"2012-04-21T25:30:00Z\""),
      rpc::invalid_argument_exception); // Invalid hour
    EXPECT_THROW(
      parse("timestamp_field = \"2012-13-21T11:30:00Z\""),
      rpc::invalid_argument_exception); // Invalid month
}

TEST_F(AIPFilterTest, WhitespaceHandling) {
    auto msg = create_test_message(1, "test", false);

    // Various whitespace formats should work
    EXPECT_TRUE(parse("int_field=1")(msg));
    EXPECT_TRUE(parse("int_field = 1")(msg));
    EXPECT_TRUE(parse("  int_field  =  1  ")(msg));

    // AND with whitespace
    EXPECT_TRUE(parse("int_field=1 AND bool_field=false")(msg));
    EXPECT_TRUE(parse("  int_field  =  1  AND  bool_field  =  false  ")(msg));

    // Missing space around AND should fail
    EXPECT_THROW(
      parse("int_field=1AND bool_field=false"),
      rpc::invalid_argument_exception);
}

TEST_F(AIPFilterTest, FilterLengthLimits) {
    // Test filter at reasonable length
    std::string reasonable_filter
      = "int_field = 42 AND string_field = \"test\"";
    EXPECT_NO_THROW(parse(reasonable_filter));

    // Test filter over the limit (1024 characters)
    constexpr auto length = aip_filter_parser::max_filter_length + 1;
    std::string oversized_filter(length, 'x');
    EXPECT_THROW(parse(oversized_filter), rpc::invalid_argument_exception);

    // Verify the error message mentions the limit
    try {
        parse(oversized_filter);
        FAIL() << "Expected exception for oversized filter";
    } catch (const rpc::invalid_argument_exception& e) {
        EXPECT_THAT(std::string(e.what()), ::testing::HasSubstr("1024"));
    }
}

TEST_F(AIPFilterTest, PredicateReusability) {
    auto predicate = parse("int_field = 1 AND bool_field = false");

    // Same predicate can be used multiple times
    auto msg1 = create_test_message(1, "uid1", false);
    auto msg2 = create_test_message(1, "uid2", false);
    auto msg3 = create_test_message(2, "uid3", false);

    EXPECT_TRUE(predicate(msg1));
    EXPECT_TRUE(predicate(msg2));
    EXPECT_FALSE(predicate(msg3));
}

TEST_F(AIPFilterTest, IntegerBoundaryValues) {
    auto msg = create_test_message();

    // Test basic boundary values
    msg.set_int_field(std::numeric_limits<int64_t>::max());
    EXPECT_TRUE(parse("int_field = 9223372036854775807")(msg));

    msg.set_int_field(std::numeric_limits<int64_t>::min());
    EXPECT_TRUE(parse("int_field = -9223372036854775808")(msg));

    // Test unsigned boundaries
    msg.set_uint_field(std::numeric_limits<uint64_t>::max());
    EXPECT_TRUE(parse("uint_field = 18446744073709551615")(msg));

    // Test int32 boundary values
    msg.set_int32_field(std::numeric_limits<int32_t>::max());
    EXPECT_TRUE(parse("int32_field = 2147483647")(msg));
    EXPECT_THROW(
      parse("int32_field < 2147483650"), rpc::invalid_argument_exception);

    msg.set_int32_field(std::numeric_limits<int32_t>::min());
    EXPECT_TRUE(parse("int32_field = -2147483648")(msg));

    // Negative values should fail for unsigned types
    EXPECT_THROW(parse("uint_field = -1"), rpc::invalid_argument_exception);

    // Test overflow conditions
    EXPECT_THROW(
      parse("int_field = 9223372036854775808"),
      rpc::invalid_argument_exception); // max + 1
}

TEST_F(AIPFilterTest, CardinalityKeywords) {
    auto msg = aip_test::test_message{};

    // Sanity check optional field presence
    EXPECT_FALSE(msg.has_optional_int_field());

    // TODO(CORE-13425): this should be false as per the AIP-160, but requires
    // improvements to our protogen's reflection support
    EXPECT_TRUE(parse("optional_int_field = 0")(msg));

    EXPECT_THROW(
      parse("repeated_int_field = 0")(msg), rpc::invalid_argument_exception);
    EXPECT_THROW(parse("map_field = 0")(msg), rpc::invalid_argument_exception);
}

} // namespace admin
