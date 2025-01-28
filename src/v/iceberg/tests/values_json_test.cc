/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/random.h"
#include "gmock/gmock.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/values.h"
#include "iceberg/values_json.h"
#include "json/document.h"
#include "json/json.h"

#include <absl/numeric/int128.h>
#include <gtest/gtest.h>

#include <ranges>

using namespace iceberg;

namespace {

template<typename T>
concept StructuredValue = std::is_same_v<T, struct_value>
                          || std::is_same_v<T, list_value>
                          || std::is_same_v<T, map_value>;

struct values_json_test_case {
    values_json_test_case() = default;
    values_json_test_case(
      std::string description,
      primitive_value val,
      field_type type,
      std::string expect = "")
      : desc(std::move(description))
      , val(std::move(val))
      , type(std::move(type))
      , json_rep(std::move(expect)) {}

    values_json_test_case(
      std::string description, field_type type, std::string expect = "")
      : desc(std::move(description))
      , val(int_value{})
      , type(std::move(type))
      , json_rep(std::move(expect)) {}
    template<StructuredValue T>
    values_json_test_case(
      std::string description,
      std::unique_ptr<T> val,
      field_type type,
      std::string expect = "")
      : desc(std::move(description))
      , val(std::move(val))
      , type(std::move(type))
      , json_rep(std::move(expect)) {}
    values_json_test_case(const values_json_test_case& other)
      : desc(other.desc)
      , val(make_copy(other.val))
      , type(make_copy(other.type))
      , json_rep(other.json_rep) {}
    values_json_test_case(values_json_test_case&& other) noexcept
      : desc(std::move(other.desc))
      , val(std::move(other.val))
      , type(std::move(other.type))
      , json_rep(std::move(other.json_rep)) {}

    ~values_json_test_case() = default;

    values_json_test_case& operator=(const values_json_test_case&) = delete;
    values_json_test_case& operator=(values_json_test_case&&) = delete;

    friend std::ostream&
    operator<<(std::ostream& os, const values_json_test_case&);

    std::string desc;
    value val;
    field_type type;
    std::string json_rep;
};

std::ostream& operator<<(std::ostream& os, const values_json_test_case& tc) {
    return os << tc.desc;
}

template<typename T>
ss::sstring json_str(T v) {
    json::StringBuffer buf;
    json::Writer<json::StringBuffer> writer(buf);
    rjson_serialize(writer, v);
    return buf.GetString();
}

static auto some_uuid = uuid_t::create();
static auto some_random_bytes = random_generators::get_bytes();

static const std::vector<values_json_test_case> test_cases{
  values_json_test_case{
    "boolean_value false",
    boolean_value{false},
    boolean_type{},
    "false",
  },
  values_json_test_case{
    "boolean_value true",
    boolean_value{true},
    boolean_type{},
    "true",
  },
  values_json_test_case{
    "int_value max",
    int_value{std::numeric_limits<int>::max()},
    int_type{},
    json_str<int>(std::numeric_limits<int>::max()),
  },
  values_json_test_case{
    "int_value min",
    int_value{std::numeric_limits<int>::min()},
    int_type{},
    json_str<int>(std::numeric_limits<int>::min()),
  },
  values_json_test_case{
    "long_value max",
    long_value{std::numeric_limits<long>::max()},
    long_type{},
    json_str<long>(std::numeric_limits<long>::max()),
  },
  values_json_test_case{
    "long_value min",
    long_value{std::numeric_limits<long>::min()},
    long_type{},
    json_str<long>(std::numeric_limits<long>::min()),
  },
  values_json_test_case{
    "long_value (short range value)",
    long_value{1},
    long_type{},
    json_str<long>(1),
  },
  values_json_test_case{
    "float_value max",
    float_value{std::numeric_limits<float>::max() / 10},
    float_type{},
    json_str<float>(std::numeric_limits<float>::max() / 10),
  },
  values_json_test_case{
    "float_value min",
    float_value{std::numeric_limits<float>::min() * 10},
    float_type{},
    json_str<float>(std::numeric_limits<float>::min() * 10),
  },
  values_json_test_case{
    "double_value max",
    double_value{std::numeric_limits<double>::max()},
    double_type{},
    json_str<double>(std::numeric_limits<double>::max()),
  },
  values_json_test_case{
    "double_value min",
    double_value{std::numeric_limits<double>::min()},
    double_type{},
    json_str<double>(std::numeric_limits<double>::min()),
  },
  values_json_test_case{
    "date_value",
    date_value{365},
    date_type{},
    R"("1971-01-01")",
  },
  values_json_test_case{
    "time_value",
    time_value{(24L * 60L * 60L) * 1000000L - 999999L},
    time_type{},
    R"("23:59:59.000001")",
  },
  values_json_test_case{
    "timestamp_value",
    timestamp_value{365ul * 24ul * 60ul * 60ul * 1000000ul + 23},
    timestamp_type{},
    R"("1971-01-01T00:00:00.000023")",
  },
  values_json_test_case{
    "timestamptz_value",
    timestamptz_value{365ul * 24ul * 60ul * 60ul * 1000000ul + 23},
    timestamptz_type{},
    R"("1971-01-01T00:00:00.000023+00:00")",
  },
  values_json_test_case{
    "string_value",
    string_value{bytes_to_iobuf(bytes::from_string("foobar"))},
    string_type{},
    R"("foobar")",
  },
  values_json_test_case{
    "uuid_value",
    uuid_value{some_uuid},
    uuid_type{},
    fmt::format(R"("{}")", some_uuid),
  },
  values_json_test_case{
    "fixed_value",
    fixed_value{bytes_to_iobuf(bytes{1, 2, 3, 4, 5, 6, 7, 8, 255})},
    fixed_type{9},
    fmt::format(R"("{}")", to_hex(bytes{1, 2, 3, 4, 5, 6, 7, 8, 255})),
  },
  values_json_test_case{
    "binary_value",
    binary_value{bytes_to_iobuf(some_random_bytes)},
    binary_type{},
    fmt::format(R"("{}")", to_hex(some_random_bytes)),
  },
  values_json_test_case{
    "decimal_value",
    decimal_value{absl::int128{std::numeric_limits<long>::max()}},
    decimal_type{21, 8},
    R"("92233720368.54775807")",
  },
  values_json_test_case{
    "decimal_value (trailing decimal point)",
    decimal_value{absl::int128{std::numeric_limits<long>::max()}},
    decimal_type{21, 0},
    R"("9223372036854775807.")",
  },
  values_json_test_case{
    "struct_value",
    std::make_unique<struct_value>([]() -> decltype(struct_value::fields) {
        decltype(struct_value::fields) vec;
        vec.emplace_back(int_value{23});
        vec.emplace_back(
          string_value{bytes_to_iobuf(bytes::from_string("foobar"))});
        vec.emplace_back(
          std::make_unique<list_value>([]() -> decltype(list_value::elements) {
              decltype(list_value::elements) vec;
              for (auto i : std::views::iota(0, 4)) {
                  if (i == 0) {
                      vec.emplace_back(std::nullopt);
                  } else {
                      vec.emplace_back(int_value{i});
                  }
              }
              return vec;
          }()));
        return vec;
    }()),
    []() -> field_type {
        struct_type s;
        s.fields.emplace_back(
          nested_field::create(8, "field_one", field_required::no, int_type{}));
        s.fields.emplace_back(nested_field::create(
          9, "field_two", field_required::no, string_type{}));
        s.fields.emplace_back(nested_field::create(
          10,
          "field_three",
          field_required::no,
          list_type::create(4, field_required::no, int_type{})));
        return s;
    }(),
    R"({"8":23,"9":"foobar","10":[null,1,2,3]})",
  },
  values_json_test_case{
    "list_value",
    std::make_unique<list_value>([]() -> decltype(list_value::elements) {
        decltype(list_value::elements) vec;
        for (auto i : std::views::iota(0, 3)) {
            vec.emplace_back(std::make_unique<struct_value>(
              [i]() -> decltype(struct_value::fields) {
                  decltype(struct_value::fields) vec;
                  if (i == 0) {
                      vec.emplace_back(string_value{
                        bytes_to_iobuf(bytes::from_string("foobar"))});
                  } else {
                      vec.emplace_back(std::nullopt);
                  }
                  return vec;
              }()));
        }
        return vec;
    }()),
    list_type::create(
      1,
      field_required::no,
      []() -> field_type {
          struct_type s;
          s.fields.emplace_back(
            nested_field::create(42, "key", field_required::no, string_type{}));
          return s;
      }()),
    R"([{"42":"foobar"},{"42":null},{"42":null}])",
  },
  values_json_test_case{
    "map_value",
    std::make_unique<map_value>([]() -> decltype(map_value::kvs) {
        decltype(map_value::kvs) vec;
        vec.emplace_back(kv_value{
          int_value{23},
          string_value{bytes_to_iobuf(bytes::from_string("foobar"))}});
        vec.emplace_back(kv_value{int_value{42}, std::nullopt});
        return vec;
    }()),
    map_type::create(1, int_type{}, 2, field_required::no, string_type{}),
    R"({"keys":[23,42],"values":["foobar",null]})",
  },
};

struct ValuesJsonTestBase
  : ::testing::Test
  , testing::WithParamInterface<values_json_test_case> {
    const auto& get_value() const { return GetParam().val; }
    const field_type& get_type() const { return GetParam().type; }
    std::string_view get_json_rep() const { return GetParam().json_rep; }
    std::string_view get_description() const { return GetParam().desc; }
};

} // namespace

struct ValuesJsonTest : public ValuesJsonTestBase {};

INSTANTIATE_TEST_SUITE_P(
  ValuesJsonSerde, ValuesJsonTest, ::testing::ValuesIn(test_cases));

std::string value_to_json_str(const value& v, const field_type& t) {
    json::chunked_buffer buf;
    iceberg::json_writer w(buf);
    value_to_json(w, v, t);
    auto p = iobuf_parser(std::move(buf).as_iobuf());
    std::string str;
    str.resize(p.bytes_left());
    p.consume_to(p.bytes_left(), str.data());
    validate_utf8(str);
    return str;
}

TEST_P(ValuesJsonTest, CanSerializeValues) {
    std::string s;
    ASSERT_NO_THROW(s = value_to_json_str(get_value(), get_type()));
    ASSERT_EQ(s, get_json_rep());
}

TEST_P(ValuesJsonTest, CanDeSerializeValues) {
    json::Document parsed;
    parsed.Parse(get_json_rep().data());
    std::optional<value> parsed_value;
    ASSERT_NO_THROW(
      parsed_value = value_from_json(parsed, get_type(), field_required::yes));
    EXPECT_EQ(parsed_value, get_value());
}

namespace {
static const std::vector<values_json_test_case> serialization_errs{
  values_json_test_case{
    "type mismatch for value",
    boolean_value{false},
    struct_type{},
  },
  values_json_test_case{
    // TODO(oren): too strict maybe?
    "Wrong number of fields for struct",
    std::make_unique<struct_value>(),
    []() -> field_type {
        struct_type s{};
        s.fields.emplace_back(
          nested_field::create(0, "foo", field_required::no, int_type{}));
        return s;
    }(),
  },
  values_json_test_case{
    "Found null nested field",
    std::make_unique<struct_value>([]() -> struct_value {
        struct_value v;
        v.fields.emplace_back(int_value{1});
        return v;
    }()),
    []() -> field_type {
        struct_type s;
        s.fields.emplace_back(nullptr);
        return s;
    }(),
  },
  values_json_test_case{
    "Found null value for required field foo",
    std::make_unique<struct_value>([]() -> struct_value {
        struct_value v;
        v.fields.emplace_back(std::nullopt);
        return v;
    }()),
    []() -> field_type {
        struct_type s;
        s.fields.emplace_back(
          nested_field::create(0, "foo", field_required::yes, int_type{}));
        return s;
    }(),
  },
  values_json_test_case{
    "Malformed list_type",
    std::make_unique<list_value>(),
    []() -> field_type {
        auto l = list_type::create(0, field_required::yes, int_type{});
        l.element_field = nullptr;
        return l;
    }(),
  },
  values_json_test_case{
    "Found null value for required list element",
    std::make_unique<list_value>([]() -> list_value {
        list_value l{};
        l.elements.emplace_back(std::nullopt);
        return l;
    }()),
    list_type::create(0, field_required::yes, int_type{}),
  },
  values_json_test_case{
    "Malformed map_type",
    std::make_unique<map_value>(),
    []() -> field_type {
        auto m = map_type::create(
          0, int_type{}, 1, field_required::yes, int_type{});
        m.key_field = nullptr;
        return m;
    }(),
  },
  values_json_test_case{
    "Found null value for required map value for key 'int(2)'",
    std::make_unique<map_value>([]() -> map_value {
        map_value m;
        m.kvs.emplace_back(int_value{2}, std::nullopt);
        return m;
    }()),
    []() -> field_type {
        auto m = map_type::create(
          0, int_type{}, 1, field_required::yes, int_type{});
        return m;
    }(),
  },
  values_json_test_case{
    "Expected fixed_value of type",
    fixed_value{bytes_to_iobuf(bytes{1, 2, 3, 4, 5, 6, 7, 8, 9})},
    fixed_type{2},
  },
};

} // namespace

struct ValuesJsonSerializationErrorsTest : public ValuesJsonTestBase {};

INSTANTIATE_TEST_SUITE_P(
  ValuesJsonSerde,
  ValuesJsonSerializationErrorsTest,
  ::testing::ValuesIn(serialization_errs));

TEST_P(ValuesJsonSerializationErrorsTest, SerializationFailsGracefully) {
    ASSERT_THROW(
      {
          try {
              value_to_json_str(get_value(), get_type());
          } catch (const std::invalid_argument& e) {
              EXPECT_THAT(e.what(), testing::HasSubstr(get_description()));
              throw;
          }
      },
      std::invalid_argument);
}

namespace {
static const std::vector<values_json_test_case> parse_errs{
  values_json_test_case{
    "Unexpected JSON null",
    int_type{},
    "null",
  },

  /* LIST TESTS */
  values_json_test_case{
    "Expected a JSON array for list_value",
    list_type::create(0, field_required::no, int_type{}),
    "{}",
  },
  values_json_test_case{
    "Unexpected JSON null for required int",
    list_type::create(0, field_required::yes, int_type{}),
    "[null]",
  },

  /* STRUCT TESTS  */
  values_json_test_case{
    "Expected a JSON object for struct_value",
    struct_type{},
    "1",
  },
  values_json_test_case{
    "Expected JSON object with 0 members, got 1",
    struct_type{},
    R"({"1":2})",
  },
  values_json_test_case{
    "Null field in struct type",
    []() -> struct_type {
        struct_type s{};
        s.fields.emplace_back(nullptr);
        return s;
    }(),
    R"({"1":2})",
  },
  values_json_test_case{
    "Expected int value, got 5",
    []() -> struct_type {
        struct_type s{};
        s.fields.emplace_back(
          nested_field::create(0, "foo", field_required::yes, int_type{}));
        return s;
    }(),
    R"({"1":"foobar"})",
  },

  /* MAP TESTS */
  values_json_test_case{
    "No member named 'values'",
    map_type::create(0, int_type{}, 1, field_required::no, int_type{}),
    R"({"keys":[],"not_values":[]})",
  },
  values_json_test_case{
    "Expected array for field 'keys'",
    map_type::create(0, int_type{}, 1, field_required::no, int_type{}),
    R"({"keys":{},"values":[]})",
  },
  values_json_test_case{
    "No member named 'keys'",
    map_type::create(0, int_type{}, 1, field_required::no, int_type{}),
    R"({"values":[]})",
  },
  values_json_test_case{
    "Expected complete key-value mapping, got 1 keys and 0 values",
    map_type::create(0, int_type{}, 1, field_required::no, int_type{}),
    R"({"keys":[1],"values":[]})",
  },
  values_json_test_case{
    "Expected int value, got 5",
    map_type::create(0, int_type{}, 1, field_required::no, int_type{}),
    R"({"keys":["foobar"],"values":[3]})",
  },
  values_json_test_case{
    "Expected int value, got 5",
    map_type::create(0, int_type{}, 1, field_required::no, int_type{}),
    R"({"keys":[1],"values":["string"]})",
  },
  values_json_test_case{
    "Unexpected JSON null for required int",
    map_type::create(0, int_type{}, 1, field_required::no, int_type{}),
    R"({"keys":[null],"values":[2]})",
  },
  values_json_test_case{
    "Unexpected JSON null for required int",
    map_type::create(0, int_type{}, 1, field_required::yes, int_type{}),
    R"({"keys":[1],"values":[null]})",
  },

  /* PRIMITIVE TESTS */
  values_json_test_case{
    "Expected int value, got 2",
    int_type{},
    "true",
  },
  values_json_test_case{
    "Expected int value, got 6",
    int_type{},
    json_str<long>(std::numeric_limits<long>::max()),
  },
  values_json_test_case{
    "Expected float value, got double",
    float_type{},
    json_str<double>(std::numeric_limits<double>::max()),
  },
  values_json_test_case{
    "Expected float value",
    float_type{},
    json_str<short>(std::numeric_limits<short>::max()),
  },
  values_json_test_case{
    "Expected JSON string for date_value, got 6",
    date_type{},
    "23",
  },
  values_json_test_case{
    "Failed to parse date string 'foobar', expected format '%F'",
    date_type{},
    R"("foobar")",
  },
  values_json_test_case{
    "Expected fractional part for time_value, got '00:00:00'",
    time_type{},
    R"("00:00:00")",
  },
  values_json_test_case{
    "Expected 6-digit microsecond resolution, got '111'",
    time_type{},
    R"("00:00:00.111")",
  },
  values_json_test_case{
    "Failed to parse microseconds: 'foobar'",
    time_type{},
    R"("00:00:00.foobar")",
  },
  values_json_test_case{
    "Failed to parse date string '00::00', expected format '%T'",
    time_type{},
    R"("00::00.111111")",
  },
  values_json_test_case{
    "Failed to parse date string '00::00', expected format '%T'",
    time_type{},
    R"("00::00.111111")",
  },
  values_json_test_case{
    "Expected fractional part for timestamp_value, got '1971-01-01T00:00:00'",
    timestamp_type{},
    R"("1971-01-01T00:00:00")",
  },
  values_json_test_case{
    "Expected 6-digit microsecond resolution, got '023'",
    timestamp_type{},
    R"("1971-01-01T00:00:00.023")",
  },
  values_json_test_case{
    "Failed to parse microseconds: 'foobar'",
    timestamp_type{},
    R"("1971-01-01T00:00:00.foobar")",
  },
  values_json_test_case{
    "Failed to parse date string 'junk1971-01-01T00:00:00', expected format "
    "'%FT%T'",
    timestamp_type{},
    R"("junk1971-01-01T00:00:00.000023")",
  },
  values_json_test_case{
    "Expected fractional part for timestamptz_value, got '1971-01-01T00:00:00'",
    timestamptz_type{},
    R"("1971-01-01T00:00:00")",
  },
  values_json_test_case{
    "Expected 6-digit microsecond resolution, got '023'",
    timestamptz_type{},
    R"("1971-01-01T00:00:00.023+00:00")",
  },
  values_json_test_case{
    "Failed to parse microseconds: 'foobar'",
    timestamptz_type{},
    R"("1971-01-01T00:00:00.foobar+00:00")",
  },
  values_json_test_case{
    "Expected offset part for timestamptz_value, got "
    "'1971-01-01T00:00:00.000023'",
    timestamptz_type{},
    R"("1971-01-01T00:00:00.000023")",
  },
  values_json_test_case{
    "Failed to parse date string 'junk1971-01-01T00:00:00', expected format "
    "'%FT%T'",
    timestamptz_type{},
    R"("junk1971-01-01T00:00:00.000023+00:00")",
  },
  values_json_test_case{
    "Expected 8 hex digits for fixed[4]: got 4",
    fixed_type{4},
    R"("ffff")",
  },
  values_json_test_case{
    "Failed to parse uuid: invalid uuid string",
    uuid_type{},
    R"("ffff")",
  },
  values_json_test_case{
    "Expected even length hex string, got 9",
    binary_type{},
    R"("123456789")",
  },
  values_json_test_case{
    "Failed to parse hex byte 'GG' - ec: 'generic:22'",
    binary_type{},
    R"("GGGGGGGG")",
  },
  values_json_test_case{
    fmt::format(
      "Expected at most 5-byte precision for {}, got 6", decimal_type{5, 2}),
    decimal_type{5, 2},
    R"("1000.00")",
  },
  values_json_test_case{
    "Failed to parse int128",
    decimal_type{5, 2},
    R"("FuB.Ar")",
  },
};

} // namespace

struct ValuesJsonParseErrorsTest : public ValuesJsonTestBase {};

INSTANTIATE_TEST_SUITE_P(
  ValuesJsonSerde, ValuesJsonParseErrorsTest, ::testing::ValuesIn(parse_errs));

TEST_P(ValuesJsonParseErrorsTest, ParsingFailsGracefully) {
    json::Document parsed;
    parsed.Parse(get_json_rep().data());
    ASSERT_THROW(
      value_from_json(parsed, get_type(), field_required::yes),
      std::invalid_argument);
    ASSERT_THROW(
      {
          try {
              value_from_json(parsed, get_type(), field_required::yes);
          } catch (const std::invalid_argument& e) {
              EXPECT_THAT(e.what(), testing::HasSubstr(get_description()));
              throw;
          }
      },
      std::invalid_argument);
}

namespace {
struct decimal_parsing_test_case {
    std::string description;
    std::string json;
    decimal_value value;
    decimal_type type;
    std::optional<std::string> re_json = std::nullopt;
};

std::ostream&
operator<<(std::ostream& os, const decimal_parsing_test_case& tc) {
    return os << tc.description;
}

struct DecimalRoundTripTest
  : ::testing::Test
  , testing::WithParamInterface<decimal_parsing_test_case> {};

static const std::vector<decimal_parsing_test_case> decimal_cases{
  decimal_parsing_test_case{
    "simple",
    R"("1234.5678")",
    decimal_value{12345678},
    decimal_type{8, 4},
  },
  decimal_parsing_test_case{
    "simple (negative)",
    R"("-1234.5678")",
    decimal_value{-12345678},
    decimal_type{8, 4},
  },
  decimal_parsing_test_case{
    "no integral part",
    R"("0.12345678")",
    decimal_value{12345678},
    decimal_type{8, 8},
    R"(".12345678")",
  },
  decimal_parsing_test_case{
    "no integral part (negative)",
    R"("-0.12345678")",
    decimal_value{-12345678},
    decimal_type{8, 8},
    R"("-.12345678")",
  },
  decimal_parsing_test_case{
    "trailing decimal point",
    R"("12345678.")",
    decimal_value{12345678},
    decimal_type{8, 0},
  },
  decimal_parsing_test_case{
    "trailing decimal point (negative)",
    R"("-12345678.")",
    decimal_value{-12345678},
    decimal_type{8, 0},
  },
  decimal_parsing_test_case{
    "no trailing decimal point",
    R"("12345678")",
    decimal_value{12345678},
    decimal_type{8, 0},
  },
  decimal_parsing_test_case{
    "no trailing decimal point (negative)",
    R"("-12345678")",
    decimal_value{-12345678},
    decimal_type{8, 0},
  },
  decimal_parsing_test_case{
    "truncates trailing digits", // TODO(oren): rounding?
    R"("1234.5678888")",
    decimal_value(12345678),
    decimal_type{8, 4},
  },
  decimal_parsing_test_case{
    "truncates trailing digits (negative)", // TODO(oren): rounding
    R"("-1234.5678888")",
    decimal_value(-12345678),
    decimal_type{8, 4},
  },
  decimal_parsing_test_case{
    "parses left padding but DOES NOT serialize it",
    R"("000000001234.5678")",
    decimal_value(12345678),
    decimal_type{16, 4},
    R"("1234.5678")",
  },
  decimal_parsing_test_case{
    "parses left padding but DOES NOT serialize it (negative)",
    R"("-000000001234.5678")",
    decimal_value(-12345678),
    decimal_type{16, 4},
    R"("-1234.5678")",
  },
  decimal_parsing_test_case{
    "behaves sanely if we undershoot precision",
    R"("1234.5678")",
    decimal_value(12345678),
    decimal_type{16, 4},
    R"("1234.5678")",
  },
  decimal_parsing_test_case{
    "behaves sanely if we undershoot precision (negative)",
    R"("-1234.5678")",
    decimal_value(-12345678),
    decimal_type{16, 4},
    R"("-1234.5678")",
  },
  decimal_parsing_test_case{
    "parses right padding and DOES serialize it",
    R"("1234.56780000")",
    decimal_value(123456780000),
    decimal_type{16, 8},
    R"("1234.56780000")",
  },
  decimal_parsing_test_case{
    "parses right padding and DOES serialize it (negative)",
    R"("-1234.56780000")",
    decimal_value(-123456780000),
    decimal_type{16, 8},
    R"("-1234.56780000")",
  },
  decimal_parsing_test_case{
    "scale exceeds value width",
    R"(".000012345678")",
    decimal_value{12345678},
    decimal_type{16, 12},
    R"(".000012345678")",
  },
  decimal_parsing_test_case{
    "scale exceeds value width (negative)",
    R"("-.000012345678")",
    decimal_value{-12345678},
    decimal_type{16, 12},
    R"("-.000012345678")",
  },
};

INSTANTIATE_TEST_SUITE_P(
  ValuesJsonSerde, DecimalRoundTripTest, ::testing::ValuesIn(decimal_cases));

TEST_P(DecimalRoundTripTest, JsonToValToJsonToVal) {
    const auto& [desc, json, expected_value, type, re_json] = GetParam();

    // parse the provided JSON string, check success, extract the decimal_value,
    // compare to expected
    json::Document parsed;
    parsed.Parse(json);
    std::optional<value> parsed_value;
    ASSERT_NO_THROW(
      parsed_value = value_from_json(parsed, type, field_required::yes));
    ASSERT_TRUE(parsed_value.has_value());
    ASSERT_TRUE(std::holds_alternative<primitive_value>(parsed_value.value()));
    ASSERT_TRUE(std::holds_alternative<decimal_value>(
      std::get<primitive_value>(parsed_value.value())));

    auto parsed_dec = std::get<decimal_value>(
      std::get<primitive_value>(parsed_value.value()));
    ASSERT_EQ(parsed_dec.val, expected_value.val);

    // then re-serialize to JSON and parse again. check success and that the
    // resulting value is equal to the original parse

    std::string s;
    ASSERT_NO_THROW(s = value_to_json_str(parsed_value.value(), type));
    if (re_json.has_value()) {
        ASSERT_EQ(re_json, s);
    }

    // note that we don't care about string equality to the original serialized
    // form because we want to be flexible with parsing rules

    json::Document parsed_2;
    parsed_2.Parse(json);
    std::optional<value> parsed_value_2;
    ASSERT_NO_THROW(
      parsed_value_2 = value_from_json(parsed_2, type, field_required::yes));
    ASSERT_TRUE(parsed_value_2.has_value());
    ASSERT_TRUE(
      std::holds_alternative<primitive_value>(parsed_value_2.value()));
    ASSERT_TRUE(std::holds_alternative<decimal_value>(
      std::get<primitive_value>(parsed_value_2.value())));

    auto parsed_dec_2 = std::get<decimal_value>(
      std::get<primitive_value>(parsed_value_2.value()));
    ASSERT_EQ(parsed_dec_2.val, expected_value.val);
    ASSERT_EQ(parsed_dec_2.val, parsed_dec.val);
}

} // namespace
