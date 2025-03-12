/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/partition_spec_parser.h"

#include <gtest/gtest.h>

#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_set>

using datalake::parse_partition_spec;
using iceberg::unresolved_partition_spec;

namespace {
/**
 * @brief Make sure parsing succeeds
 *
 * Invokes parser against each of `input_variants` and makes sure
 * `expected_fields` is returned for each of them.
 *
 * @param expected_fields expected parsing results
 * @param input_variants strings to parse
 */
void expect_success(
  chunked_vector<unresolved_partition_spec::field> expected_fields,
  std::vector<std::string_view> input_variants) {
    unresolved_partition_spec expected_spec{
      .fields = std::move(expected_fields)};
    for (const auto& input : input_variants) {
        auto res = parse_partition_spec(input);
        ASSERT_FALSE(res.has_error())
          << "input: " << input << "error: " << res.error();
        ASSERT_EQ(res.value(), expected_spec) << "input: " << input;
    }
}

std::string_view
match_as_sv(const std::sub_match<std::string::const_iterator>& sm) {
    return {sm.first, sm.second};
}

auto const_char_subrange_as_sv(std::ranges::subrange<const char*>&& item) {
    return std::string_view(item);
}

/**
 * @brief Make sure parsing fails
 *
 * Removes asterisk character from the input string. Invokes parser against the
 * remaining string. Makes sure that
 *   1) parsing fails exactly at the position where the asterisk was
 *   2) parser expects `expected_expectations` instead of what was found there
 *
 * @param marked_input input string, must have a single asterisk character
 * @param expected_expectations collection of the tokens parser is supposed to
 * expect at the posision
 */
void expect_failure(
  std::string_view marked_input,
  std::unordered_set<std::string_view> expected_expectations) {
    using namespace std::literals;

    auto exp_err_pos = marked_input.find('*');
    ASSERT_NE(exp_err_pos, marked_input.npos);
    auto parsed = marked_input.substr(0, exp_err_pos);
    auto unparsed = marked_input.substr(exp_err_pos + 1);
    auto unmarked_input = std::string{parsed} + std::string{unparsed};

    ASSERT_EQ(unmarked_input.find('*'), unmarked_input.npos);
    auto res = parse_partition_spec(unmarked_input);
    ASSERT_TRUE(res.has_error());
    std::string actual_err = res.error();

    std::regex re{"col (.*): expected (.*) \\(got instead: \"(.*)\"\\)"};
    std::smatch matches;
    ASSERT_TRUE(std::regex_match(actual_err, matches, re));

    // column no
    ASSERT_EQ(match_as_sv(matches[1]), std::to_string(exp_err_pos));

    // expectations
    std::unordered_set<std::string_view> actual_expectations{
      std::from_range,
      std::views::split(match_as_sv(matches[2]), " or "sv)
        | std::views::transform(&const_char_subrange_as_sv)};
    ASSERT_EQ(expected_expectations, actual_expectations);

    // got instead
    ASSERT_EQ(unparsed, match_as_sv(matches[3]));
}

} // namespace

TEST(PartitionSpecParserTest, TestParse) {
    expect_success({}, {"", "()", "   (  )  ", "\t\r\n"});

    expect_success(
      {
        {.source_name = {"foo"},
         .transform = iceberg::identity_transform{},
         .name = "foo"},
      },
      {"(foo)", "fOo", " Foo ", "(`foo`)"});

    expect_success(
      {
        {.source_name = {"foo", "bar"},
         .transform = iceberg::identity_transform{},
         .name = "foo.bar"},
        {.source_name = {"baz"},
         .transform = iceberg::identity_transform{},
         .name = "baz"},
      },
      {"(foo.bar, baz)", "fOo.`bar`,`baz`", " (`foo`   .bar  ,baZ)"});

    expect_success(
      {
        {.source_name = {"redpanda", "timestamp"},
         .transform = iceberg::hour_transform{},
         .name = "redpanda.timestamp_hour"},
        {.source_name = {"my_ts"},
         .transform = iceberg::day_transform{},
         .name = "my_day"},
      },
      {" (hour(redpanda.timestamp), day(my_ts) as my_day )",
       "hoUr  (`redpanda`.timestamp      ), day(my_tS)aS`my_day`   ",
       " (HOUR(redpanda   .   `timestamp`)      ,day(   `my_ts`)AS my_day)  "});

    expect_success(
      {
        {.source_name = {"tricky`id", "", "yet\"@nOther )(one"},
         .transform = iceberg::bucket_transform{.n = 0},
         .name = "tricky`id..yet\"@nOther )(one_bucket"},
        {.source_name = {"as"},
         .transform = iceberg::truncate_transform{.length = 9000},
         .name = "as_truncate"},
        {.source_name = {"foo"},
         .transform = iceberg::identity_transform{},
         .name = "bar"},
        {.source_name = {"foo"},
         .transform = iceberg::identity_transform{},
         .name = "foo"},
      },
      {
        "bucket(0, `tricky``id`.``.`yet\"@nOther )(one`), truncate(9000, as), "
        "foo "
        "as bar, foo as foo",
        "  ( bucket (00000000000000000000000 ,`tricky``id` . `` . "
        "`yet\"@nOther "
        ")(one`     )    ,trunCATe(   009000,as    \n),   FOO AS BAR,FOO AS "
        "FOO)",
      });

    // asterisk only marks where we expect a failure
    expect_failure(
      "(foo,bar,hour*",
      {"\",\"", "\")\"", "\"(\"", "\".\"", "alphanumeric", "\"_\"", "\"AS\""});
    expect_failure(
      "(foo,bar,baz*",
      {"\",\"", "\")\"", "\".\"", "alphanumeric", "\"_\"", "\"AS\""});
    expect_failure(
      "foo,bar,baz*(qux)",
      {"\",\"", "\".\"", "alphanumeric", "\"_\"", "\"AS\"", "end of input"});
    expect_failure("`foo`.`bar`.`foo``bar`.`baz*", {"\"`\""});
    expect_failure("hour(*145, foo)", {"letter", "\"_\"", "\"`\""});
    expect_failure(
      "hour(a145*, foo)", {"\")\"", "\".\"", "alphanumeric", "\"_\""});
    expect_failure("(bucket(*-0, foo))", {"integer between 0 and 4294967295"});
    expect_failure(
      "(bucket(*4294967296, foo))", {"integer between 0 and 4294967295"});
    expect_failure("(bucket(3*.5, foo))", {"\",\""});
    expect_failure("( foo as bar *as baz )", {"\",\"", "\")\""});
    expect_failure(
      "qux as year*(quux)", {"\",\"", "alphanumeric", "\"_\"", "end of input"});
    expect_failure("qux as year *(quux)", {"\",\"", "end of input"});
    expect_failure("(foo,hour(bar),baz)*qux", {"end of input"});
    expect_failure("(foo,hour(bar),baz)*(qux,quux)", {"end of input"});
    expect_failure(
      "hour(foo)as bar, truncate(10, `a`.b.`c`.*)",
      {"letter", "\"_\"", "\"`\""});
    expect_failure(
      "`a`.b.`c``d` .`e` *`f`.gh",
      {"\".\"", "\"AS\"", "\",\"", "end of input"});
    expect_failure(
      "truncate(0, `a`.b.`c``d` .`e` *`f`.gh)", {"\".\"", "\")\""});
}
