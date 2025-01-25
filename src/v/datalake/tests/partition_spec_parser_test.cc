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

using datalake::parse_partition_spec;
using iceberg::unresolved_partition_spec;

TEST(PartitionSpecParserTest, TestParse) {
    {
        auto res = parse_partition_spec("()");
        ASSERT_FALSE(res.has_error()) << res.error();
        ASSERT_EQ(res.value(), unresolved_partition_spec{});
    }

    {
        auto res = parse_partition_spec("(foo)");
        ASSERT_FALSE(res.has_error()) << res.error();
        auto expected = chunked_vector<unresolved_partition_spec::field>{
          unresolved_partition_spec::field{
            .source_name = {"foo"},
            .transform = iceberg::identity_transform{},
            .name = "foo"},
        };
        ASSERT_EQ(
          res.value(),
          unresolved_partition_spec{.fields = std::move(expected)});
    }

    {
        auto res = parse_partition_spec(" (foo.bar, baz ) ");
        ASSERT_FALSE(res.has_error()) << res.error();
        auto expected = chunked_vector<unresolved_partition_spec::field>{
          unresolved_partition_spec::field{
            .source_name = {"foo", "bar"},
            .transform = iceberg::identity_transform{},
            .name = "foo.bar"},
          unresolved_partition_spec::field{
            .source_name = {"baz"},
            .transform = iceberg::identity_transform{},
            .name = "baz"},
        };
        ASSERT_EQ(
          res.value(),
          unresolved_partition_spec{.fields = std::move(expected)});
    }

    {
        auto res = parse_partition_spec(
          " (hour(redpanda.timestamp), day(my_ts) as my_day )");
        ASSERT_FALSE(res.has_error()) << res.error();
        auto expected = chunked_vector<unresolved_partition_spec::field>{
          unresolved_partition_spec::field{
            .source_name = {"redpanda", "timestamp"},
            .transform = iceberg::hour_transform{},
            .name = "redpanda.timestamp_hour"},
          unresolved_partition_spec::field{
            .source_name = {"my_ts"},
            .transform = iceberg::day_transform{},
            .name = "my_day"},
        };
        ASSERT_EQ(
          res.value(),
          unresolved_partition_spec{.fields = std::move(expected)});
    }
}
