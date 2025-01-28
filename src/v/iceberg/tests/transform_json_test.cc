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
#include "iceberg/transform_json.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(TransformJsonSerde, TestTransforms) {
    const auto check_roundtrip = [](const transform& t, const char* expected) {
        auto t_str = transform_to_str(t);
        ASSERT_STREQ(expected, t_str.c_str());
        auto t_roundtrip = transform_from_str(t_str);
        ASSERT_EQ(t_roundtrip, t);
    };
    ASSERT_NO_FATAL_FAILURE(check_roundtrip(identity_transform{}, "identity"));
    ASSERT_NO_FATAL_FAILURE(check_roundtrip(year_transform{}, "year"));
    ASSERT_NO_FATAL_FAILURE(check_roundtrip(month_transform{}, "month"));
    ASSERT_NO_FATAL_FAILURE(check_roundtrip(day_transform{}, "day"));
    ASSERT_NO_FATAL_FAILURE(check_roundtrip(hour_transform{}, "hour"));
    ASSERT_NO_FATAL_FAILURE(check_roundtrip(void_transform{}, "void"));
    ASSERT_NO_FATAL_FAILURE(check_roundtrip(bucket_transform{0}, "bucket[0]"));
    ASSERT_NO_FATAL_FAILURE(
      check_roundtrip(bucket_transform{4294967295}, "bucket[4294967295]"));
    ASSERT_NO_FATAL_FAILURE(
      check_roundtrip(truncate_transform{0}, "truncate[0]"));
    ASSERT_NO_FATAL_FAILURE(
      check_roundtrip(truncate_transform{4294967295}, "truncate[4294967295]"));
}
