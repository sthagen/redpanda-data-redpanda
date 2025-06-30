// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"

#include <gtest/gtest.h>

using namespace model;

TEST(TopicIdPartitionTest, TestHappyFromString) {
    auto tidp = topic_id_partition::from(
      "deadbeef-0000-0000-0000-000000000000/12345");
    ASSERT_STREQ(
      ss::sstring(tidp.topic_id()).data(),
      "deadbeef-0000-0000-0000-000000000000");
    ASSERT_EQ(12345, tidp.partition());
}

TEST(TopicIdPartitionTest, TestNoPartition) {
    ASSERT_ANY_THROW(
      topic_id_partition::from("deadbeef-0000-0000-0000-000000000000"));
}

TEST(TopicIdPartitionTest, TestEmptyPartition) {
    ASSERT_ANY_THROW(
      topic_id_partition::from("deadbeef-0000-0000-0000-000000000000/"));
}

TEST(TopicIdPartitionTest, TestBadUuid) {
    ASSERT_THROW(
      topic_id_partition::from("not-a-uuid/12345"), std::runtime_error);
}
