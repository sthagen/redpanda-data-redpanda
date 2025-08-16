// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "absl/hash/hash.h"
#include "model/kitp.h"

#include <gtest/gtest.h>

using namespace model;

namespace {

const topic_id topic_id_none{};
const topic_id topic_id_1{uuid_t::create()};
const topic_id topic_id_2{uuid_t::create()};
const topic topic_1{"topic_1"};
const topic topic_2{"topic_2"};
const partition_id part_id{42};

template<typename T>
void test_hash() {
    const T without_id_1{topic_id_none, topic_1, part_id};
    const T without_id_1_copy{topic_id_none, topic_1, part_id};
    const T without_id_2{topic_id_none, topic_2, part_id};
    const T with_id_1{topic_id_1, topic_1, part_id};
    const T with_id_1_copy{topic_id_1, topic_1, part_id};
    const T with_id_2{topic_id_2, topic_2, part_id};

    auto hasher = absl::Hash<T>{};
    auto expected = hasher(without_id_1);
    ASSERT_EQ(expected, hasher(without_id_1_copy));
    ASSERT_EQ(expected, hasher(with_id_1));
    ASSERT_EQ(expected, hasher(with_id_1_copy));

    ASSERT_NE(expected, hasher(without_id_2));
    ASSERT_NE(expected, hasher(with_id_2));
}

template<typename T>
void test_equality() {
    const T without_id_1{topic_id_none, topic_1, part_id};
    const T without_id_1_copy{topic_id_none, topic_1, part_id};
    const T without_id_2{topic_id_none, topic_2, part_id};
    const T with_id_1{topic_id_1, topic_1, part_id};
    const T with_id_1_copy{topic_id_1, topic_1, part_id};
    const T with_id_2{topic_id_2, topic_2, part_id};

    // Test equal
    ASSERT_EQ(without_id_1, without_id_1_copy);
    ASSERT_EQ(with_id_1, with_id_1_copy);

    // Test equivalence (id is not compared if either is default)
    ASSERT_EQ(without_id_1, with_id_1);
    ASSERT_EQ(with_id_1, without_id_1);
    ASSERT_EQ(with_id_2, without_id_2);
    ASSERT_EQ(without_id_1_copy, with_id_1);
    ASSERT_EQ(without_id_1_copy, with_id_1_copy);
    ASSERT_EQ(without_id_1, with_id_1_copy);

    // Test inequality
    ASSERT_NE(without_id_2, without_id_1);
    ASSERT_NE(without_id_1, with_id_2);
    ASSERT_NE(with_id_2, without_id_1);
}

} // namespace

TEST(KitpViewTest, TestHash) { test_hash<kitp_view>(); }
TEST(KitpTest, TestHash) { test_hash<kitp>(); }
TEST(KitpWithHashViewTest, TestHash) { test_hash<kitp_with_hash>(); }

TEST(KitpViewTest, TestEquality) { test_equality<kitp_view>(); }
TEST(KitpTest, TestEquality) { test_equality<kitp>(); }
TEST(KitpWithHashViewTest, TestEquality) { test_equality<kitp_with_hash>(); }
