// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

#include "cloud_topics/level_one/maintenance/keyed_priority_queue.h"

#include <gtest/gtest.h>

#include <functional>

namespace cloud_topics::l1 {

namespace {

TEST(KeyedPriorityQueueTest, UpsertUpdatesInPlace) {
    keyed_priority_queue<int, int, std::less<int>> kpq{std::less<int>{}};
    kpq.upsert(1, 5);
    kpq.upsert(2, 7);
    EXPECT_EQ(kpq.size(), 2);
    EXPECT_EQ(kpq.top().first, 2); // value 7

    kpq.upsert(1, 10); // bump key 1 above key 2; size unchanged (no duplicate)
    EXPECT_EQ(kpq.size(), 2);
    EXPECT_EQ(kpq.top().first, 1);
    EXPECT_EQ(kpq.top().second, 10);

    kpq.upsert(1, 1); // lower it back down
    EXPECT_EQ(kpq.size(), 2);
    EXPECT_EQ(kpq.top().first, 2);
}

TEST(KeyedPriorityQueueTest, EraseRemovesKey) {
    keyed_priority_queue<int, int, std::less<int>> kpq{std::less<int>{}};
    kpq.upsert(1, 5);
    kpq.upsert(2, 7);
    kpq.erase(2);
    EXPECT_EQ(kpq.size(), 1);
    EXPECT_FALSE(kpq.contains(2));
    EXPECT_EQ(kpq.top().first, 1);
}

TEST(KeyedPriorityQueueTest, EqualValuesRetainedAndTieBrokenByKey) {
    // The backing std::set ranks elements by value first, key second, and
    // treats two as duplicates when neither sorts below the other. Without the
    // key tie-break, equal-valued entries would compare equivalent and the
    // unique set would drop all but one. All three keys must therefore be kept;
    // among equal values the largest key is the top.
    keyed_priority_queue<int, int, std::less<int>> kpq{std::less<int>{}};
    kpq.upsert(1, 5);
    kpq.upsert(2, 5);
    kpq.upsert(3, 5);
    EXPECT_EQ(kpq.size(), 3); // not collapsed to 1
    EXPECT_EQ(kpq.top().first, 3);
    EXPECT_EQ(kpq.top().second, 5);

    // Erasing the top reveals the next key in tie-break order.
    kpq.erase(3);
    EXPECT_EQ(kpq.size(), 2);
    EXPECT_EQ(kpq.top().first, 2);

    // Re-upserting an existing key at the same value re-keys its slot without
    // creating a duplicate or disturbing the order.
    kpq.upsert(2, 5);
    EXPECT_EQ(kpq.size(), 2);
    EXPECT_EQ(kpq.top().first, 2);
}

} // namespace

} // namespace cloud_topics::l1
