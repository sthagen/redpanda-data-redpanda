/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cloud_topics/level_one/metastore/offset_interval_map.h"
#include "model/fundamental.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>

using namespace cloud_topics::l1;
using o = kafka::offset;

namespace {
MATCHER_P3(MatchesEntry, base, last, value, "") {
    return arg.base_offset == base && arg.last_offset == last
           && arg.value == value;
}
} // namespace

TEST(OffsetIntervalMapTest, TestEmpty) {
    offset_interval_map<int> m;
    ASSERT_TRUE(m.empty());
    ASSERT_EQ(m.size(), 0u);
    ASSERT_TRUE(m.insert(o{0}, o{1}, 5));
    ASSERT_FALSE(m.empty());
    ASSERT_EQ(m.size(), 1u);
}

TEST(OffsetIntervalMapTest, TestInsertEmptyRangeFails) {
    offset_interval_map<int> m;
    // last < base is an empty range and must not be inserted.
    ASSERT_FALSE(m.insert(o{5}, o{4}, 1));
    ASSERT_TRUE(m.empty());
}

TEST(OffsetIntervalMapTest, TestInsertRejectsOverlap) {
    offset_interval_map<int> m;
    ASSERT_TRUE(m.insert(o{1}, o{5}, 1));

    // The same range, partial overlaps on either side, a contained range, and
    // a superset all overlap an existing interval and are rejected.
    ASSERT_FALSE(m.insert(o{1}, o{5}, 2));
    ASSERT_FALSE(m.insert(o{0}, o{1}, 2));
    ASSERT_FALSE(m.insert(o{5}, o{10}, 2));
    ASSERT_FALSE(m.insert(o{2}, o{3}, 2));
    ASSERT_FALSE(m.insert(o{0}, o{10}, 2));

    // A failed insert leaves the existing range and value untouched.
    EXPECT_THAT(m.to_vec(), testing::ElementsAre(MatchesEntry(o{1}, o{5}, 1)));
}

TEST(OffsetIntervalMapTest, TestInsertIntoGapSucceeds) {
    offset_interval_map<int> m;
    ASSERT_TRUE(m.insert(o{0}, o{2}, 1));
    ASSERT_TRUE(m.insert(o{10}, o{12}, 2));

    // A range that fits entirely in the gap between two existing ranges is
    // inserted.
    ASSERT_TRUE(m.insert(o{5}, o{7}, 3));
    EXPECT_THAT(
      m.to_vec(),
      testing::ElementsAre(
        MatchesEntry(o{0}, o{2}, 1),
        MatchesEntry(o{5}, o{7}, 3),
        MatchesEntry(o{10}, o{12}, 2)));
}

TEST(OffsetIntervalMapTest, TestInsertAdjacentAtBoundarySucceeds) {
    // Ranges that touch at a boundary (next base == prev last + 1) do not
    // overlap, so both insert; a range sharing the boundary offset overlaps
    // and is rejected.
    offset_interval_map<int> m;
    ASSERT_TRUE(m.insert(o{1}, o{5}, 1));
    ASSERT_FALSE(m.insert(o{5}, o{10}, 2));
    ASSERT_TRUE(m.insert(o{6}, o{10}, 3));
    EXPECT_THAT(
      m.to_vec(),
      testing::ElementsAre(
        MatchesEntry(o{1}, o{5}, 1), MatchesEntry(o{6}, o{10}, 3)));
}

TEST(OffsetIntervalMapTest, TestEmptyMapQueries) {
    offset_interval_map<int> m;
    ASSERT_TRUE(m.empty());
    ASSERT_FALSE(m.contains(o{0}));
    ASSERT_FALSE(m.covers(o{0}, o{0}));
    EXPECT_THAT(m.to_vec(), testing::ElementsAre());
    ASSERT_FALSE(m.make_stream().has_next());
}

TEST(OffsetIntervalMapTest, TestInsertDoesNotCoalesceAdjacent) {
    // Unlike offset_interval_set, adjacent ranges are not merged: each keeps
    // its own value.
    offset_interval_map<int> m;
    ASSERT_TRUE(m.insert(o{1}, o{2}, 10));
    ASSERT_TRUE(m.insert(o{3}, o{4}, 20));
    EXPECT_THAT(
      m.to_vec(),
      testing::ElementsAre(
        MatchesEntry(o{1}, o{2}, 10), MatchesEntry(o{3}, o{4}, 20)));
}

TEST(OffsetIntervalMapTest, TestContains) {
    offset_interval_map<int> m;
    ASSERT_TRUE(m.insert(o{1}, o{2}, 1));
    ASSERT_FALSE(m.contains(o{0}));
    ASSERT_TRUE(m.contains(o{1}));
    ASSERT_TRUE(m.contains(o{2}));
    ASSERT_FALSE(m.contains(o{3}));

    ASSERT_TRUE(m.insert(o{4}, o{6}, 2));
    ASSERT_TRUE(m.contains(o{5}));
    ASSERT_FALSE(m.contains(o{3}));
}

TEST(OffsetIntervalMapTest, TestCovers) {
    offset_interval_map<int> m;
    ASSERT_FALSE(m.covers(o{0}, o{0}));

    ASSERT_TRUE(m.insert(o{1}, o{5}, 1));
    ASSERT_TRUE(m.covers(o{1}, o{5}));
    ASSERT_TRUE(m.covers(o{2}, o{4}));
    ASSERT_FALSE(m.covers(o{0}, o{5}));
    ASSERT_FALSE(m.covers(o{1}, o{6}));
    ASSERT_FALSE(m.covers(o{6}, o{10}));
}

TEST(OffsetIntervalMapTest, TestCoversDisjointIntervals) {
    offset_interval_map<int> m;
    ASSERT_TRUE(m.insert(o{0}, o{5}, 1));
    ASSERT_TRUE(m.insert(o{10}, o{20}, 2));

    ASSERT_TRUE(m.covers(o{0}, o{5}));
    ASSERT_TRUE(m.covers(o{10}, o{20}));
    // Spanning the gap between the two ranges is not covered.
    ASSERT_FALSE(m.covers(o{4}, o{11}));
    ASSERT_FALSE(m.covers(o{0}, o{20}));
    ASSERT_FALSE(m.covers(o{5}, o{10}));
}

TEST(OffsetIntervalMapTest, TestCoversSpansContiguousRanges) {
    // Ranges are not coalesced, so a query may span several contiguous ones.
    // Every offset in [0, 10] is present across [0, 4] and [5, 10], so it is
    // covered even though no single range spans it.
    offset_interval_map<int> m;
    ASSERT_TRUE(m.insert(o{0}, o{4}, 1));
    ASSERT_TRUE(m.insert(o{5}, o{10}, 2));

    ASSERT_TRUE(m.covers(o{0}, o{10}));
    ASSERT_TRUE(m.covers(o{3}, o{8}));
    ASSERT_TRUE(m.covers(o{4}, o{5}));

    EXPECT_FALSE(m.covers(o{10}, o{5})); // inverted range

    // A gap anywhere in the queried range breaks coverage.
    ASSERT_TRUE(m.insert(o{12}, o{15}, 3));
    ASSERT_FALSE(m.covers(o{0}, o{15}));
    ASSERT_FALSE(m.covers(o{8}, o{12}));
}

TEST(OffsetIntervalMapTest, TestStreamOrdersByOffset) {
    offset_interval_map<int> m;
    EXPECT_THAT(m.to_vec(), testing::ElementsAre());

    ASSERT_TRUE(m.insert(o{10}, o{12}, 100));
    ASSERT_TRUE(m.insert(o{1}, o{3}, 200));
    ASSERT_TRUE(m.insert(o{5}, o{5}, 300));

    // Streamed in ascending base-offset order, values preserved.
    EXPECT_THAT(
      m.to_vec(),
      testing::ElementsAre(
        MatchesEntry(o{1}, o{3}, 200),
        MatchesEntry(o{5}, o{5}, 300),
        MatchesEntry(o{10}, o{12}, 100)));
}
