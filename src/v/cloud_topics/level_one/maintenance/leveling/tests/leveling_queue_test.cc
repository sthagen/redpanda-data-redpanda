// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

#include "cloud_topics/level_one/maintenance/leveling/leveling_queue.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "model/fundamental.h"

#include <seastar/core/shared_ptr.hh>

#include <gtest/gtest.h>

#include <functional>
#include <vector>

namespace cloud_topics::l1 {

namespace {

const model::topic_id test_topic = model::topic_id::create();

model::topic_id_partition tidp(int p) {
    return model::topic_id_partition(test_topic, model::partition_id(p));
}

leveling_job_ptr mk_job(int p, size_t score) {
    auto ntp = model::ntp(
      model::ns("test"), model::topic("t"), model::partition_id(p));
    auto meta = ss::make_lw_shared<log_compaction_meta>(
      tidp(p), std::move(ntp));
    // extent_count carries the priority directly; size_bytes is kept at 1 so
    // the comparator below orders exactly by the intended score.
    return ss::make_lw_shared<leveling_job>(
      std::move(meta),
      levelable_range{
        .base_offset = kafka::offset(p * 1000),
        .last_offset = kafka::offset(p * 1000 + 99),
        .size_bytes = 1,
        .extent_count = score,
      },
      metastore::compaction_epoch{0});
}

leveling_cmp_t by_extent_count() {
    return [](const leveling_job_ptr& a, const leveling_job_ptr& b) {
        return a->range.extent_count < b->range.extent_count;
    };
}

TEST(LevelingQueueTest, EmptyPopIsNoop) {
    leveling_queue q(by_extent_count());
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0);
    EXPECT_EQ(q.partition_count(), 0);
    q.pop(); // no-op on an empty queue
    EXPECT_TRUE(q.empty());
}

TEST(LevelingQueueTest, SinglePartitionBestFirst) {
    leveling_queue q(by_extent_count());
    q.push(mk_job(0, 5));
    q.push(mk_job(0, 9)); // best arrives after a smaller range
    q.push(mk_job(0, 2));
    EXPECT_EQ(q.size(), 3);
    EXPECT_EQ(q.partition_count(), 1);

    std::vector<size_t> got;
    while (!q.empty()) {
        got.push_back(q.top()->range.extent_count);
        q.pop();
    }
    EXPECT_EQ(got, (std::vector<size_t>{9, 5, 2}));
}

TEST(LevelingQueueTest, GlobalBestFirstAcrossPartitions) {
    leveling_queue q(by_extent_count());
    // p0: 5,9,2   p1: 7,1   p2: 4
    q.push(mk_job(0, 5));
    q.push(mk_job(0, 9));
    q.push(mk_job(0, 2));
    q.push(mk_job(1, 7));
    q.push(mk_job(1, 1));
    q.push(mk_job(2, 4));

    std::vector<size_t> got;
    std::optional<size_t> prev;
    while (!q.empty()) {
        const auto score = q.top()->range.extent_count;
        q.pop();
        if (prev) {
            EXPECT_LE(score, *prev) << "ordering not best-first";
        }
        prev = score;
        got.push_back(score);
    }
    EXPECT_EQ(got, (std::vector<size_t>{9, 7, 5, 4, 2, 1}));
}

TEST(LevelingQueueTest, OneCandidatePerPartition) {
    leveling_queue q(by_extent_count());
    q.push(mk_job(0, 5));
    q.push(mk_job(0, 9));
    q.push(mk_job(1, 7));
    EXPECT_EQ(q.partition_count(), 2);
    EXPECT_EQ(q.size(), 3);

    EXPECT_EQ(q.top()->range.extent_count, 9); // p0's best
    EXPECT_EQ(q.top()->meta->tidp, tidp(0));
    q.pop();
    EXPECT_EQ(q.partition_count(), 2); // p0 still has its 5 queued
    EXPECT_EQ(q.size(), 2);
}

TEST(LevelingQueueTest, MidStreamReprioritizationInPlace) {
    leveling_queue q(by_extent_count());
    q.push(mk_job(0, 3));
    q.push(mk_job(1, 8));
    EXPECT_EQ(q.top()->range.extent_count, 8); // p1
    q.pop();

    // A better range for p0 arrives mid-stream; it must surface before p0's 3.
    q.push(mk_job(0, 10));
    EXPECT_EQ(q.top()->range.extent_count, 10);
    q.pop();
    EXPECT_EQ(q.top()->range.extent_count, 3);
    q.pop();
    EXPECT_TRUE(q.empty());
}

TEST(LevelingQueueTest, StaleHeadDoesNotJumpAhead) {
    // The case generation stamps would otherwise be needed for: p0 holds
    // {12,9,5}; p3 holds a single 6 sitting between p0's values. An addressable
    // outer queue keeps p0's slot at its true current top, so p3/6 is served
    // before p0/5 rather than after a stale "9" or "12" head.
    leveling_queue q(by_extent_count());
    q.push(mk_job(0, 5));
    q.push(mk_job(0, 9));
    q.push(mk_job(0, 12));
    q.push(mk_job(3, 6));

    std::vector<std::pair<model::topic_id_partition, size_t>> got;
    while (!q.empty()) {
        const auto& job = q.top();
        got.emplace_back(job->meta->tidp, job->range.extent_count);
        q.pop();
    }
    const std::vector<std::pair<model::topic_id_partition, size_t>> want = {
      {tidp(0), 12}, {tidp(0), 9}, {tidp(3), 6}, {tidp(0), 5}};
    EXPECT_EQ(got, want);
}

TEST(LevelingQueueTest, ClearDropsPartitionAndReprioritizes) {
    leveling_queue q(by_extent_count());
    q.push(mk_job(0, 9));
    q.push(mk_job(0, 8));
    q.push(mk_job(1, 7));
    EXPECT_EQ(q.size(), 3);

    // Clearing p0 removes both its ranges; p1 is now the global best.
    q.clear(tidp(0));
    EXPECT_EQ(q.size(), 1);
    EXPECT_EQ(q.partition_count(), 1);
    ASSERT_FALSE(q.empty());
    EXPECT_EQ(q.top()->meta->tidp, tidp(1));
    EXPECT_EQ(q.top()->range.extent_count, 7);
    q.pop();
    EXPECT_TRUE(q.empty());
}

TEST(LevelingQueueTest, ClearAbsentPartitionIsNoop) {
    leveling_queue q(by_extent_count());
    q.push(mk_job(0, 5));
    q.clear(tidp(9)); // never pushed
    EXPECT_EQ(q.size(), 1);
    EXPECT_EQ(q.partition_count(), 1);
}

TEST(LevelingQueueTest, RebuildAfterClearMergesCorrectly) {
    // Models a metastore re-collection: clear a partition's run, then rebuild
    // it from a fresh sample whose best beats everything else.
    leveling_queue q(by_extent_count());
    q.push(mk_job(0, 4));
    q.push(mk_job(1, 6));
    q.clear(tidp(0));
    q.push(mk_job(0, 9)); // fresh, larger range supersedes the old 4

    std::vector<size_t> got;
    while (!q.empty()) {
        got.push_back(q.top()->range.extent_count);
        q.pop();
    }
    EXPECT_EQ(got, (std::vector<size_t>{9, 6}));
}

TEST(LevelingQueueTest, EqualScoresAcrossPartitionsAllRetained) {
    // Partitions whose best jobs tie on score must each stay a distinct head
    // rather than collapsing: the outer queue orders by score first and breaks
    // ties on the partition key, so equal-scored heads remain distinct elements
    // instead of compared-equivalent duplicates. Among equal scores the larger
    // partition id wins.
    leveling_queue q(by_extent_count());
    q.push(mk_job(0, 5));
    q.push(mk_job(1, 5));
    q.push(mk_job(2, 5));
    EXPECT_EQ(q.size(), 3);
    EXPECT_EQ(q.partition_count(), 3);

    std::vector<model::topic_id_partition> got;
    while (!q.empty()) {
        EXPECT_EQ(q.top()->range.extent_count, 5);
        got.push_back(q.top()->meta->tidp);
        q.pop();
    }
    // Tie broken by key (topic_id_partition): descending partition id.
    EXPECT_EQ(got, (std::vector{tidp(2), tidp(1), tidp(0)}));
}

TEST(LevelingQueueTest, ClearNonHeadPartitionLeavesHead) {
    // Clearing a partition that does not hold the global best drops only that
    // partition; the head (a different partition) is undisturbed.
    leveling_queue q(by_extent_count());
    q.push(mk_job(0, 9)); // p0 is the global best
    q.push(mk_job(1, 5));
    q.push(mk_job(1, 3));
    ASSERT_EQ(q.partition_count(), 2);
    ASSERT_EQ(q.size(), 3);

    q.clear(tidp(1)); // not the head
    EXPECT_EQ(q.partition_count(), 1);
    EXPECT_EQ(q.size(), 1);
    ASSERT_FALSE(q.empty());
    EXPECT_EQ(q.top()->meta->tidp, tidp(0));
    EXPECT_EQ(q.top()->range.extent_count, 9);
}

TEST(LevelingQueueTest, ReusableAfterDrainingToEmpty) {
    // Draining to empty leaves no per-partition state behind: the queue is
    // fully reusable and a subsequent round orders independently.
    leveling_queue q(by_extent_count());
    q.push(mk_job(0, 5));
    q.push(mk_job(1, 7));
    while (!q.empty()) {
        q.pop();
    }
    ASSERT_TRUE(q.empty());
    ASSERT_EQ(q.partition_count(), 0);

    q.push(mk_job(0, 3));
    q.push(mk_job(2, 8));
    EXPECT_EQ(q.size(), 2);
    EXPECT_EQ(q.partition_count(), 2);
    EXPECT_EQ(q.top()->range.extent_count, 8);
    EXPECT_EQ(q.top()->meta->tidp, tidp(2));
}

} // namespace

} // namespace cloud_topics::l1
