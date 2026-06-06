// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

#include "cloud_topics/level_one/maintenance/compaction/compaction_queue.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/shared_ptr.hh>

#include <gtest/gtest.h>

#include <vector>

namespace cloud_topics::l1 {

namespace {

const model::topic_id test_topic = model::topic_id::create();

// A single topic with varying partition ids, so that `topic_id_partition`
// ordering (topic first, then partition) reduces to ordering by partition id.
model::topic_id_partition tidp(int p) {
    return model::topic_id_partition(test_topic, model::partition_id(p));
}

// Builds a compaction job for partition `p`, carrying `ratio` as its score.
compaction_job_ptr mk_job(int p, double ratio) {
    auto ntp = model::ntp(
      model::ns("test"), model::topic("t"), model::partition_id(p));
    auto meta = ss::make_lw_shared<log_compaction_meta>(
      tidp(p), std::move(ntp));
    return ss::make_lw_shared<compaction_job>(
      std::move(meta),
      compaction_info_and_timestamp{
        .info = {.dirty_ratio = ratio},
        .collected_at = model::timestamp::now(),
        .max_compactible_offset = kafka::offset::max(),
      });
}

compaction_cmp_t by_dirty_ratio() {
    return [](const compaction_job_ptr& a, const compaction_job_ptr& b) {
        return a->info_and_ts.info.dirty_ratio
               < b->info_and_ts.info.dirty_ratio;
    };
}

double top_ratio(const compaction_queue& q) {
    return q.top()->info_and_ts.info.dirty_ratio;
}

TEST(CompactionQueueTest, EmptyQueue) {
    compaction_queue q(by_dirty_ratio());
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);
    EXPECT_FALSE(q.contains(tidp(0)));
    // Clearing an absent CTP on an empty queue is a no-op.
    q.clear(tidp(0));
    EXPECT_TRUE(q.empty());
}

TEST(CompactionQueueTest, OrdersHighestRatioFirst) {
    compaction_queue q(by_dirty_ratio());
    q.push(mk_job(0, 0.1));
    q.push(mk_job(1, 0.9));
    q.push(mk_job(2, 0.5));
    EXPECT_EQ(q.size(), 3u);

    std::vector<double> got;
    while (!q.empty()) {
        got.push_back(top_ratio(q));
        q.pop();
    }
    EXPECT_EQ(got, (std::vector<double>{0.9, 0.5, 0.1}));
    EXPECT_TRUE(q.empty());
}

// Re-pushing the same CTP updates its entry in place rather than appending a
// duplicate: the queue holds at most one entry per `topic_id_partition`.
TEST(CompactionQueueTest, PushSameCtpUpdatesInPlace) {
    compaction_queue q(by_dirty_ratio());
    q.push(mk_job(0, 0.5));
    EXPECT_EQ(q.size(), 1u);

    q.push(mk_job(0, 0.9)); // same CTP, higher ratio
    EXPECT_EQ(q.size(), 1u);
    EXPECT_DOUBLE_EQ(top_ratio(q), 0.9);

    q.push(mk_job(0, 0.1)); // same CTP, lower ratio
    EXPECT_EQ(q.size(), 1u);
    EXPECT_DOUBLE_EQ(top_ratio(q), 0.1);

    q.pop();
    EXPECT_TRUE(q.empty());
}

// Re-pushing a CTP with a fresher (higher) score must reposition its single
// entry relative to other CTPs, not append a duplicate.
TEST(CompactionQueueTest, RepushReprioritizesAmongOtherCtps) {
    compaction_queue q(by_dirty_ratio());
    q.push(mk_job(0, 0.2));
    q.push(mk_job(1, 0.5));
    EXPECT_EQ(q.size(), 2u);
    EXPECT_EQ(q.top()->meta->tidp, tidp(1)); // 0.5 > 0.2

    // Re-push CTP 0 with a higher ratio: it overtakes CTP 1, and the queue
    // still holds exactly two entries (no duplicate for CTP 0).
    q.push(mk_job(0, 0.8));
    EXPECT_EQ(q.size(), 2u);
    EXPECT_EQ(q.top()->meta->tidp, tidp(0));
    EXPECT_DOUBLE_EQ(top_ratio(q), 0.8);

    // CTP 1 follows once CTP 0 is popped.
    q.pop();
    EXPECT_EQ(q.size(), 1u);
    EXPECT_EQ(q.top()->meta->tidp, tidp(1));
}

TEST(CompactionQueueTest, ClearEvictsQueuedCtp) {
    compaction_queue q(by_dirty_ratio());
    q.push(mk_job(0, 0.9));
    q.push(mk_job(1, 0.1));
    EXPECT_EQ(q.size(), 2u);

    q.clear(tidp(0));
    EXPECT_EQ(q.size(), 1u);
    EXPECT_FALSE(q.contains(tidp(0)));
    EXPECT_TRUE(q.contains(tidp(1)));
    // The evicted (higher-ratio) CTP no longer surfaces as the top.
    EXPECT_DOUBLE_EQ(top_ratio(q), 0.1);
}

TEST(CompactionQueueTest, ClearAbsentCtpIsNoop) {
    compaction_queue q(by_dirty_ratio());
    q.push(mk_job(0, 0.5));
    EXPECT_EQ(q.size(), 1u);

    q.clear(tidp(7)); // never queued
    EXPECT_EQ(q.size(), 1u);
    EXPECT_TRUE(q.contains(tidp(0)));
}

// Distinct CTPs with equal scores must all be retained (not collapsed by the
// backing set), with ties broken by `topic_id_partition`.
TEST(CompactionQueueTest, EqualRatiosRetainedAndTieBrokenByTidp) {
    compaction_queue q(by_dirty_ratio());
    q.push(mk_job(0, 0.5));
    q.push(mk_job(1, 0.5));
    q.push(mk_job(2, 0.5));
    EXPECT_EQ(q.size(), 3u);

    // Among equal ratios the largest tidp (largest partition id) is the top.
    EXPECT_TRUE(q.contains(tidp(2)));
    q.pop();
    EXPECT_FALSE(q.contains(tidp(2)));
    EXPECT_TRUE(q.contains(tidp(1)));
    q.pop();
    EXPECT_TRUE(q.contains(tidp(0)));
    EXPECT_EQ(q.size(), 1u);
}

TEST(CompactionQueueTest, ReusableAfterDrainingToEmpty) {
    compaction_queue q(by_dirty_ratio());
    q.push(mk_job(0, 0.5));
    q.pop();
    EXPECT_TRUE(q.empty());

    // Re-queuing the same CTP after it drained behaves like a fresh insert.
    q.push(mk_job(0, 0.3));
    EXPECT_EQ(q.size(), 1u);
    EXPECT_TRUE(q.contains(tidp(0)));
    EXPECT_DOUBLE_EQ(top_ratio(q), 0.3);
}

} // namespace

} // namespace cloud_topics::l1
