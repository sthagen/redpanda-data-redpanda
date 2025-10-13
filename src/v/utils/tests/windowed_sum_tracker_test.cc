/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"
#include "gtest/gtest.h"
#include "utils/windowed_sum_tracker.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>

namespace {

using clock = ss::lowres_clock;
using namespace std::chrono_literals;

TEST(WindowedSumTrackerTests, EmptyTracker) {
    auto now = clock::now();
    auto tracker = windowed_sum_tracker<4, 4s / 1ns>(now);

    EXPECT_EQ(tracker.window_total(now), 0);

    // Advancing time without records should remain empty
    now += std::chrono::seconds(10);
    EXPECT_EQ(tracker.window_total(now), 0);

    // Even if we call record
    tracker.record(0, now);
    EXPECT_EQ(tracker.window_total(now), 0);
}

TEST(WindowedSumTrackerTests, MinimalBucketCountLifecycle) {
    auto now = clock::now();
    auto tracker = windowed_sum_tracker<2, 1s / 1ns>(now);

    // Record and verify immediate availability
    tracker.record(100, now);
    EXPECT_EQ(tracker.window_total(now), 100);

    // Multiple records in same bucket accumulate
    tracker.record(50, now);
    EXPECT_EQ(tracker.window_total(now), 150);

    // Just before expiry
    now += std::chrono::milliseconds(999);
    EXPECT_EQ(tracker.window_total(now), 150);

    // At expiry boundary
    now += std::chrono::milliseconds(1);
    EXPECT_EQ(tracker.window_total(now), 150);

    // Half-way through the next bucket, the previous bucket is 50% interpolated
    now += std::chrono::milliseconds(500);
    EXPECT_EQ(tracker.window_total(now), 75);

    now += std::chrono::milliseconds(500);
    EXPECT_EQ(tracker.window_total(now), 0);
}

TEST(WindowedSumTrackerTests, MultiBucketExpiration) {
    auto now = clock::now();
    auto tracker = windowed_sum_tracker<5, 4s / 1ns>(now);

    // Fill a window worth of buckets with different values
    for (int i = 1; i <= 4; ++i) {
        tracker.record(i * 10, now);
        now += std::chrono::seconds(1);
    }

    // All buckets active (total: 10+20+30+40 = 100)
    now -= std::chrono::milliseconds(1);
    EXPECT_EQ(tracker.window_total(now), 100);

    // First bucket 50% expires (total: 10/2+20+30+40 = 95)
    now += std::chrono::milliseconds(500);
    EXPECT_EQ(tracker.window_total(now), 95);

    // Advance to expire all buckets
    now += std::chrono::seconds(4);
    EXPECT_EQ(tracker.window_total(now), 0);
}

TEST(WindowedSumTrackerTests, NonExactBucketBoundaries) {
    auto now = clock::now();
    auto tracker = windowed_sum_tracker<3, 3s / 1ns>(now);

    tracker.record(10, now);
    now += std::chrono::milliseconds(800); // Still in first bucket
    tracker.record(20, now);
    EXPECT_EQ(tracker.window_total(now), 30);

    now += std::chrono::milliseconds(300); // Cross to second bucket
    tracker.record(40, now);
    EXPECT_EQ(tracker.window_total(now), 70);
}

TEST(WindowedSumTrackerTests, MultiBucketJump) {
    auto now = clock::now();
    auto tracker = windowed_sum_tracker<5, 4s / 1ns>(now);

    tracker.record(100, now);
    EXPECT_EQ(tracker.window_total(now), 100);

    // Jump 2 buckets worth
    now += std::chrono::seconds(2);
    EXPECT_EQ(tracker.window_total(now), 100);
}

TEST(WindowedSumTrackerTests, LargeTimeJumpClearsWindow) {
    auto now = clock::now();
    auto tracker = windowed_sum_tracker<5, 4s / 1ns>(now);

    tracker.record(100, now);
    EXPECT_EQ(tracker.window_total(now), 100);

    // Jump well beyond window duration and the window sum should be 0
    now += std::chrono::seconds(20);
    EXPECT_EQ(tracker.window_total(now), 0);

    // Check that calling record with 0 doesn't change this
    tracker.record(0, now);
    EXPECT_EQ(tracker.window_total(now), 0);
}

TEST(WindowedSumTrackerTests, MinimalJumpToClearWindow) {
    constexpr auto window = 60s;
    constexpr auto bucket_count = 4;
    constexpr auto bucket_size = 20s;

    auto now = clock::now();

    using tracker_t = windowed_sum_tracker<bucket_count, window / 1ns>;
    auto tracker = tracker_t(now);

    // Sanity check the bucket_size
    EXPECT_EQ(tracker_t::bucket_size, bucket_size);

    tracker.record(100, now);
    EXPECT_EQ(tracker.window_total(now), 100);

    // Jump ahead by a single window + a bucket size to clear the value written
    // at the beginning of the first window
    now += window + bucket_size;
    EXPECT_EQ(tracker.window_total(now), 0);
}

} // namespace
