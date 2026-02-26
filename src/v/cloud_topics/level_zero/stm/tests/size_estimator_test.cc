/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/size_estimator.h"
#include "gtest/gtest.h"
#include "model/fundamental.h"

#include <gtest/gtest.h>

namespace ct = cloud_topics;

namespace {

// Use a small checkpoint interval for testing (100 bytes).
constexpr uint64_t test_interval = 100;

TEST(size_estimator_test, empty_estimator) {
    ct::size_estimator est(test_interval);
    EXPECT_EQ(est.total_bytes(), 0);
    EXPECT_EQ(est.checkpoint_count(), 0);
    EXPECT_EQ(est.estimate_bytes_at(model::offset{0}), 0);
    EXPECT_EQ(est.estimated_active_bytes(model::offset{0}), 0);
}

TEST(size_estimator_test, single_record_below_threshold) {
    ct::size_estimator est(test_interval);
    est.record(model::offset{0}, 50);

    EXPECT_EQ(est.total_bytes(), 50);
    // No checkpoint yet (50 < 100 threshold).
    EXPECT_EQ(est.checkpoint_count(), 0);
    // All data is active regardless of low watermark.
    EXPECT_EQ(est.estimated_active_bytes(model::offset{-1}), 50);
    // At or past the last recorded offset returns total bytes.
    EXPECT_EQ(est.estimate_bytes_at(model::offset{0}), 50);
}

TEST(size_estimator_test, checkpoint_created_at_threshold) {
    ct::size_estimator est(test_interval);
    // Write enough to trigger a checkpoint.
    est.record(model::offset{0}, 60);
    est.record(model::offset{1}, 60);

    EXPECT_EQ(est.total_bytes(), 120);
    EXPECT_EQ(est.checkpoint_count(), 1);
}

TEST(size_estimator_test, multiple_checkpoints) {
    ct::size_estimator est(test_interval);

    // Create several checkpoints with uniform entry sizes.
    for (int i = 0; i < 10; i++) {
        est.record(model::offset{i}, 50);
    }

    // Total = 500 bytes, threshold = 100.
    // Checkpoints at cumulative 100, 200, 300, 400, 500 (offsets 1,3,5,7,9).
    EXPECT_EQ(est.total_bytes(), 500);
    EXPECT_EQ(est.checkpoint_count(), 5);
}

TEST(size_estimator_test, idempotent_on_replay) {
    ct::size_estimator est(test_interval);
    est.record(model::offset{5}, 100);
    est.record(model::offset{10}, 100);

    EXPECT_EQ(est.total_bytes(), 200);

    // Replaying earlier offsets should be no-ops.
    est.record(model::offset{5}, 100);
    est.record(model::offset{10}, 100);
    est.record(model::offset{0}, 50);

    EXPECT_EQ(est.total_bytes(), 200);
}

TEST(size_estimator_test, interpolation_between_checkpoints) {
    ct::size_estimator est(test_interval);

    // Create entries with uniform sizes to get predictable interpolation.
    // 10 entries of 50 bytes each at offsets 0..9.
    for (int i = 0; i < 10; i++) {
        est.record(model::offset{i}, 50);
    }

    // Total = 500, checkpoints at offsets where cumulative hits thresholds.
    // First checkpoint at offset 1 (cumulative 100).
    // Estimate at offset 0 (before first checkpoint) should be 0.
    // This is acceptable since the error is bounded by the interval.

    // Estimate at the last recorded offset should return total.
    EXPECT_EQ(est.estimate_bytes_at(model::offset{9}), 500);

    // Active bytes with nothing truncated.
    EXPECT_EQ(est.estimated_active_bytes(model::offset{-1}), 500);
}

TEST(size_estimator_test, active_bytes_with_truncation) {
    ct::size_estimator est(test_interval);

    // Create 20 entries, each 50 bytes, at offsets 0..19.
    for (int i = 0; i < 20; i++) {
        est.record(model::offset{i}, 50);
    }

    EXPECT_EQ(est.total_bytes(), 1000);

    // Truncate below offset 10 (roughly half the data).
    auto active = est.estimated_active_bytes(model::offset{10});
    // Should be approximately 500 bytes (within the checkpoint interval).
    EXPECT_GT(active, 400ULL);
    EXPECT_LT(active, 600ULL);

    // Truncate everything.
    EXPECT_EQ(est.estimated_active_bytes(model::offset{19}), 0);

    // Truncate nothing.
    EXPECT_EQ(est.estimated_active_bytes(model::offset{-1}), 1000);
}

TEST(size_estimator_test, variable_entry_sizes) {
    ct::size_estimator est(test_interval);

    // First half: small entries (10 bytes each).
    for (int i = 0; i < 50; i++) {
        est.record(model::offset{i}, 10);
    }
    // Second half: large entries (100 bytes each).
    for (int i = 50; i < 100; i++) {
        est.record(model::offset{i}, 100);
    }

    // Total = 50*10 + 50*100 = 500 + 5000 = 5500.
    EXPECT_EQ(est.total_bytes(), 5500);

    // Truncating the first half (small entries) should remove ~500 bytes.
    auto active_after_small_truncated = est.estimated_active_bytes(
      model::offset{49});
    EXPECT_GT(active_after_small_truncated, 4900ULL);
    EXPECT_LT(active_after_small_truncated, 5100ULL);

    // Truncating the second half should remove a lot more.
    auto active_after_most_truncated = est.estimated_active_bytes(
      model::offset{90});
    EXPECT_LT(active_after_most_truncated, 1200ULL);
}

TEST(size_estimator_test, gc_removes_old_checkpoints) {
    ct::size_estimator est(test_interval);

    // Create enough data for several checkpoints.
    for (int i = 0; i < 100; i++) {
        est.record(model::offset{i}, 50);
    }

    auto initial_count = est.checkpoint_count();
    EXPECT_GT(initial_count, 5ULL);

    // GC below offset 50.
    est.gc_below(model::offset{50});
    EXPECT_LT(est.checkpoint_count(), initial_count);

    // Size estimates should still work after GC.
    auto active = est.estimated_active_bytes(model::offset{50});
    EXPECT_GT(active, 2000ULL);
    EXPECT_LT(active, 3000ULL);
}

TEST(size_estimator_test, gc_retains_interpolation_checkpoint) {
    ct::size_estimator est(test_interval);

    for (int i = 0; i < 20; i++) {
        est.record(model::offset{i}, 50);
    }

    // After GC, at least one checkpoint at or below the GC point remains
    // for interpolation.
    est.gc_below(model::offset{10});
    EXPECT_GE(est.checkpoint_count(), 1ULL);
}

TEST(size_estimator_test, gc_below_first_checkpoint_is_noop) {
    ct::size_estimator est(test_interval);

    // Create multiple checkpoints spanning offsets 0..9.
    for (int i = 0; i < 10; i++) {
        est.record(model::offset{i}, 50);
    }
    auto count = est.checkpoint_count();
    EXPECT_GT(count, 1ULL);

    // GC target is at or before the first checkpoint — nothing to remove.
    est.gc_below(model::offset{0});
    EXPECT_EQ(est.checkpoint_count(), count);
}

TEST(size_estimator_test, gc_with_single_checkpoint) {
    ct::size_estimator est(test_interval);

    // Create exactly one checkpoint.
    est.record(model::offset{0}, 100);
    EXPECT_EQ(est.checkpoint_count(), 1);

    // GC should not remove the only checkpoint.
    est.gc_below(model::offset{0});
    EXPECT_EQ(est.checkpoint_count(), 1);
}

TEST(size_estimator_test, bootstrap_from_log_replay) {
    // Simulate the STM bootstrap scenario: the log has been truncated
    // and we replay only the surviving entries.
    ct::size_estimator est(test_interval);

    // Suppose the log was truncated and now starts at offset 50.
    // Replay surviving entries from offset 50..99.
    for (int i = 50; i < 100; i++) {
        est.record(model::offset{i}, 50);
    }

    EXPECT_EQ(est.total_bytes(), 2500);

    // Since we only recorded from offset 50, all data is "active".
    // Asking for bytes below offset 50 should give 0 (no data recorded there).
    EXPECT_EQ(est.estimated_active_bytes(model::offset{49}), 2500);

    // Truncating to offset 75 should remove roughly half.
    auto active = est.estimated_active_bytes(model::offset{75});
    EXPECT_GT(active, 1100ULL);
    EXPECT_LT(active, 1400ULL);
}

TEST(size_estimator_test, downsample_caps_checkpoint_count) {
    // interval=100 bytes, max_checkpoints=10.
    ct::size_estimator est(100, 10);

    // Write enough to create many more than 10 checkpoints at the
    // original interval.
    for (int i = 0; i < 1000; i++) {
        est.record(model::offset{i}, 50);
    }

    // Downsampling should have fired, keeping count bounded.
    EXPECT_LE(est.checkpoint_count(), 10ULL);
    // Interval stays constant (not doubled on downsample).
    EXPECT_EQ(est.checkpoint_interval(), 100ULL);
    // Total bytes is still exact.
    EXPECT_EQ(est.total_bytes(), 50000ULL);
}

TEST(size_estimator_test, downsample_preserves_accuracy) {
    ct::size_estimator est(100, 10);

    // Uniform entries: 200 entries of 50 bytes each = 10000 bytes total.
    for (int i = 0; i < 200; i++) {
        est.record(model::offset{i}, 50);
    }

    EXPECT_EQ(est.total_bytes(), 10000ULL);

    // With uniform data, interpolation after downsampling should still be
    // reasonably accurate. Truncating roughly halfway.
    auto active = est.estimated_active_bytes(model::offset{100});
    EXPECT_GT(active, 4000ULL);
    EXPECT_LT(active, 6000ULL);

    // Boundary conditions still exact.
    EXPECT_EQ(est.estimated_active_bytes(model::offset{-1}), 10000ULL);
    EXPECT_EQ(est.estimated_active_bytes(model::offset{199}), 0ULL);
}

TEST(size_estimator_test, downsample_keeps_interval_constant) {
    ct::size_estimator est(100, 10);

    auto initial_interval = est.checkpoint_interval();

    // Push past the cap many times to trigger repeated downsampling.
    for (int i = 0; i < 5000; i++) {
        est.record(model::offset{i}, 50);
    }

    // Interval stays constant regardless of how many downsample rounds.
    EXPECT_EQ(est.checkpoint_interval(), initial_interval);
    // Count stays bounded.
    EXPECT_LE(est.checkpoint_count(), 10ULL);
    // Total bytes is still exact.
    EXPECT_EQ(est.total_bytes(), 250000ULL);
}

TEST(size_estimator_test, downsample_retains_first_and_last) {
    ct::size_estimator est(100, 5);

    // Create data that exceeds the cap.
    for (int i = 0; i < 100; i++) {
        est.record(model::offset{i}, 50);
    }

    EXPECT_GE(est.checkpoint_count(), 2ULL);
    // total_bytes is still exact after downsampling.
    EXPECT_EQ(est.total_bytes(), 5000ULL);
}

TEST(size_estimator_test, precision_recovers_after_gc_and_regrowth) {
    // Scenario from the design review: reconciler stops, partition
    // accumulates heavy data triggering many downsample rounds, then
    // reconciler resumes (GC clears old data), new data arrives.
    // Tip estimates must recover full precision at the original interval.
    //
    // With the old bug (_checkpoint_interval doubled on each downsample),
    // the interval would ratchet up to ~6400 after phase 1. Phase 3
    // writes only 4300 bytes, well below the inflated interval, so zero
    // new checkpoints would be created — the estimator would have a single
    // coarse interpolation span across the entire fresh-data region,
    // yielding ~1000 bytes of error at the midpoint.
    ct::size_estimator est(100, 20);

    // Phase 1: Heavy accumulation (reconciler offline).
    for (int i = 0; i < 1000; i++) {
        est.record(model::offset{i}, 50);
    }
    EXPECT_EQ(est.total_bytes(), 50000ULL);
    EXPECT_EQ(est.checkpoint_interval(), 100ULL);

    // Phase 2: Reconciler catches up — GC to the very tip.
    est.gc_below(model::offset{999});
    auto count_after_gc = est.checkpoint_count();

    // Phase 3: Fresh data with growing entry sizes. The non-linear
    // cumulative curve (quadratic in offset) makes estimation accuracy
    // sensitive to checkpoint density: coarse checkpoints give poor
    // interpolation, fine ones give good interpolation.
    for (int i = 1000; i < 1040; i++) {
        est.record(model::offset{i}, 10 + static_cast<uint64_t>(i - 1000) * 5);
    }
    // Phase 3 total: sum(10 + 5k, k=0..39) = 400 + 3900 = 4300.
    EXPECT_EQ(est.total_bytes(), 54300ULL);

    // New fine-grained checkpoints were created at the original interval.
    EXPECT_GT(est.checkpoint_count(), count_after_gc);

    // Estimation near the midpoint of fresh data.
    // True cumulative at offset 1019:
    //   50000 + sum(10+5k, k=0..19) = 50000 + 200 + 950 = 51150.
    //
    // With fine-grained checkpoints near the tip (max=20 gives enough
    // headroom), the nearest bracketing checkpoints are only a few
    // offsets apart, so interpolation error is small (~10 bytes).
    //
    // With old bug (no new checkpoints, single coarse interpolation
    // from ~999 to 1039): estimate ≈ 52150, error ≈ 1000.
    auto estimate = est.estimate_bytes_at(model::offset{1019});
    EXPECT_GT(estimate, 50900ULL);
    EXPECT_LT(estimate, 51500ULL);
}

TEST(size_estimator_test, new_checkpoints_arrive_at_original_interval) {
    // Directly verify that new checkpoints are created at the original
    // byte interval after many downsample rounds, not at an inflated one.
    ct::size_estimator est(100, 10);

    // Phase 1: Trigger many downsample rounds.
    for (int i = 0; i < 500; i++) {
        est.record(model::offset{i}, 50);
    }

    // GC to make room and isolate phase 2.
    est.gc_below(model::offset{498});
    auto count_after_gc = est.checkpoint_count();
    EXPECT_LE(count_after_gc, 3ULL);

    // Phase 2: Write exactly 5 intervals worth of data (500 bytes).
    // At original interval=100, this should produce ~5 new checkpoints.
    // With old inflated interval (~3200 after this many rounds), zero
    // new checkpoints would be created.
    for (int i = 500; i < 510; i++) {
        est.record(model::offset{i}, 50);
    }

    EXPECT_GE(est.checkpoint_count(), count_after_gc + 4);
}

TEST(size_estimator_test, memory_bounded_across_gc_regrowth_cycles) {
    // Verify that checkpoint count stays bounded through repeated cycles
    // of data accumulation and GC, addressing the memory concern of
    // unbounded growth when the reconciler is not running.
    ct::size_estimator est(100, 10);

    for (int cycle = 0; cycle < 5; cycle++) {
        int base = cycle * 200;

        // Accumulate data.
        for (int i = base; i < base + 200; i++) {
            est.record(model::offset{i}, 50);
        }
        EXPECT_LE(est.checkpoint_count(), 10ULL);

        // GC old data (reconciler catches up).
        est.gc_below(model::offset{base + 180});
    }

    // After all cycles: count bounded, total exact, interval unchanged.
    EXPECT_LE(est.checkpoint_count(), 10ULL);
    EXPECT_EQ(est.total_bytes(), 50000ULL);
    EXPECT_EQ(est.checkpoint_interval(), 100ULL);
}

TEST(size_estimator_test, format_to) {
    ct::size_estimator est(test_interval);
    est.record(model::offset{0}, 200);

    auto str = fmt::format("{}", est);
    EXPECT_NE(str.find("total_bytes=200"), std::string::npos);
    EXPECT_NE(str.find("checkpoints="), std::string::npos);
}

} // anonymous namespace
