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

#pragma once

#include "base/seastarx.h"

#include <seastar/core/lowres_clock.hh>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>

/// Tracks the sum of recorded values over a sliding time window using
/// fixed-size buckets.
///
/// The window size corresponds to (bucket_count - 1) buckets, and an additional
/// bucket is used to track a bucket worth of data just before the start of the
/// window. This additional bucket is included using partial-interpolation to
/// account for the fact that the current bucket is also partial. This smooths
/// the sum over bucket-rolls and avoid systematically undercounting the sum.
template<size_t BucketCount, std::chrono::nanoseconds::rep WindowSizeNanos>
class windowed_sum_tracker {
public:
    using clock = ss::lowres_clock;
    using time_point = clock::time_point;
    using duration = clock::duration;

    /// The number of backing buckets to use
    static constexpr size_t bucket_count = BucketCount;

    /// The sliding window to aggregate over
    static constexpr duration window_size = std::chrono::nanoseconds(
      WindowSizeNanos);

    /// The duration a single bucket covers
    static constexpr duration bucket_size = window_size / (bucket_count - 1);

    static_assert(bucket_count > 1, "bucket_count must be >1");
    static_assert(bucket_size.count() > 0, "bucket_size must be >0");
    static_assert(window_size.count() > 0, "window_size must be >0");

    explicit windowed_sum_tracker(time_point now)
      : _cur_bucket_zero(now) {}

    windowed_sum_tracker()
      : windowed_sum_tracker(clock::now()) {}

    /// Record a value at the specified time.
    void record(uint64_t value, time_point now = clock::now()) {
        advance(now);
        _buckets[_cur_bucket] += value;
    }

    /// Returns the sum of all values recorded within the sliding window
    /// ending at `now`. The window spans [now - window_size, now].
    /// Note: the first bucket corresponding to the beginning of the window may
    /// be interpolated.
    uint64_t window_total(time_point now = clock::now()) const {
        // The _buckets[_cur_bucket] corresponds to the window
        // [_cur_bucket_zero, _cur_bucket_zero + bucket_size], so use an
        // open-ended window to ensure that the current bucket is not
        // extrapolated, since it is already "partial".
        const auto window_end = time_point::max();
        const auto window_start = now - window_size;

        uint64_t total = 0;
        for (size_t i = 0; i < bucket_count; ++i) {
            total += partial_bucket_sum(i, window_start, window_end);
        }
        return total;
    }

private:
    // Advances the bucket ring to the correct position for `now`,
    // clearing buckets that have expired.
    void advance(time_point now) {
        const auto elapsed = now - _cur_bucket_zero;
        if (elapsed < bucket_size) {
            return;
        }

        // Avoid a long loop for long time jumps
        if (elapsed > window_size + bucket_size) {
            std::ranges::fill(_buckets, 0);
            _cur_bucket = 0;
            _cur_bucket_zero = now;
            return;
        }

        // Advance buckets incrementally, clearing expired ones
        const auto buckets_to_advance = static_cast<size_t>(
          elapsed / bucket_size);
        for (size_t i = 0; i < buckets_to_advance; ++i) {
            _cur_bucket = (_cur_bucket + 1) % bucket_count;
            _buckets[_cur_bucket] = 0;
        }
        _cur_bucket_zero += buckets_to_advance * bucket_size;
    }

    constexpr static size_t circular_distance(size_t from, size_t to) {
        return (to + bucket_count - from) % bucket_count;
    }

    // Calculate the contribution of a bucket that may be partially in the
    // window
    // NOLINTBEGIN(bugprone-easily-swappable-parameters)
    uint64_t partial_bucket_sum(
      size_t bucket_idx, time_point window_start, time_point window_end) const {
        // NOLINTEND(bugprone-easily-swappable-parameters)
        const auto buckets_ago = circular_distance(bucket_idx, _cur_bucket);
        const auto bucket_start
          = _cur_bucket_zero
            - duration(static_cast<int64_t>(buckets_ago) * bucket_size.count());
        const auto bucket_end = bucket_start + bucket_size;

        const auto overlap_start = std::max(bucket_start, window_start);
        const auto overlap_end = std::min(bucket_end, window_end);
        const auto overlap = overlap_end - overlap_start;

        if (overlap >= bucket_size) {
            return _buckets[bucket_idx];
        } else if (overlap <= overlap.zero()) {
            return 0;
        }

        // Partial overlap: use lerp to estimate contribution
        const double fraction = static_cast<double>(overlap.count())
                                / static_cast<double>(bucket_size.count());
        return std::llround(
          std::lerp(0.0, static_cast<double>(_buckets[bucket_idx]), fraction));
    }

    std::array<uint64_t, bucket_count> _buckets{};

    // The index of the bucket that we are currently filling
    size_t _cur_bucket{0};

    // The start time of _buckets[_cur_bucket]
    time_point _cur_bucket_zero;
};
