/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "base/likely.h"
#include "base/vassert.h"
#include "utils/named_type.h"

#include <array>
#include <chrono>
#include <iterator>
#include <numeric>
#include <ratio>

/*
 * simple moving average with configurable sample count
 */

template<typename T, size_t Samples>
class moving_average {
public:
    explicit moving_average(T initial_value)
      : _value(initial_value) {}

    void update(T v) {
        _running_sum -= _samples[_idx];
        _samples[_idx] = v;
        _running_sum += v;
        _idx = (_idx + 1) % Samples;
        if (unlikely(_valid_samples < Samples)) {
            _valid_samples++;
        }
        _value = _running_sum / _valid_samples;
    }

    T get() const { return _value; }

private:
    std::array<T, Samples> _samples{0};
    T _running_sum{0};
    T _value{0};
    size_t _idx{0};
    size_t _valid_samples{0};
};

template<typename T, typename Clock>
class timed_moving_average {
    /// Timestamp normalized by resolution
    using normalized_timestamp_t
      = named_type<size_t, struct normalized_time_tag>;
    struct bucket {
        T value{};
        size_t num_samples{0};
        normalized_timestamp_t ix;
    };

public:
    using clock_t = Clock;
    using duration_t = typename Clock::duration;
    using timestamp_t = typename Clock::time_point;

    timed_moving_average(
      T initial_value,
      std::chrono::nanoseconds depth,
      std::chrono::nanoseconds resolution = std::chrono::milliseconds(100))
      : _num_buckets(depth.count() / resolution.count())
      , _resolution(resolution)
      , _buckets(_num_buckets, bucket{})
      , _end(_num_buckets) {
        vassert(_num_buckets > 0, "Resolution is too small");
        _buckets[index(_end)] = {
          .value = initial_value, .num_samples = 1, .ix = _end};
    }

    timed_moving_average(
      std::chrono::nanoseconds depth, std::chrono::nanoseconds resolution)
      : _num_buckets(depth.count() / resolution.count())
      , _resolution(resolution)
      , _buckets(_num_buckets, bucket{})
      , _end(_num_buckets) {
        vassert(_num_buckets > 0, "Resolution is too small");
    }

    /**
     * @brief Updates moving average. Note that timestamps shouldn't ever move
     * backwards
     *
     * @param v The new sample to be added to the average
     * @param ts The timestamp for when the sample occurred.
     * @param num_samples If `v` contains more than one sample indicate how many
     * here. Note that this must be greater than 0.
     */
    void update(T v, timestamp_t ts, size_t num_samples = 1) noexcept {
        vassert(
          num_samples > 0, "at least one sample needs to be in the update");
        // NOTE: we have to add num_buckets to the value to avoid
        // overflow.
        auto end = normalize(ts) + normalized_timestamp_t(_num_buckets);
        vassert(
          end >= _end,
          "Timestamp moved backward in time, current {}, previous {}",
          _end,
          end);
        _end = end;
        auto& bucket = _buckets[index(end)];
        if (bucket.ix == _end) {
            bucket.value += v;
            bucket.num_samples += num_samples;
        } else {
            bucket.ix = _end;
            bucket.value = v;
            bucket.num_samples = num_samples;
        }
    }

    /**
     * @brief Returns the current average. Note that at least one sample needs
     * to be added to the average before calling this method.
     */
    T get() const noexcept {
        T running_sum{};
        size_t num_samples = 0;
        for (size_t i = 0; i < _num_buckets; i++) {
            auto ix = _end - normalized_timestamp_t(i);
            auto& b = _buckets[index(ix)];
            if (b.ix != ix) {
                break;
            }
            running_sum += b.value;
            num_samples += b.num_samples;
        }
        vassert(num_samples != 0, "timed_moving_average invariant broken");
        return running_sum / num_samples;
    }

    /**
     * @brief Returns whether any samples have been added to this average.
     */
    bool has_samples() const noexcept {
        // The only time the average won't have any samples is between when it
        // was created without an initial value and when the first update
        // occurs. Hence that is the only case that is checked here.
        const auto& b = _buckets[index(_end)];
        if (b.ix != _end) {
            return false;
        }

        return b.num_samples > 0;
    }

private:
    size_t index(normalized_timestamp_t ts) const noexcept {
        return ts() % _num_buckets;
    }
    normalized_timestamp_t normalize(timestamp_t ts) {
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
          ts.time_since_epoch());
        auto norm = nanos.count() / _resolution.count();
        return normalized_timestamp_t(norm);
    }
    size_t _num_buckets;
    std::chrono::nanoseconds _resolution;
    std::vector<bucket> _buckets;
    normalized_timestamp_t _end;
};
