/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "utils/moving_average.h"

#include <seastar/core/lowres_clock.hh>

#include <optional>

namespace kafka {

struct partition_metadata {
    model::offset start_offset;
    model::offset high_watermark;
    model::offset last_stable_offset;
    size_t avg_bytes_per_offset;
};

using namespace std::chrono_literals;

class fetch_metadata_cache {
public:
    explicit fetch_metadata_cache()
      : _eviction_timer([this] { evict(); }) {
        _eviction_timer.arm_periodic(eviction_timeout);
    }

    fetch_metadata_cache(const fetch_metadata_cache&) = delete;
    fetch_metadata_cache(fetch_metadata_cache&&) = delete;

    fetch_metadata_cache& operator=(const fetch_metadata_cache&) = delete;
    fetch_metadata_cache& operator=(fetch_metadata_cache&&) = delete;
    ~fetch_metadata_cache() = default;

    void insert_or_assign(
      const model::ktp_with_hash& ktp,
      model::offset start_offset,
      model::offset hw,
      model::offset lso,
      size_t offset_count,
      size_t total_size_bytes) {
        auto& e = _cache[ktp];
        e.reset_ts();
        if (offset_count > 0) {
            if (e.bytes_per_offset.has_samples()) {
                auto avg_bytes_per_offset = static_cast<double>(
                  e.bytes_per_offset.get());
                auto bytes_per_offset = static_cast<double>(total_size_bytes)
                                        / static_cast<double>(offset_count);
                auto abs_err = std::abs(
                  bytes_per_offset - avg_bytes_per_offset);
                const auto epsilon = 1e-6; // prevent division by zero
                auto re = abs_err / (bytes_per_offset + epsilon);
                _error.update(re, e.timestamp);
            }

            e.bytes_per_offset.update(
              total_size_bytes, e.timestamp, offset_count);
        }
        e.md = {
          .start_offset = start_offset,
          .high_watermark = hw,
          .last_stable_offset = lso,
          .avg_bytes_per_offset = e.bytes_per_offset.has_samples()
                                    ? e.bytes_per_offset.get()
                                    : 0,
        };
    }

    std::optional<partition_metadata> get(const model::ktp_with_hash& ktp) {
        auto it = _cache.find(ktp);
        return it != _cache.end()
                 ? std::make_optional<partition_metadata>(it->second.md)
                 : std::nullopt;
    }

    /**
     * @brief Return the number of items currently cached.
     */
    size_t size() const { return _cache.size(); }

    void setup_metrics() {
        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        namespace sm = ss::metrics;
        _metrics.add_group(
          prometheus_sanitize::metrics_name("kafka:fetch_metadata_cache"),
          {sm::make_gauge(
            "error",
            [this] { return _error.has_samples() ? _error.get() : 0.0; },
            sm::description(
              "A moving average of the relative error for bytes per offset."))},
          {},
          {sm::shard_label});
    }

private:
    using entry_sliding_window_t
      = timed_moving_average<size_t, ss::lowres_clock>;
    using error_sliding_window_t
      = timed_moving_average<double, ss::lowres_clock>;

    constexpr static std::chrono::seconds eviction_timeout{60};
    constexpr static auto bytes_per_offset_window{1h};
    constexpr static auto bytes_per_offset_resolution{20min};

    struct entry {
        partition_metadata md;
        ss::lowres_clock::time_point timestamp;
        entry_sliding_window_t bytes_per_offset{
          bytes_per_offset_window, bytes_per_offset_resolution};

        void reset_ts() { timestamp = ss::lowres_clock::now(); }
    };

    void evict() {
        const auto now = ss::lowres_clock::now();
        std::erase_if(_cache, [&now](const auto& e) {
            return (e.second.timestamp + eviction_timeout) < now;
        });
    }

    chunked_hash_map<model::ktp_with_hash, entry> _cache;
    ss::timer<> _eviction_timer;
    error_sliding_window_t _error{
      bytes_per_offset_window, bytes_per_offset_resolution};
    metrics::internal_metric_groups _metrics;
};
} // namespace kafka
