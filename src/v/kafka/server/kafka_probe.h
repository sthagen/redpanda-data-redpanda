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

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics.hh>

#include <chrono>
#include <cstdint>

namespace kafka {
class kafka_probe {
public:
    using hist_t = log_hist_internal;

    kafka_probe() = default;
    kafka_probe(const kafka_probe&) = delete;
    kafka_probe& operator=(const kafka_probe&) = delete;
    kafka_probe(kafka_probe&&) = delete;
    kafka_probe& operator=(kafka_probe&&) = delete;
    ~kafka_probe() = default;

    void setup_metrics() {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        std::vector<sm::label_instance> labels{
          sm::label("latency_metric")("microseconds")};

        _metrics.add_group(
          prometheus_sanitize::metrics_name("kafka:latency"),
          {
            sm::make_histogram(
              "fetch_latency_us",
              sm::description("Fetch Latency"),
              labels,
              [this] { return _fetch_latency.internal_histogram_logform(); }),
            sm::make_histogram(
              "produce_latency_us",
              sm::description("Produce Latency"),
              labels,
              [this] { return _produce_latency.internal_histogram_logform(); }),
          },
          {},
          {sm::shard_label});

        _metrics.add_group(
          "kafka",
          {
            sm::make_histogram(
              "batch_size",
              sm::description(
                "Batch size across all topics measured at the kafka layer."),
              labels,
              [this] { return _batch_size.batch_size_histogram_logform(); }),
          },
          {},
          {sm::shard_label});
    }

    void setup_public_metrics() {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_public_metrics()) {
            return;
        }
        _public_metrics.add_group(
          prometheus_sanitize::metrics_name("kafka"),
          {
            sm::make_histogram(
              "request_latency_seconds",
              sm::description("Internal latency of kafka produce requests"),
              {metrics::make_namespaced_label("request")("produce")},
              [this] { return _produce_latency.public_histogram_logform(); })
              .aggregate({sm::shard_label}),
            sm::make_histogram(
              "request_latency_seconds",
              sm::description("Internal latency of kafka consume requests"),
              {metrics::make_namespaced_label("request")("consume")},
              [this] { return _fetch_latency.public_histogram_logform(); })
              .aggregate({sm::shard_label}),
          });
    }

    std::unique_ptr<hist_t::measurement> auto_produce_measurement() {
        return _produce_latency.auto_measure();
    }

    void record_fetch_latency(std::chrono::microseconds micros) {
        _fetch_latency.record(micros.count());
    }

    void record_batch(uint64_t size) { _batch_size.record(size); }

private:
    hist_t _produce_latency;
    hist_t _fetch_latency;
    // non partition or topic related as that is too expensive for histograms
    batch_size_hist _batch_size;
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace kafka
