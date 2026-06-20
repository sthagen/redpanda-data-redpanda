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

#include "metrics/instance_metrics.h"
#include "metrics/instance_type_detector.h"
#include "test_utils/metrics.h"

#include <seastar/core/abort_source.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>
#include <utility>

using namespace instance_info;

using ::testing::Optional;

// Registration touches the seastar metric registry, so it must run on a
// reactor; the gtest harness runs each test on a seastar thread.
TEST(instance_metrics, register_metrics_without_error) {
    detected_instance inst{
      .provider = cloud_provider::aws,
      .instance_type = "m6id.4xlarge",
      .capacity = capacity_info{
        .vcpus = 16,
        .memory_bytes = 64ULL * 1024 * 1024 * 1024,
        .disk_bytes = 950'000'000'000ULL,
        .network_bytes_per_sec = 2'500'000'000ULL,
        .read_iops = 400'000,
        .write_iops = 200'000,
      }};

    ss::abort_source as;
    instance_metrics metrics{as};
    EXPECT_NO_THROW(metrics.register_metrics(inst));
}

// Every "instance" series should carry the detected cloud_provider and
// instance_type identity labels. find_metric_value looks a series up by its
// exact label set (it adds the shard label itself), so a present value proves
// the series carries precisely these labels with these values; a missing label
// or a stray extra one would make the lookup miss and return nullopt.
TEST(instance_metrics, series_carry_identity_labels) {
    detected_instance inst{
      .provider = cloud_provider::aws,
      .instance_type = "m6id.4xlarge",
      .capacity = capacity_info{
        .vcpus = 16,
        .memory_bytes = 64ULL * 1024 * 1024 * 1024,
        .disk_bytes = 950'000'000'000ULL,
        .network_bytes_per_sec = 2'500'000'000ULL,
        .read_iops = 400'000,
        .write_iops = 200'000,
      }};

    ss::abort_source as;
    instance_metrics metrics{as};
    metrics.register_metrics(inst);

    const auto h = ss::metrics::default_handle();
    const std::initializer_list<std::pair<ss::sstring, ss::sstring>> id{
      {"cloud_provider", "aws"}, {"instance_type", "m6id.4xlarge"}};

    // Capacity gauges: present under the identity labels and reporting the
    // values from the detected instance.
    EXPECT_THAT(
      test_utils::find_metric_value<uint64_t>("instance_vcpus", h, id),
      Optional(16));
    EXPECT_THAT(
      test_utils::find_metric_value<uint64_t>("instance_memory_bytes", h, id),
      Optional(64ULL * 1024 * 1024 * 1024));
    EXPECT_THAT(
      test_utils::find_metric_value<uint64_t>("instance_disk_bytes", h, id),
      Optional(950'000'000'000ULL));
    EXPECT_THAT(
      test_utils::find_metric_value<uint64_t>(
        "instance_network_bytes_per_sec", h, id),
      Optional(2'500'000'000ULL));
    EXPECT_THAT(
      test_utils::find_metric_value<uint64_t>("instance_read_iops", h, id),
      Optional(400'000));
    EXPECT_THAT(
      test_utils::find_metric_value<uint64_t>("instance_write_iops", h, id),
      Optional(200'000));

    // Info metric: the identity labels plus instance_has_capacity=1, reading 1
    // because the instance was fully detected.
    EXPECT_THAT(
      test_utils::find_metric_value<uint64_t>(
        "instance_info",
        h,
        {{"cloud_provider", "aws"},
         {"instance_type", "m6id.4xlarge"},
         {"instance_has_capacity", "1"}}),
      Optional(1));
}

// Nothing detected: only the info metric (value 0, "unknown" labels) is
// registered. The capacity gauges are skipped.
TEST(instance_metrics, register_metrics_no_detection) {
    ss::abort_source as;
    instance_metrics metrics{as};
    EXPECT_NO_THROW(metrics.register_metrics(std::nullopt));
}

// Detected, but the instance type is absent from the table: the identity is
// known (info metric) but capacity is not, so the capacity gauges are skipped.
TEST(instance_metrics, register_metrics_detected_without_capacity) {
    detected_instance inst{
      .provider = cloud_provider::aws,
      .instance_type = "unknown.instance.type",
      .capacity = std::nullopt};

    ss::abort_source as;
    instance_metrics metrics{as};
    EXPECT_NO_THROW(metrics.register_metrics(inst));
}
