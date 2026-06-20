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

#pragma once

#include "base/seastarx.h"
#include "metrics/instance_type_detector.h"
#include "metrics/metrics.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <optional>

namespace instance_info {

/// Detects the local node's cloud instance type and, once known, exposes its
/// info (vCPUs, memory, disk, network, IOPS) as Prometheus metrics on the
/// internal and public endpoints.
///
/// Detection runs in the background (kicked off by start()), so startup is
/// never blocked on the cloud metadata service. The "info" series is always
/// registered: with the detected identity and value 1 on success, or with
/// "unknown" labels and value 0 when nothing was detected, so a missing series
/// means "not scraped" rather than "not detected". The capacity gauges are only
/// registered when the instance type is found in the table. All values are
/// constant for the life of the process.
class instance_metrics {
public:
    explicit instance_metrics(ss::abort_source& as);
    instance_metrics(const instance_metrics&) = delete;
    instance_metrics& operator=(const instance_metrics&) = delete;
    instance_metrics(instance_metrics&&) = delete;
    instance_metrics& operator=(instance_metrics&&) = delete;
    ~instance_metrics() = default;

    /// Kick off background detection + registration. Returns immediately.
    void start();

    /// Wait for the background detection to finish (or be aborted).
    ss::future<> stop();

    /// Register the metrics for a detection result. nullopt means nothing was
    /// detected (info reads 0 with "unknown" labels). Normally invoked by the
    /// background detection started by start(); exposed for tests.
    void register_metrics(const std::optional<detected_instance>& inst);

private:
    ss::future<> detect_and_register();

    ss::abort_source& _as;
    ss::gate _gate;
    detected_instance _instance;
    metrics::all_metrics_groups _metrics;
};

} // namespace instance_info
