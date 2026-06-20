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
#include "metrics/metrics.h"
#include "ssx/future-util.h"

#include <seastar/core/metrics.hh>

namespace instance_info {

instance_metrics::instance_metrics(ss::abort_source& as)
  : _as(as) {}

void instance_metrics::start() {
    // Background: detection may hit the cloud metadata service, so it must not
    // block startup. The gate keeps it bounded for shutdown.
    ssx::spawn_with_gate(_gate, [this] { return detect_and_register(); });
}

ss::future<> instance_metrics::stop() { return _gate.close(); }

ss::future<> instance_metrics::detect_and_register() {
    if (!metrics::any_enabled()) {
        // Short-circuit when no metrics are enabled.
        co_return;
    }
    register_metrics(co_await detect_instance(_as));
}

void instance_metrics::register_metrics(
  const std::optional<detected_instance>& inst) {
    if (inst) {
        _instance = *inst;
    }

    namespace sm = ss::metrics;

    // All series live under the "instance" group (redpanda_instance_*), exposed
    // on both the internal and public endpoints (all_metrics_groups). The info
    // metric is always registered and carries an extra instance_has_capacity
    // label; the capacity gauges are registered only when capacity is known, so
    // they go in a separate add_group call below.
    const bool detected = inst.has_value();
    const bool has_capacity = _instance.capacity.has_value();
    const std::string_view provider_label = detected ? to_string_view(
                                                         _instance.provider)
                                                     : "unknown";
    const ss::sstring type_label = detected ? _instance.instance_type
                                            : ss::sstring{"unknown"};

    // The detected identity, repeated on every series so each can be filtered
    // by provider/instance type without joining against the info metric.
    const std::vector<sm::label_instance> identity_labels{
      sm::label("cloud_provider")(provider_label),
      sm::label("instance_type")(type_label)};

    // Info metric: 1 with the detected identity as labels on success, else 0
    // with "unknown" labels. Always registered so dashboards can join on it and
    // tell "not scraped" from "nothing detected". The instance_has_capacity
    // label is 1 when the type was found in the table (capacity gauges below
    // are present), 0 otherwise.
    auto info_labels = identity_labels;
    info_labels.push_back(
      sm::label("instance_has_capacity")(has_capacity ? "1" : "0"));
    _metrics.add_group(
      "instance",
      {sm::make_gauge(
        "info",
        [detected] { return detected ? 1 : 0; },
        sm::description(
          "Cloud instance info (1 if a cloud instance was detected, else 0). "
          "Labels: cloud_provider and instance_type carry the detected "
          "identity, or unknown when nothing was detected; "
          "instance_has_capacity is 1 when the host-capacity gauges below are "
          "present, else 0."),
        info_labels)});

    // Capacity gauges only when the instance type was found in the table.
    if (!_instance.capacity.has_value()) {
        return;
    }

    // Value gauges report constants captured from _instance (which outlives
    // them, this object being non-movable). These are nominal vendor-advertised
    // figures estimated from the instance type, not measured values. They carry
    // the same cloud_provider/instance_type identity labels as the info metric
    // so each series is self-describing without joining against info.
    _metrics.add_group(
      "instance",
      {
        sm::make_gauge(
          "vcpus",
          [this] { return _instance.capacity->vcpus; },
          sm::description("Host vCPU count (estimated from instance type)"),
          identity_labels),
        sm::make_gauge(
          "memory_bytes",
          [this] { return _instance.capacity->memory_bytes; },
          sm::description(
            "Host memory in bytes (estimated from instance type)"),
          identity_labels),
        sm::make_gauge(
          "disk_bytes",
          [this] { return _instance.capacity->disk_bytes; },
          sm::description(
            "Host local SSD capacity in bytes (estimated "
            "from instance type)"),
          identity_labels),
        sm::make_gauge(
          "network_bytes_per_sec",
          [this] { return _instance.capacity->network_bytes_per_sec; },
          sm::description(
            "Host network bandwidth in bytes/sec, one "
            "direction (estimated from instance type)"),
          identity_labels),
        sm::make_gauge(
          "read_iops",
          [this] { return _instance.capacity->read_iops; },
          sm::description(
            "Host local SSD read IOPS (estimated from instance type)"),
          identity_labels),
        sm::make_gauge(
          "write_iops",
          [this] { return _instance.capacity->write_iops; },
          sm::description(
            "Host local SSD write IOPS (estimated from instance type)"),
          identity_labels),
      });
}

} // namespace instance_info
