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
#include "cluster/topic_table.h"
#include "config/property.h"

#include <seastar/core/future.hh>

#include <optional>

namespace cluster {

/// Monitors the number of topics in the cluster and controls whether the topic
/// label should be aggregated.
class topic_metrics_watcher {
    // When topic count hovers around the aggregation limit we want to avoid
    // constantly enabling/disbaling topic label aggregation.
    constexpr static long double hysteresis_coeff = (1.0 + 0.05);

public:
    topic_metrics_watcher(
      topic_table& tt,
      config::binding<std::optional<size_t>>&& topic_agg_limit);

    ss::future<> stop();

private:
    ss::gate _gate;
    topic_table& _tt;
    config::binding<std::optional<size_t>> _topic_agg_limit;
    bool is_aggregated{false};

    void maybe_aggregate_topic_label();
};
} // namespace cluster
