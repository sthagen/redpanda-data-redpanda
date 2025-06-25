/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "absl/container/flat_hash_map.h"
#include "base/seastarx.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>

/*
 * Class which allows tracking all our registered metrics and their
 * aggregation labels for both values of our aggregate_metrics config flag.
 *
 * Hence, it can be used to easily swap the aggregation labels for all
 * registered metrics when the config value changes. See
 * `aggregate_metrics_watcher`
 *
 * We use it as a global threadlocal instance so that it can easily be accessed
 * from all probes.
 *
 * Note in general "aggregated" metrics refer to the seastar defintion of
 * aggregated labels. I.e.: for two or more metric series that differ only in
 * labels that are "aggregated" labels they will be combined into a single
 * series with their values summed and the aggregated labels removed.
 */
class metrics_registry {
public:
    using group_name_t = ss::sstring;
    using metric_name_t = ss::sstring;
    static metrics_registry& local() { return _local_instance; }

    struct group_info {
        bool has_topic_label{false};
        bool partition_label_aggregated{false};
        bool topic_label_initially_aggregated{false};

        // aggregation labels when aggregate_metrics=false
        std::vector<ss::metrics::label> non_aggregated_labels;
        // aggregation labels when aggregate_metrics=true
        std::vector<ss::metrics::label> aggregated_labels;

        bool should_aggregate_topic_label() const;
    };

    /*!
     * @brief Register a metric with the metrics registry.
     * @param group_name The name of the metric group.
     * @param metric_name The name of the metric in the group
     * @param non_aggregated_labels The labels to be used for aggregation when
     * aggregate_metrics=false
     * @param aggregated_labels The labels to be used for aggregation when
     * aggregate_metrics=true
     */
    const group_info& register_metric(
      const group_name_t& group_name,
      const metric_name_t& metric_name,
      const std::vector<ss::metrics::label>& non_aggregated_labels,
      const std::vector<ss::metrics::label>& aggregated_labels,
      bool has_topic_label);

    // For all registered metrics, update the labels to be used for aggregation
    // depending on the new state of whether aggregation is turned on
    void update_aggregation_labels(bool aggregate_metrics);

    void enable_topic_label_aggregation();
    void disable_topic_label_aggregation();

private:
    static thread_local metrics_registry
      _local_instance; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

    // We store the aggregation labels for a metric:
    // group_name -> metric_name -> aggregation_labels
    absl::flat_hash_map<
      group_name_t,
      absl::flat_hash_map<metric_name_t, group_info>>
      _registry;

    bool _aggregated_topic_label{false};
};
