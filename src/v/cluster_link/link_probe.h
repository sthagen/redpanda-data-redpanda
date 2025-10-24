/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster_link/link.h"
#include "cluster_link/replication/types.h"
#include "metrics/metrics.h"

namespace cluster_link {
class link_probe {
public:
    link_probe(link& link);
    link_probe(const link_probe&) = delete;
    link_probe& operator=(const link_probe&) = delete;
    link_probe(link_probe&&) = delete;
    link_probe& operator=(link_probe&&) = delete;
    ~link_probe() = default;

    void setup_counter_metrics();
    void setup_status_metrics();
    void reset_status_metrics() { _public_status_metrics.reset(); }

    void handle_on_leadership_change(
      const ::model::ntp& ntp, ntp_leader is_ntp_leader);

    replication::link_data_probe_ptr get_link_data() { return _fetch_data; }

    static inline const ss::sstring shadow_link_group{"shadow_link"};
    static inline const ss::metrics::label shadow_link_name{"shadow_link_name"};

private:
    int count_topics(const model::mirror_topic_status) const;

    link& _link;

    replication::link_data_probe_ptr _fetch_data;

    // These metrics are defined on every shard
    metrics::public_metric_groups _public_counter_metrics;

    // These metrics are only defined on the controller leader
    std::optional<metrics::public_metric_groups> _public_status_metrics;
};
} // namespace cluster_link
