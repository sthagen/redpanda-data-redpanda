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

#include "metrics/metrics.h"
#include "model/fundamental.h"

namespace cluster_link::replication {

class partition_replicator;

class replication_probe {
public:
    struct configuration {
        ss::sstring group_name;
        std::vector<ss::metrics::label_instance> labels;
    };

    explicit replication_probe(
      configuration, ::model::ntp, partition_replicator&);
    replication_probe(const replication_probe&) = delete;
    replication_probe& operator=(const replication_probe&) = delete;
    replication_probe(replication_probe&&) = delete;
    replication_probe& operator=(replication_probe&&) = delete;
    ~replication_probe() = default;

    void setup_metrics(configuration, ::model::ntp);

private:
    partition_replicator& _partition_replicator;

    metrics::public_metric_groups _public_metrics;
};
} // namespace cluster_link::replication
