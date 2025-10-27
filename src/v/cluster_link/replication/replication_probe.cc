/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/replication_probe.h"

#include "cluster_link/replication/partition_replicator.h"
#include "config/configuration.h"

#include <seastar/core/metrics.hh>

namespace cluster_link::replication {

replication_probe::replication_probe(
  configuration config, ::model::ntp ntp, partition_replicator& pr)
  : _partition_replicator{pr}
  , _public_metrics{} {
    setup_metrics(std::move(config), ntp);
}

void replication_probe::setup_metrics(configuration config, ::model::ntp ntp) {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    auto topic_label = metrics::make_namespaced_label("topic");
    auto partition_label = metrics::make_namespaced_label("partition");

    std::vector<sm::label_instance> labels = {
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };
    std::ranges::copy(config.labels, std::back_inserter(labels));

    _public_metrics.add_group(
      config.group_name,
      {
        sm::make_gauge(
          "shadow_lag",
          [this] { return _partition_replicator.get_partition_lag(); },
          sm::description(
            "Lag of the shadow partition against the source partition"),
          labels)
          .aggregate({sm::shard_label}),
      });
}

} // namespace cluster_link::replication
