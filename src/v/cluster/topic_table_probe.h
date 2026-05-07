// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "container/chunked_hash_map.h"
#include "metrics/metrics.h"

namespace cluster {

class topic_table_probe {
public:
    explicit topic_table_probe(const topic_table&, model::node_id);

    topic_table_probe(const topic_table_probe&) = delete;
    topic_table_probe(topic_table_probe&&) = delete;
    topic_table_probe& operator=(const topic_table_probe&) = delete;
    topic_table_probe& operator=(topic_table_probe&&) = delete;

    void handle_topic_creation(create_topic_cmd::key_t);
    void handle_topic_deletion(const delete_topic_cmd::key_t&);

    void increment_leadership_changes(model::topic_namespace_view);

    void handle_update(
      const std::vector<model::broker_shard>& previous_replicas,
      const std::vector<model::broker_shard>& result_replicas);
    void handle_update_finish(
      const std::vector<model::broker_shard>& previous_replicas,
      const std::vector<model::broker_shard>& result_replicas);
    void handle_update_cancel(
      const std::vector<model::broker_shard>& previous_replicas,
      const std::vector<model::broker_shard>& result_replicas);
    void handle_update_cancel_finish(
      const std::vector<model::broker_shard>& previous_replicas,
      const std::vector<model::broker_shard>& result_replicas);

private:
    void setup_metrics();
    void setup_public_metrics();
    void setup_internal_metrics();

    struct per_topic_state {
        ss::metrics::metric_groups public_metrics{
          metrics::public_metrics_handle};
        ss::metrics::metric_groups internal_metrics{
          ss::metrics::default_handle()};
        uint64_t leadership_changes{0};
    };

    const topic_table& _topic_table;
    model::node_id _node_id;
    chunked_hash_map<
      model::topic_namespace,
      std::unique_ptr<per_topic_state>,
      model::topic_namespace_hash,
      model::topic_namespace_eq>
      _per_topic;
    metrics::internal_metric_groups _internal_metrics;
    metrics::public_metric_groups _public_metrics;
    int32_t _moving_to_partitions = 0;
    int32_t _moving_from_partitions = 0;
    int32_t _cancelling_movements = 0;
};

} // namespace cluster
