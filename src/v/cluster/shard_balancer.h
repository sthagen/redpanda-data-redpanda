/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/controller_backend.h"
#include "cluster/shard_placement_table.h"
#include "container/chunked_hash_map.h"
#include "ssx/event.h"
#include "utils/mutex.h"

namespace cluster {

/// shard_balancer runs on shard 0 of each node and manages assignments of
/// partitions hosted on this node to shards. It does this by modifying
/// shard_placement_table and notifying controller_backend of these modification
/// so that it can perform necessary reconciling actions.
///
/// If node-local core assignment is enabled, shard_balancer is responsible for
/// assigning partitions to shards and it will do it using topic-aware counts
/// balancing heuristic. In legacy mode it will use shard assignments from
/// topic_table.
class shard_balancer {
public:
    // single instance
    static constexpr ss::shard_id shard_id = 0;

    shard_balancer(
      ss::sharded<shard_placement_table>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<topic_table>&,
      ss::sharded<controller_backend>&);

    ss::future<> start();
    ss::future<> stop();

    /// Persist current shard_placement_table contents to kvstore. Executed once
    /// when enabling the node_local_core_assignment feature (assumes that it is
    /// in the "preparing" state).
    ss::future<> enable_persistence();

    /// Manually set shard placement for an ntp that has a replica on this node.
    ss::future<errc> reassign_shard(model::ntp, ss::shard_id);

private:
    void process_delta(const topic_table::delta&);

    ss::future<> assign_fiber();
    ss::future<> do_assign_ntps(mutex::units& lock);

    void maybe_assign(
      const model::ntp&,
      chunked_hash_map<model::ntp, std::optional<shard_placement_target>>&);

    ss::future<> set_target(
      const model::ntp&,
      const std::optional<shard_placement_target>&,
      mutex::units& lock);

    using shard2count_t = std::vector<int32_t>;
    struct topic_data_t {
        explicit topic_data_t()
          : shard2count(ss::smp::count, 0) {}

        int32_t total_count = 0;
        shard2count_t shard2count;
    };

    ss::shard_id choose_shard(
      const model::ntp&,
      const topic_data_t&,
      std::optional<ss::shard_id> prev) const;

    void update_counts(
      const model::ntp&,
      topic_data_t&,
      const std::optional<shard_placement_target>& prev,
      const std::optional<shard_placement_target>& next);

private:
    shard_placement_table& _shard_placement;
    features::feature_table& _features;
    ss::sharded<topic_table>& _topics;
    ss::sharded<controller_backend>& _controller_backend;
    model::node_id _self;

    cluster::notification_id_type _topic_table_notify_handle;
    ssx::event _wakeup_event{"shard_balancer"};
    mutex _mtx{"shard_balancer"};
    ss::gate _gate;

    chunked_hash_set<model::ntp> _to_assign;

    chunked_hash_map<
      model::topic_namespace,
      topic_data_t,
      model::topic_namespace_hash,
      model::topic_namespace_eq>
      _topic2data;
    shard2count_t _total_counts;
};

} // namespace cluster
