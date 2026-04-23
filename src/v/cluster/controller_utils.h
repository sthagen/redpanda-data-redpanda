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
#include "cluster/controller_stm.h"
#include "cluster/fwd.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"

#include <seastar/core/sharded.hh>

#include <optional>

namespace cluster {

class partition;

template<typename Cmd>
ss::future<std::error_code> replicate_and_wait(
  ss::sharded<controller_stm>& stm,
  ss::sharded<ss::abort_source>& as,
  Cmd&& cmd,
  model::timeout_clock::time_point timeout,
  std::optional<model::term_id> term = std::nullopt) {
    return stm.invoke_on(
      controller_stm_shard,
      [cmd = std::forward<Cmd>(cmd), term, &as, timeout](
        controller_stm& stm) mutable {
          return do_replicate_and_wait(
            stm, as.local(), std::forward<Cmd>(cmd), timeout, term);
      });
}

template<typename Cmd>
ss::future<std::error_code> do_replicate_and_wait(
  controller_stm& stm,
  ss::abort_source& as,
  Cmd&& cmd,
  model::timeout_clock::time_point timeout,
  std::optional<model::term_id> term = std::nullopt) {
    vassert(
      ss::this_shard_id() == controller_stm_shard,
      "do_replicate_and_wait must be called on controller_stm_shard");

    if (!stm.throttle<Cmd>()) {
        return ss::make_ready_future<std::error_code>(
          errc::throttling_quota_exceeded);
    }

    auto b = serde_serialize_cmd(std::forward<Cmd>(cmd));
    return stm.replicate_and_wait(std::move(b), timeout, as, term);
}

/// Calculates expected log revision of a partition with replicas assignment
/// determined by partition_replicas_view on a particular node (if the partition
/// is expected to be there)
std::optional<model::revision_id> log_revision_on_node(
  const topic_table::partition_replicas_view&, model::node_id);

/// Calculates the partition placement target (i.e. log revision and shard id)
/// on a particular node of a partition with replicas assignment determined by
/// partition_replicas_view (including effects of an in-progress or cancelled
/// update if present).
std::optional<shard_placement_target> placement_target_on_node(
  const topic_table::partition_replicas_view&, model::node_id);

partition_state get_partition_state(ss::lw_shared_ptr<cluster::partition>);
partition_raft_state get_partition_raft_state(consensus_ptr);
std::vector<partition_stm_state> get_partition_stm_state(consensus_ptr);

/// Copies the state of all persisted stms from source kvs
ss::future<> copy_persistent_stm_state(
  model::ntp ntp,
  storage::kvstore& source_kvs,
  ss::shard_id target_shard,
  ss::sharded<storage::api>&);

ss::future<> remove_persistent_stm_state(model::ntp ntp, storage::kvstore&);

/// Copies all bits of partition kvstore state from source kvstore to kvstore on
/// target shard.
ss::future<> copy_persistent_state(
  const model::ntp&,
  raft::group_id,
  storage::kvstore& source_kvs,
  ss::shard_id target_shard,
  ss::sharded<storage::api>&);

/// Removes all bits of partition kvstore state in source kvstore.
ss::future<> remove_persistent_state(
  const model::ntp&, raft::group_id, storage::kvstore& source_kvs);

} // namespace cluster
