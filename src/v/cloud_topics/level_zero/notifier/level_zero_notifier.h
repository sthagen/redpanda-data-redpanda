/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "rpc/fwd.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <chrono>
#include <expected>
#include <optional>

namespace cloud_topics {

/// Per-shard service that replicates post-compaction local-retention floor
/// (min_allowed_local_threshold) updates to the partition that owns them.
///
/// L1 compaction (running on the maintenance shard) hands the new floor for a
/// partition to set_min_allowed_local_threshold. The service resolves the
/// partition's home shard, hops there via container().invoke_on, and
/// replicates the floor through ctp_stm_api, retrying transient failures up to
/// max_attempts. The replication result is returned to the caller, which
/// decides whether a failure is fatal -- the call is not fire-and-forget.
class level_zero_notifier
  : public ss::peering_sharded_service<level_zero_notifier> {
public:
    /// Maximum number of replication attempts before giving up.
    static constexpr int max_attempts = 3;

    /// Default backoff between replication attempts.
    static constexpr auto default_retry_backoff = std::chrono::seconds{5};

    /// Maximum number of floor replications in flight on a single shard.
    static constexpr size_t max_concurrent_replications = 16;

    level_zero_notifier(
      model::node_id self,
      ss::sharded<cluster::partition_leaders_table>* leaders,
      ss::sharded<cluster::metadata_cache>* metadata,
      ss::sharded<cluster::shard_table>* shard_table,
      ss::sharded<cluster::partition_manager>* partition_manager,
      ss::sharded<rpc::connection_cache>* connections,
      std::chrono::milliseconds retry_backoff = default_retry_backoff);

    ss::future<> stop();

    /// Replicate the new min_allowed_local_threshold floor for a locally
    /// observed partition (i.e. a notification originating on this broker, from
    /// this or another shard). The floor is first applied locally; if local
    /// replication reports not_leader (leadership has moved away from this
    /// broker) the call falls back to forwarding it to the new leader over RPC,
    /// waiting for that leader to appear in cluster metadata (max_attempts
    /// metadata lookups, default_retry_backoff between them). The partition is
    /// identified by its topic_id_partition (matching L1), resolved to an ntp
    /// via the metadata cache. The call is not fire-and-forget.
    ss::future<std::expected<void, ctp_stm_api_errc>>
    set_min_allowed_local_threshold(
      model::topic_id_partition tidp, kafka::offset new_floor);

    /// Resolve the partition's home shard on THIS broker and replicate the new
    /// floor through ctp_stm_api. Used by the RPC handler on the leader node:
    /// it performs NO leader resolution or forwarding, so a broker that
    /// receives the RPC never forwards it again.
    ss::future<std::expected<void, ctp_stm_api_errc>>
    set_min_allowed_local_threshold_locally(
      model::topic_id_partition tidp, kafka::offset new_floor);

    /// Replicate new_floor through `api` in a single attempt. Exposed for
    /// testing against a raft_fixture-backed ctp_stm.
    ss::future<std::expected<void, ctp_stm_api_errc>>
    replicate(model::ntp ntp, ctp_stm_api& api, kafka::offset new_floor);

private:
    // Resolve a topic_id_partition to an ntp via the metadata cache. Returns
    // nullopt when the topic id is unknown (e.g. deleted topic or stale
    // metadata on this node).
    std::optional<model::ntp>
    resolve_ntp(const model::topic_id_partition& tidp) const;

    // Resolve the home shard on THIS broker and replicate on it.
    ss::future<std::expected<void, ctp_stm_api_errc>>
    replicate_locally(model::ntp ntp, kafka::offset new_floor);

    // Forward the floor update to the partition's new leader over RPC, retrying
    // the metadata lookup max_attempts times (default_retry_backoff between
    // attempts) while the new leader is elected and propagated. Returns
    // not_leader if no remote leader appears.
    ss::future<std::expected<void, ctp_stm_api_errc>>
    wait_for_leader_and_dispatch(
      model::ntp ntp, model::topic_id_partition tidp, kafka::offset new_floor);

    // Runs on the partition's home shard: look up the ctp_stm via
    // partition_manager and replicate under the per-shard concurrency limit.
    ss::future<std::expected<void, ctp_stm_api_errc>>
    replicate_on_home_shard(model::ntp ntp, kafka::offset new_floor);

    // Send the floor update to a remote leader node via a single RPC.
    ss::future<std::expected<void, ctp_stm_api_errc>> remote_dispatch(
      model::node_id leader,
      model::topic_id_partition tidp,
      kafka::offset new_floor);

    // This node's id; used to detect the local-leader case and as the RPC
    // source node.
    model::node_id _self;
    ss::sharded<cluster::partition_leaders_table>* _leaders;
    ss::sharded<cluster::metadata_cache>* _metadata;
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<rpc::connection_cache>* _connections;
    std::chrono::milliseconds _retry_backoff;
    ssx::semaphore _inflight{
      max_concurrent_replications, "level_zero_notifier::inflight"};
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace cloud_topics
