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
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <chrono>
#include <expected>

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
      ss::sharded<cluster::shard_table>* shard_table,
      ss::sharded<cluster::partition_manager>* partition_manager,
      std::chrono::milliseconds retry_backoff = default_retry_backoff);

    ss::future<> stop();

    /// Resolve the partition's home shard, hop there, and replicate the new
    /// min_allowed_local_threshold floor through ctp_stm_api. Returns the
    /// replication result; a partition that is not hosted on this node (or has
    /// no ctp_stm) is reported as not-leader error.
    ss::future<std::expected<void, ctp_stm_api_errc>>
    set_min_allowed_local_threshold(model::ntp ntp, kafka::offset new_floor);

    /// Replicate new_floor through `api`, retrying transient failures up to
    /// max_attempts with the configured backoff. Exposed for testing against a
    /// raft_fixture-backed ctp_stm.
    ss::future<std::expected<void, ctp_stm_api_errc>> replicate_with_retries(
      model::ntp ntp, ctp_stm_api& api, kafka::offset new_floor);

private:
    // Runs on the partition's home shard: look up the ctp_stm via
    // partition_manager and replicate under the per-shard concurrency limit.
    ss::future<std::expected<void, ctp_stm_api_errc>>
    replicate_on_home_shard(model::ntp ntp, kafka::offset new_floor);

    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<cluster::partition_manager>* _partition_manager;
    std::chrono::milliseconds _retry_backoff;
    ssx::semaphore _inflight{
      max_concurrent_replications, "level_zero_notifier::inflight"};
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace cloud_topics
