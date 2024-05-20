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

#include "base/seastarx.h"
#include "cluster/types.h"
#include "container/chunked_hash_map.h"
#include "storage/fwd.h"
#include "utils/mutex.h"
#include "utils/rwlock.h"

#include <seastar/core/sharded.hh>

#include <absl/container/node_hash_map.h>

namespace cluster {

/// Node-local data structure tracking ntp -> shard mapping for partition
/// replicas hosted by this node. Both target state and current shard-local
/// state for each ntp are tracked. Target state is supposed to be modified by
/// shard_balancer and current shard-local state is supposed to be modified by
/// controller_backend as it creates/deletes/moves partitions.
///
/// Shard-local assignments and current states are persisted to kvstore on every
/// change. On startup this kvstore state is used to recover in-memory state.
/// In other words, we read kvstore only when initializing, and during normal
/// operation we only write to it.
///
/// Note that in contrast to `cluster::shard_table` (that helps other parts of
/// the system to find the shard where the `cluster::partition` object for some
/// ntp resides) this is a more granular table that is internal to the partition
/// reconciliation process.
class shard_placement_table
  : public ss::peering_sharded_service<shard_placement_table> {
public:
    // assignment modification methods must be called on this shard
    static constexpr ss::shard_id assignment_shard_id = 0;

    /// Struct used to express the fact that a partition replica of some ntp is
    /// expected on this shard.
    struct shard_local_assignment {
        raft::group_id group;
        model::revision_id log_revision;
        model::shard_revision_id shard_revision;

        friend std::ostream&
        operator<<(std::ostream&, const shard_local_assignment&);
    };

    enum class hosted_status {
        /// Cross-shard transfer is in progress, we are the destination
        receiving,
        /// Normal state, we can start the partition instance.
        hosted,
        /// We have transferred our state to somebody else, now our copy must be
        /// deleted.
        obsolete,
    };

    /// Current state of shard-local partition kvstore data on this shard.
    struct shard_local_state {
        raft::group_id group;
        model::revision_id log_revision;
        hosted_status status;
        model::shard_revision_id shard_revision;

        shard_local_state(
          raft::group_id g,
          model::revision_id lr,
          hosted_status s,
          model::shard_revision_id sr)
          : group(g)
          , log_revision(lr)
          , status(s)
          , shard_revision(sr) {}

        shard_local_state(
          const shard_local_assignment& as, hosted_status status)
          : shard_local_state(
            as.group, as.log_revision, status, as.shard_revision) {}

        friend std::ostream&
        operator<<(std::ostream&, const shard_local_state&);
    };

    enum class reconciliation_action {
        /// Partition must be removed from this node
        remove,
        /// Partition must be transferred to other shard
        transfer,
        /// Wait until target catches up with topic_table
        wait_for_target_update,
        /// Partition must be created on this shard
        create,
    };

    /// A struct holding both current shard-local and target states for an ntp.
    struct placement_state {
        /// Based on expected log revision for this ntp on this node
        /// (queried from topic_table) and the placement state, calculate the
        /// required reconciliation action for this NTP on this shard.
        reconciliation_action get_reconciliation_action(
          std::optional<model::revision_id> expected_log_revision) const;

        friend std::ostream& operator<<(std::ostream&, const placement_state&);

        placement_state() = default;

        /// Current shard-local state for this ntp. Will be non-null if
        /// some kvstore state for this ntp exists on this shard.
        std::optional<shard_local_state> current;
        /// If non-null, the ntp is expected to exist on this shard.
        std::optional<shard_local_assignment> assigned;

    private:
        friend class shard_placement_table;

        /// If placement_state is in the _states map, then is_empty() is false.
        bool is_empty() const {
            return !current && !_is_initial_for && !assigned;
        }

        /// If this shard is the initial shard for some incarnation of this
        /// partition on this node, this field will contain the corresponding
        /// log revision. Invariant: if both _is_initial_for and current
        /// are present, _is_initial_for > current.log_revision
        std::optional<model::revision_id> _is_initial_for;
        /// If x-shard transfer is in progress, will hold the destination. Note
        /// that it is initialized from target but in contrast to target, it
        /// can't change mid-transfer.
        std::optional<ss::shard_id> _next;
    };

    using ntp2state_t = absl::node_hash_map<model::ntp, placement_state>;

    explicit shard_placement_table(storage::kvstore&);

    /// Must be called on assignment_shard_id.
    bool is_persistence_enabled() const;
    ss::future<> enable_persistence();

    /// Must be called on assignment_shard_id.
    /// precondition: is_persistence_enabled() == true
    ss::future<> initialize_from_kvstore(
      const chunked_hash_map<raft::group_id, model::ntp>& local_group2ntp);

    /// Must be called on assignment_shard_id.
    /// precondition: is_persistence_enabled() == false
    ss::future<>
    initialize_from_topic_table(ss::sharded<topic_table>&, model::node_id self);

    using shard_callback_t = std::function<void(const model::ntp&)>;

    /// Must be called only on assignment_shard_id. Shard callback will be
    /// called on shards that have been modified by this invocation.
    ss::future<> set_target(
      const model::ntp&,
      std::optional<shard_placement_target>,
      shard_callback_t);

    // getters

    /// Must be called on assignment_shard_id.
    std::optional<shard_placement_target> get_target(const model::ntp&) const;

    std::optional<placement_state> state_on_this_shard(const model::ntp&) const;

    const ntp2state_t& shard_local_states() const { return _states; }

    // partition lifecycle methods

    ss::future<std::error_code>
    prepare_create(const model::ntp&, model::revision_id expected_log_rev);

    // return value is a tri-state:
    // * if it returns a shard_id value, a transfer to that shard must be
    // performed
    // * if it returns errc::success, transfer has already been performed
    // * else, we must wait before we begin the transfer.
    ss::future<result<ss::shard_id>>
    prepare_transfer(const model::ntp&, model::revision_id expected_log_rev);

    ss::future<> finish_transfer_on_destination(
      const model::ntp&, model::revision_id expected_log_rev);

    ss::future<> finish_transfer_on_source(
      const model::ntp&, model::revision_id expected_log_rev);

    ss::future<std::error_code>
    prepare_delete(const model::ntp&, model::revision_id cmd_revision);

    ss::future<>
    finish_delete(const model::ntp&, model::revision_id expected_log_rev);

private:
    ss::future<> do_delete(
      const model::ntp&,
      placement_state&,
      ss::rwlock::holder& persistence_lock_holder);

    ss::future<> persist_shard_local_state();

    ss::future<ss::foreign_ptr<std::unique_ptr<ntp2state_t>>>
    gather_init_states(const chunked_hash_map<raft::group_id, model::ntp>&);

    struct ntp_init_data;

    ss::future<>
    scatter_init_data(const chunked_hash_map<model::ntp, ntp_init_data>&);

    ss::future<>
    do_initialize_from_topic_table(const topic_table&, model::node_id self);

private:
    friend class shard_placement_test_fixture;

    // per-shard state
    //
    // node_hash_map for pointer stability
    ntp2state_t _states;
    // lock is needed to sync enabling persistence with shard-local
    // modifications.
    ssx::rwlock _persistence_lock;
    bool _persistence_enabled = false;
    storage::kvstore& _kvstore;

    // only on shard 0, _ntp2entry will hold targets for all ntps on this node.
    struct entry_t {
        std::optional<shard_placement_target> target;
        mutex mtx;

        entry_t()
          : mtx("shard_placement_table") {}
    };

    chunked_hash_map<model::ntp, std::unique_ptr<entry_t>> _ntp2entry;
    model::shard_revision_id _cur_shard_revision{0};
};

std::ostream& operator<<(std::ostream&, shard_placement_table::hosted_status);

} // namespace cluster
