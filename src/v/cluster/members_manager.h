/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "config/seed_server.h"
#include "config/tls_config.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/types.h"
#include "random/simple_time_jitter.h"
#include "rpc/fwd.h"
#include "storage/fwd.h"

namespace cluster {

// Implementation of a raft::mux_state_machine that is responsible for
// updating information about cluster members, joining the cluster, updating
// member states, and creating intra-cluster connections.
//
// This class receives updates from members_frontend by way of a Raft record
// batch being committed. In addition to various controller command batch
// types, it reacts to Raft configuration batch types, e.g. when a new node is
// added to the controller Raft group.
//
// All the updates are propagated to core-local cluster::members_table
// instances. There is only one instance of members_manager running on
// core-0. The members_manager is also responsible for validation of node
// configuration invariants.
class members_manager {
public:
    static constexpr auto accepted_commands = make_commands_list<
      decommission_node_cmd,
      recommission_node_cmd,
      finish_reallocations_cmd,
      maintenance_mode_cmd>{};
    static constexpr ss::shard_id shard = 0;
    static constexpr size_t max_updates_queue_size = 100;

    // Update types, used for communication between the manager and backend.
    //
    // NOTE: maintenance mode doesn't interact with the members_backend,
    // instead interacting with each core via their respective drain_manager.
    enum class node_update_type : int8_t {
        // A node has been added to the cluster.
        added,

        // A node has been decommissioned from the cluster.
        decommissioned,

        // A node has been recommissioned after an incomplete decommission.
        recommissioned,

        // All reallocations associated with a given node update have completed
        // (e.g. it's been fully decommissioned, indicating it can no longer be
        // recommissioned).
        reallocation_finished,
    };

    // Node update information to be processed by the members_backend.
    struct node_update {
        model::node_id id;
        node_update_type type;
        model::offset offset;

        bool is_commissioning() const {
            return type == members_manager::node_update_type::decommissioned
                   || type == members_manager::node_update_type::recommissioned;
        }

        friend std::ostream& operator<<(std::ostream&, const node_update&);
    };

    members_manager(
      consensus_ptr,
      ss::sharded<members_table>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<storage::api>&,
      ss::sharded<drain_manager>&,
      ss::sharded<ss::abort_source>&);

    // Checks invariants. Must be called before calling start().
    ss::future<> validate_configuration_invariants();

    // Initializes connections to all known members.
    ss::future<> start();

    // Sends a join RPC if we aren't already a member, else sends a node
    // configuration update if our local state differs from that stored in the
    // members table.
    //
    // This is separate to start() so that calling it can be delayed until
    // after our internal RPC listener is up: as soon as we send a join
    // message, the controller leader will expect us to be listening for its
    // raft messages, and if we're not ready it'll back off and make joining
    // take several seconds longer than it should.
    // (ref https://github.com/redpanda-data/redpanda/issues/3030)
    ss::future<> join_cluster();

    // Stop this manager. Only prevents new update requests; pending updates in
    // the queue are aborted separately.
    ss::future<> stop();

    // Adds a node to the controller Raft group, dispatching to the leader if
    // necessary. If the node already exists, just updates the node config
    // instead.
    ss::future<result<join_node_reply>>
    handle_join_request(join_node_request const r);

    // Applies a committed record batch, specializing handling based on the
    // batch type.
    ss::future<std::error_code> apply_update(model::record_batch);

    // Updates the configuration of a node in the existing controller Raft
    // config, dispatching to the leader if necessary.
    ss::future<result<configuration_update_reply>>
      handle_configuration_update_request(configuration_update_request);

    // Whether the given batch applies to this raft::mux_state_machine.
    bool is_batch_applicable(const model::record_batch& b) {
        return b.header().type == model::record_batch_type::node_management_cmd
               || b.header().type
                    == model::record_batch_type::raft_configuration;
    }

    // This API is backed by the seastar::queue. It can not be called
    // concurrently from multiple fibers.
    ss::future<std::vector<node_update>> get_node_updates();

private:
    using seed_iterator = std::vector<config::seed_server>::const_iterator;
    // Cluster join
    void join_raft0();
    bool is_already_member() const;

    ss::future<> initialize_broker_connection(const model::broker&);

    ss::future<result<join_node_reply>> dispatch_join_to_seed_server(
      seed_iterator it, join_node_request const& req);
    ss::future<result<join_node_reply>> dispatch_join_to_remote(
      const config::seed_server&, join_node_request&& req);

    ss::future<join_node_reply> dispatch_join_request();
    template<typename Func>
    auto dispatch_rpc_to_leader(rpc::clock_type::duration, Func&& f);

    // Raft 0 config updates
    ss::future<>
      handle_raft0_cfg_update(raft::group_configuration, model::offset);
    ss::future<> update_connections(patch<broker_ptr>);

    ss::future<> maybe_update_current_node_configuration();
    ss::future<> dispatch_configuration_update(model::broker);
    ss::future<result<configuration_update_reply>>
      do_dispatch_configuration_update(model::broker, model::broker);

    template<typename Cmd>
    ss::future<std::error_code> dispatch_updates_to_cores(model::offset, Cmd);

    ss::future<std::error_code>
      apply_raft_configuration_batch(model::record_batch);

    const std::vector<config::seed_server> _seed_servers;
    const model::broker _self;
    simple_time_jitter<model::timeout_clock> _join_retry_jitter;
    const std::chrono::milliseconds _join_timeout;
    const consensus_ptr _raft0;
    ss::sharded<members_table>& _members_table;
    ss::sharded<rpc::connection_cache>& _connection_cache;

    // Partition allocator to update when receiving node lifecycle commands.
    ss::sharded<partition_allocator>& _allocator;

    // Storage with which to look at the kv-store to store and verify
    // configuration invariants.
    //
    // TODO: since the members manager is only ever on shard-0, seems like we
    // should just be passing a single reference to the kv-store.
    ss::sharded<storage::api>& _storage;

    // Per-core management of operations that remove leadership away from a
    // node. Needs to be per-core since each shard is managed by a different
    // shard of the sharded partition_manager.
    ss::sharded<drain_manager>& _drain_manager;

    ss::sharded<ss::abort_source>& _as;

    const config::tls_config _rpc_tls_config;

    // Gate with which to guard new work (e.g. if stop() has been called).
    ss::gate _gate;

    // Cluster membership updates that have yet to be released via the call to
    // get_node_updates().
    ss::queue<node_update> _update_queue;

    // Subscription to _as with which to signal an abort to _update_queue.
    ss::abort_source::subscription _queue_abort_subscription;

    // The last config update controller log offset for which we successfully
    // updated our broker connections.
    model::offset _last_connection_update_offset;
};

std::ostream&
operator<<(std::ostream&, const members_manager::node_update_type&);
} // namespace cluster
