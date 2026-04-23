// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"

#include "base/vlog.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "raft/errc.h"
#include "rpc/errc.h"

#include <algorithm>
#include <iterator>

namespace cluster {

bool are_replica_sets_equal(
  const std::vector<model::broker_shard>& lhs,
  const std::vector<model::broker_shard>& rhs) {
    auto l_sorted = lhs;
    auto r_sorted = rhs;
    static const auto cmp =
      [](const model::broker_shard& lhs, const model::broker_shard& rhs) {
          return lhs.node_id < rhs.node_id;
      };
    std::sort(l_sorted.begin(), l_sorted.end(), cmp);
    std::sort(r_sorted.begin(), r_sorted.end(), cmp);

    return l_sorted == r_sorted;
}

custom_assignable_topic_configuration_vector
without_custom_assignments(topic_configuration_vector topics) {
    custom_assignable_topic_configuration_vector assignable_topics;
    assignable_topics.reserve(topics.size());
    std::transform(
      std::make_move_iterator(topics.begin()),
      std::make_move_iterator(topics.end()),
      std::back_inserter(assignable_topics),
      [](topic_configuration cfg) {
          return custom_assignable_topic_configuration(std::move(cfg));
      });
    return assignable_topics;
}

cluster::errc map_update_interruption_error_code(std::error_code ec) {
    if (ec.category() == cluster::error_category()) {
        return cluster::errc(ec.value());
    } else if (ec.category() == raft::error_category()) {
        switch (raft::errc(ec.value())) {
        case raft::errc::success:
            return errc::success;
        case raft::errc::not_leader:
            return errc::not_leader_controller;
        case raft::errc::timeout:
            return errc::timeout;
        case raft::errc::disconnected_endpoint:
        case raft::errc::exponential_backoff:
        case raft::errc::non_majority_replication:
        case raft::errc::vote_dispatch_error:
        case raft::errc::append_entries_dispatch_error:
        case raft::errc::replicated_entry_truncated:
        case raft::errc::leader_flush_failed:
        case raft::errc::leader_append_failed:
        case raft::errc::configuration_change_in_progress:
        case raft::errc::node_does_not_exists:
        case raft::errc::leadership_transfer_in_progress:
        case raft::errc::transfer_to_current_leader:
        case raft::errc::node_already_exists:
        case raft::errc::invalid_configuration_update:
        case raft::errc::not_voter:
        case raft::errc::invalid_target_node:
        case raft::errc::shutting_down:
        case raft::errc::replicate_batcher_cache_error:
        case raft::errc::group_not_exists:
        case raft::errc::replicate_first_stage_exception:
        case raft::errc::invalid_input_records:
            return errc::replication_error;
        }
        __builtin_unreachable();
    } else if (ec.category() == rpc::error_category()) {
        switch (rpc::errc(ec.value())) {
        case rpc::errc::success:
            return errc::success;
        case rpc::errc::client_request_timeout:
        case rpc::errc::connection_timeout:
            return errc::timeout;
        case rpc::errc::shutting_down:
            return errc::shutting_down;
        case rpc::errc::disconnected_endpoint:
        case rpc::errc::exponential_backoff:
        case rpc::errc::missing_node_rpc_client:
        case rpc::errc::service_error:
        case rpc::errc::method_not_found:
        case rpc::errc::version_not_supported:
        case rpc::errc::service_unavailable:
        case rpc::errc::unknown:
            return errc::replication_error;
        }
        __builtin_unreachable();
    } else {
        vlog(
          clusterlog.warn,
          "mapping {} error to uknown update interruption error",
          ec.message());
        return errc::unknown_update_interruption_error;
    }
}

std::optional<ss::sstring> check_result_configuration(
  const members_table::cache_t& current_brokers,
  const model::broker& new_configuration) {
    auto it = current_brokers.find(new_configuration.id());
    if (it == current_brokers.end()) {
        return fmt::format(
          "broker {} does not exists in list of cluster members",
          new_configuration.id());
    }
    auto& current_configuration = it->second.broker;

    /**
     * When cluster member configuration changes Redpanda by default doesn't
     * allow the change if a new cluster configuration would have two
     * listeners pointing to the same address. This was in conflict with the
     * logic of join request which didn't execute validation of resulting
     * configuration. Change the validation logic to only check configuration
     * fields which were changed and are going to be updated.
     */
    const bool rpc_address_changed = current_configuration.rpc_address()
                                     != new_configuration.rpc_address();
    std::vector<model::broker_endpoint> changed_endpoints;
    for (auto& new_ep : new_configuration.kafka_advertised_listeners()) {
        auto it = std::find_if(
          current_configuration.kafka_advertised_listeners().begin(),
          current_configuration.kafka_advertised_listeners().end(),
          [&new_ep](const model::broker_endpoint& ep) {
              return ep.name == new_ep.name;
          });

        if (
          it == current_configuration.kafka_advertised_listeners().end()
          || it->address != new_ep.address) {
            changed_endpoints.push_back(new_ep);
        }
    }

    for (const auto& [id, current] : current_brokers) {
        if (id == new_configuration.id()) {
            continue;
        }

        /**
         * validate if any two of the brokers would listen on the same addresses
         * after applying configuration update
         */
        if (
          rpc_address_changed
          && current.broker.rpc_address() == new_configuration.rpc_address()) {
            // error, nodes would listen on the same rpc addresses
            return fmt::format(
              "duplicate rpc endpoint {} with existing node {}",
              new_configuration.rpc_address(),
              id);
        }

        for (auto& changed_ep : changed_endpoints) {
            auto any_is_the_same = std::any_of(
              current.broker.kafka_advertised_listeners().begin(),
              current.broker.kafka_advertised_listeners().end(),
              [&changed_ep](const model::broker_endpoint& ep) {
                  return changed_ep == ep;
              });
            // error, kafka endpoint would point to the same addresses
            if (any_is_the_same) {
                return fmt::format(
                  "duplicated kafka advertised endpoint {} with existing node "
                  "{}",
                  changed_ep,
                  id);
                ;
            }
        }
    }
    return {};
}

} // namespace cluster
