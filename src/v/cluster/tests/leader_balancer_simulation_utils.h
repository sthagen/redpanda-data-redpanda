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

#include "cluster/scheduling/leader_balancer_strategy.h"
#include "cluster/scheduling/leader_balancer_types.h"
#include "cluster/tests/leader_balancer_constraints_utils.h"
#include "cluster/tests/leader_balancer_test_utils.h"
#include "random/generators.h"

#include <ranges>

namespace leader_balancer_test_utils {

namespace lbt = cluster::leader_balancer_types;

/**
 * @brief Data needed to construct a leader balancer strategy for simulation.
 */
struct simulation_cluster_data {
    cluster::leader_balancer_strategy::index_type index;
    lbt::group_id_to_topic_id group_to_topic;
    lbt::muted_index muted_index;
};

/**
 * @brief Prepares cluster data for leader balancer simulation.
 *
 * Creates a cluster where the last node starts empty (no leaders).
 * The returned data can be used to construct a strategy for simulation.
 *
 * @tparam node_count The number of nodes in the cluster
 * @return simulation_cluster_data containing index, group_to_topic mapping, and
 * muted_index
 */
template<int node_count>
simulation_cluster_data prepare_simulation_cluster() {
    constexpr auto n_groups = 50000;
    constexpr auto groups = std::views::iota(0, n_groups);
    // the last node starts without leaders
    constexpr auto n_with_leaders = node_count - 1;

    cluster_spec spec(node_count);
    // reserve generously to allow for randomization imperfections
    for (auto& node_spec : spec | std::views::take(n_with_leaders)) {
        constexpr auto expected_leaders_per_node = n_groups / n_with_leaders;
        node_spec.groups_led.reserve(2 * expected_leaders_per_node);
    }
    for (auto& node_spec : spec) {
        constexpr auto expected_followers_per_node = 2 * n_groups / node_count;
        node_spec.groups_followed.reserve(2 * expected_followers_per_node);
    }

    // functions to select random non-last nodes
    constexpr auto random_excluding_one =
      []<int total = node_count - 1>(int excluded) {
          return (random_generators::get_int(1, total - 1) + excluded) % total;
      };
    constexpr auto random_excluding_two = [random_excluding_one](
                                            int excluded1, int excluded2) {
        // all indexes below are mod (node_count - 1)
        constexpr auto mod = node_count - 1;
        // exclude (-1) and (excluded2 - excluded1 - 1)
        auto unadjusted = random_excluding_one.template operator()<mod - 1>(
          (excluded2 - excluded1 - 1 + mod) % mod);
        // adjust, so that the excluded ones are excluded1 and excluded2
        return (unadjusted + excluded1 + 1) % mod;
    };

    for (auto g_id : groups) {
        // Leaders are placed uniformly at random on all nodes but last
        auto leader_node = random_generators::get_int(0, n_with_leaders - 1);
        spec[leader_node].groups_led.push_back(g_id);

        // first follower: not with leader, not on last node (see below why)
        auto flwr1_node = random_excluding_one(leader_node);
        spec[flwr1_node].groups_followed.push_back(g_id);

        // Second follower: not with leader or with first follower.
        // For uniform replica distribution place followers onto the last node
        // with a higher probability. 3 * n_groups / node_count is the expected
        // number of followers on each node, so 3.0 / node_count is the
        // probability for each group to have a replica there.
        bool use_last_node_for_follower = random_generators::get_real<double>()
                                          <= 3.0 / node_count;
        auto flwr2_node = use_last_node_for_follower
                            ? (node_count - 1)
                            : random_excluding_two(leader_node, flwr1_node);
        spec[flwr2_node].groups_followed.push_back(g_id);
    }

    vassert(
      std::ranges::all_of(
        spec,
        [](const auto& node_spec) {
            constexpr auto expected_replicas_per_node = 3 * n_groups
                                                        / node_count;
            auto replicas_per_node = node_spec.groups_led.size()
                                     + node_spec.groups_followed.size();
            return expected_replicas_per_node * 0.8 <= replicas_per_node
                   && replicas_per_node <= 1.3 * expected_replicas_per_node;
        }),
      "The number of replicas per node significantly differs from expected");

    auto [shard_index, muted_index] = from_spec(spec, {});

    // group N belongs to topic N % 2,
    // so that topic affiliation is independent from replica placements
    auto g_id_to_t_id = group_to_topic_from_spec(
      {std::from_range,
       std::array{0, 1} | std::views::transform([&groups](int t_id) {
           return group_to_ntp_spec::value_type{
             t_id, {std::from_range, groups | std::views::filter([t_id](int g) {
                                         return g % 2 == t_id;
                                     })}};
       })});

    return {
      copy_cluster_index(shard_index.shards()),
      std::move(g_id_to_t_id),
      std::move(muted_index)};
}

} // namespace leader_balancer_test_utils
