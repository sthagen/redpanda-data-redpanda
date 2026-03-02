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

#include "cluster/scheduling/leader_balancer_random.h"
#include "leader_balancer_simulation_utils.h"

#include <gtest/gtest.h>

namespace lbt = cluster::leader_balancer_types;

constexpr int simulation_runs = 10;

/**
 * @brief Runs the leader balancer strategy until no more moves are found.
 * @return The number of leader reassignments performed
 */
inline int run_simulation(cluster::leader_balancer_strategy& strategy) {
    lbt::muted_groups_t muted_groups{};
    int n_moves = 0;
    for (;;) {
        auto movement_opt = strategy.find_movement(muted_groups);
        if (!movement_opt) {
            break;
        }
        ++n_moves;
        strategy.apply_movement(*movement_opt);
        muted_groups.add(static_cast<uint64_t>(movement_opt->group));
    }
    return n_moves;
}

template<
  std::derived_from<cluster::leader_balancer_strategy> ClimbingStrategy,
  int node_count>
double check_simulation_moves_count() {
    std::vector<int> results;
    results.reserve(simulation_runs);

    for (int i = 0; i < simulation_runs; ++i) {
        auto cluster_data
          = leader_balancer_test_utils::prepare_simulation_cluster<
            node_count>();

        auto strategy = ClimbingStrategy(
          std::move(cluster_data.index),
          std::move(cluster_data.group_to_topic),
          std::move(cluster_data.muted_index),
          std::nullopt);

        int n_moves = run_simulation(strategy);
        results.push_back(n_moves);
    }

    double sum = std::ranges::fold_left(results, 0.0, std::plus<>{});
    return sum / static_cast<double>(simulation_runs);
}

template<int node_count>
void compare_simulation_moves_count() {
    auto calibrated_moves = check_simulation_moves_count<
      lbt::calibrated_hill_climbing_strategy,
      node_count>();
    auto random_moves = check_simulation_moves_count<
      lbt::random_hill_climbing_strategy,
      node_count>();
    EXPECT_LT(calibrated_moves, 0.9 * random_moves);
}

TEST(LeaderBalancerSimulation, CompareMovesCount3) {
    compare_simulation_moves_count<3>();
}

TEST(LeaderBalancerSimulation, CompareMovesCount7) {
    compare_simulation_moves_count<7>();
}
