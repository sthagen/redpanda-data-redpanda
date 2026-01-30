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

#include "absl/container/flat_hash_map.h"
#include "cluster/scheduling/leader_balancer_constraints.h"
#include "cluster/scheduling/leader_balancer_types.h"
#include "config/leaders_preference.h"
#include "model/metadata.h"
#include "raft/fundamental.h"

#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

namespace lbt = cluster::leader_balancer_types;

class PinningConstraintTest : public ::testing::Test {
protected:
    // Helper to create a reassignment
    static lbt::reassignment create_reassignment(
      raft::group_id group_id,
      model::node_id from_node,
      model::node_id to_node) {
        return lbt::reassignment{
          group_id,
          model::broker_shard{.node_id = from_node, .shard = 0},
          model::broker_shard{.node_id = to_node, .shard = 0}};
    }

    // Helper to setup basic test data
    void SetUp() override {
        // Setup group to topic mapping
        _group_to_topic[raft::group_id{0}] = lbt::topic_id_t{0};
        _group_to_topic[raft::group_id{1}] = lbt::topic_id_t{1};
        _group_to_topic[raft::group_id{2}] = lbt::topic_id_t{2};

        for (auto node_number : std::ranges::views::iota(0, 5)) {
            const auto node_id = model::node_id{node_number};
            const auto rack_id = rack_from_node(node_id);
            _node_to_rack[node_id] = rack_id;
            auto& node_pair = _rack_to_nodes[rack_id];
            if (node_number % 2 == 0) {
                node_pair.first = node_id;
            } else {
                node_pair.second = node_id;
            }
            _last_configured_node = node_id;
        }
    }

protected:
    model::rack_id rack_from_node(model::node_id node_id) {
        return rack_from_number(int(node_id) / 2);
    }

    model::rack_id rack_from_number(int number) {
        return model::rack_id{ss::sstring{fmt::format("rack{}", number)}};
    }

    model::node_id pick_a_node(model::rack_id rack_id) {
        return _rack_to_nodes[rack_id].first;
    }

    lbt::group_id_to_topic_id _group_to_topic;
    absl::flat_hash_map<model::node_id, model::rack_id> _node_to_rack;
    absl::
      flat_hash_map<model::rack_id, std::pair<model::node_id, model::node_id>>
        _rack_to_nodes;
    model::node_id _last_configured_node{-1};
};

namespace {
// easier to differentiate bad good neutral than LT GT EQ
constexpr inline void expect_bad_reconfiguration(double value) {
    EXPECT_LT(value, 0.0) << "expected a reconfiguration value less than 0";
}
constexpr inline void expect_good_reconfiguration(double value) {
    EXPECT_GT(value, 0.0) << "expected a reconfiguration value greater than 0";
}
constexpr inline void expect_neutral_reconfiguration(double value) {
    EXPECT_EQ(value, 0.0) << "expected a reconfiguration value of 0";
}
} // namespace

TEST_F(PinningConstraintTest, OrderedRacksPriorityBasedMovement) {
    // Test that the ordered_racks preference type correctly evaluates
    // movements based on rack priority order

    const auto default_group = raft::group_id{1};

    // Create preference with ordered racks: rack0 > rack1 > rack2
    lbt::preference_index pref_idx;
    pref_idx.node2rack = _node_to_rack;
    pref_idx.default_preference.type
      = config::leaders_preference::type_t::ordered_racks;
    pref_idx.default_preference.racks = {
      rack_from_number(0), rack_from_number(1), rack_from_number(2)};

    lbt::pinning_constraint constraint(_group_to_topic, std::move(pref_idx));

    {
        // low pri to higher pri, (rack2 -> rack0)
        auto reassignment = create_reassignment(
          default_group,
          pick_a_node(rack_from_number(2)),
          pick_a_node(rack_from_number(0)));
        expect_good_reconfiguration(constraint.evaluate(reassignment));
    }

    {
        // high pri to lower pri, (rack0 -> rack2)
        auto reassignment = create_reassignment(
          default_group,
          pick_a_node(rack_from_number(0)),
          pick_a_node(rack_from_number(2)));
        expect_bad_reconfiguration(constraint.evaluate(reassignment));
    }

    {
        // no change (rack0 -> rack0), different nodes, though
        auto reassignment = create_reassignment(
          default_group,
          _rack_to_nodes[rack_from_number(0)].first,
          _rack_to_nodes[rack_from_number(0)].second);
        expect_neutral_reconfiguration(constraint.evaluate(reassignment));
    }

    {
        // litmus test, lower to higher (rack1 -> rack0)
        auto reassignment = create_reassignment(
          default_group,
          pick_a_node(rack_from_number(1)),
          pick_a_node(rack_from_number(0)));
        expect_good_reconfiguration(constraint.evaluate(reassignment));
    }
}

TEST_F(PinningConstraintTest, OrderedRacksWithUnknownRacks) {
    // Test behavior for nodes that either have racks not in the preference or
    // nodes with no rack whatsoever

    const auto default_group = raft::group_id{1};

    auto last_configured_node = _last_configured_node;
    const auto unknown_rack_node = model::node_id{++last_configured_node};
    const auto missing_rack_node = model::node_id{++last_configured_node};

    // Add a node in an unknown rack
    _node_to_rack[unknown_rack_node] = model::rack_id{"rack_unknown"};

    // Create preference with ordered racks: rack0 > rack1
    lbt::preference_index pref_idx;
    pref_idx.node2rack = _node_to_rack;
    pref_idx.default_preference.type
      = config::leaders_preference::type_t::ordered_racks;
    pref_idx.default_preference.racks = {
      rack_from_number(0), rack_from_number(1)};

    lbt::pinning_constraint constraint(_group_to_topic, std::move(pref_idx));

    for (const auto invalid_node_id : {unknown_rack_node, missing_rack_node}) {
        // invalid here is going to refer to either an unknown rack_id or node
        // with missing rack_id
        {
            // invalid rack to preferred rack (rack_invalid -> rack0)
            auto reassignment = create_reassignment(
              default_group, invalid_node_id, pick_a_node(rack_from_number(0)));
            expect_good_reconfiguration(constraint.evaluate(reassignment));
        }

        {
            // preferred rack to invalid rack (rack0 -> rack_invalid)
            auto reassignment = create_reassignment(
              default_group, pick_a_node(rack_from_number(0)), invalid_node_id);
            expect_bad_reconfiguration(constraint.evaluate(reassignment));
        }

        {
            // between invalid/non-preferred racks (rack_invalid -> rack2)
            // no preference
            auto reassignment = create_reassignment(
              default_group, invalid_node_id, pick_a_node(rack_from_number(2)));
            expect_neutral_reconfiguration(constraint.evaluate(reassignment));
        }
    }

    // check that theres no preference between unspecified and missing rack
    {
        // unknown to missing
        auto reassignment = create_reassignment(
          default_group, unknown_rack_node, missing_rack_node);
        expect_neutral_reconfiguration(constraint.evaluate(reassignment));
    }
    {
        // missing to unknown
        auto reassignment = create_reassignment(
          default_group, missing_rack_node, unknown_rack_node);
        expect_neutral_reconfiguration(constraint.evaluate(reassignment));
    }
}

TEST_F(PinningConstraintTest, OrderedRacksPerTopicPreference) {
    // Test that topic-specific preferences override default preferences

    lbt::preference_index pref_idx;
    pref_idx.node2rack = _node_to_rack;

    // Default preference: rack1 > rack2
    pref_idx.default_preference.type
      = config::leaders_preference::type_t::ordered_racks;
    pref_idx.default_preference.racks = {
      rack_from_number(1), rack_from_number(2)};

    // Topic 2 specific preference: rack2 > rack1 (reversed)
    lbt::leaders_preference topic2_pref;
    topic2_pref.type = config::leaders_preference::type_t::ordered_racks;
    topic2_pref.racks = {rack_from_number(2), rack_from_number(1)};
    pref_idx.topic2preference[lbt::topic_id_t{2}] = topic2_pref;

    lbt::pinning_constraint constraint(_group_to_topic, std::move(pref_idx));

    {
        // topic 0 uses default preference (rack1 > rack2)
        // movement from rack2 to rack1 should improve
        auto reassignment = create_reassignment(
          raft::group_id{0},
          pick_a_node(rack_from_number(2)),
          pick_a_node(rack_from_number(1)));
        expect_good_reconfiguration(constraint.evaluate(reassignment));
    }

    {
        // topic 2 uses reversed preference (rack2 > rack1)
        // same movement (rack2 to rack1) should degrade
        auto reassignment = create_reassignment(
          raft::group_id{2},
          pick_a_node(rack_from_number(2)),
          pick_a_node(rack_from_number(1)));
        expect_bad_reconfiguration(constraint.evaluate(reassignment));
    }
}

TEST_F(PinningConstraintTest, OrderedRacksEmptyPreference) {
    // Test behavior with empty rack list

    const auto default_group = raft::group_id{1};

    // Create preference with no racks specified
    lbt::preference_index pref_idx;
    pref_idx.node2rack = _node_to_rack;
    pref_idx.default_preference.type
      = config::leaders_preference::type_t::ordered_racks;
    pref_idx.default_preference.racks = {}; // Empty rack list

    lbt::pinning_constraint constraint(_group_to_topic, std::move(pref_idx));

    {
        // all movements should be neutral when no racks are preferred
        auto reassignment = create_reassignment(
          default_group,
          pick_a_node(rack_from_number(0)),
          pick_a_node(rack_from_number(1)));
        expect_neutral_reconfiguration(constraint.evaluate(reassignment));
    }

    {
        // test movement from higher numbered rack to lower numbered rack
        auto reassignment = create_reassignment(
          default_group,
          pick_a_node(rack_from_number(2)),
          pick_a_node(rack_from_number(0)));
        expect_neutral_reconfiguration(constraint.evaluate(reassignment));
    }
}

TEST_F(PinningConstraintTest, CompareOrderedVsUnorderedBehavior) {
    // Test to ensure ordered_racks behaves differently from unordered racks

    const auto default_group = raft::group_id{1};

    // Setup ordered preference: rack1 > rack2
    lbt::preference_index ordered_pref_idx;
    ordered_pref_idx.node2rack = _node_to_rack;
    ordered_pref_idx.default_preference.type
      = config::leaders_preference::type_t::ordered_racks;
    ordered_pref_idx.default_preference.racks = {
      rack_from_number(1), rack_from_number(2)};

    // Setup unordered preference (regular racks type) with same racks
    lbt::preference_index unordered_pref_idx;
    unordered_pref_idx.node2rack = _node_to_rack;
    unordered_pref_idx.default_preference.type
      = config::leaders_preference::type_t::racks;
    unordered_pref_idx.default_preference.racks = {
      rack_from_number(1), rack_from_number(2)};

    lbt::pinning_constraint ordered_constraint(
      _group_to_topic, std::move(ordered_pref_idx));
    lbt::pinning_constraint unordered_constraint(
      _group_to_topic, std::move(unordered_pref_idx));

    {
        // movement between preferred racks (rack2 -> rack1)
        auto reassignment = create_reassignment(
          default_group,
          pick_a_node(rack_from_number(2)),
          pick_a_node(rack_from_number(1)));

        // ordered should show improvement (lower priority to higher priority)
        expect_good_reconfiguration(ordered_constraint.evaluate(reassignment));

        // unordered should be neutral (both are equally preferred)
        expect_neutral_reconfiguration(
          unordered_constraint.evaluate(reassignment));
    }

    {
        // reverse movement (rack1 -> rack2)
        auto reassignment = create_reassignment(
          default_group,
          pick_a_node(rack_from_number(1)),
          pick_a_node(rack_from_number(2)));

        // ordered should show degradation (higher priority to lower priority)
        expect_bad_reconfiguration(ordered_constraint.evaluate(reassignment));

        // unordered should still be neutral (both are equally preferred)
        expect_neutral_reconfiguration(
          unordered_constraint.evaluate(reassignment));
    }
}
