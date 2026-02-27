// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"
#include "cluster/metadata_cache.h"
#include "cluster/shard_table.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/tests/utils.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "test_utils/async.h"
#include "test_utils/boost_fixture.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/sstring.hh>

#include <bits/stdint-intn.h>
#include <boost/test/tools/old/interface.hpp>
using namespace std::chrono_literals; // NOLINT

cluster::topic_configuration
create_topic_cfg(ss::sstring topic, int32_t p, int16_t r) {
    return cluster::topic_configuration(
      test_ns, model::topic(std::move(topic)), p, r);
}

FIXTURE_TEST(test_creating_same_topic_twice, cluster_test_fixture) {
    // add three nodes
    auto node_0 = create_node_application(model::node_id{0});
    wait_for_controller_leadership(node_0->controller->self()).get();
    create_node_application(model::node_id{1});
    create_node_application(model::node_id{2});

    // wait for cluster to be stable
    tests::cooperative_spin_wait_with_timeout(5s, [this] {
        return get_local_cache(model::node_id{0}).node_count() == 3
               && get_local_cache(model::node_id{1}).node_count() == 3
               && get_local_cache(model::node_id{2}).node_count() == 3;
    }).get();

    std::vector<ss::future<std::vector<cluster::topic_result>>> futures;

    futures.push_back(
      node_0->controller->get_topics_frontend().local().create_topics(
        cluster::without_custom_assignments({create_topic_cfg("test-1", 1, 3)}),
        model::timeout_clock::now() + 10s));

    futures.push_back(
      node_0->controller->get_topics_frontend().local().create_topics(
        cluster::without_custom_assignments({create_topic_cfg("test-1", 2, 3)}),
        model::timeout_clock::now() + 10s));

    ss::when_all_succeed(futures.begin(), futures.end())
      .then([](std::vector<std::vector<cluster::topic_result>> results) {
          BOOST_REQUIRE_EQUAL(results.size(), 2);
          size_t successes = 0;
          for (const auto& v : results) {
              for (const auto& inner : v) {
                  if (inner.ec == cluster::errc::success) {
                      ++successes;
                  }
              }
          }
          BOOST_REQUIRE_EQUAL(successes, 1);
      })
      .get();
}

FIXTURE_TEST(
  test_bootstrap_params_propagated_to_partition_manager, cluster_test_fixture) {
    // Create a single node cluster
    auto node_0 = create_node_application(model::node_id{0});
    wait_for_controller_leadership(node_0->controller->self()).get();

    auto& topics_frontend = node_0->controller->get_topics_frontend().local();
    auto& recovery_table
      = node_0->controller->get_cluster_recovery_table().local();
    auto& recovery_manager
      = node_0->controller->get_cluster_recovery_manager().local();

    // Define test parameters
    auto tp_ns = model::topic_namespace(test_ns, model::topic("test-topic"));
    model::partition_id pid(0);
    model::ntp ntp(tp_ns.ns, tp_ns.tp, pid);

    // Set bootstrap params BEFORE creating the topic
    // The metastore provides [start_offset, next_offset) range.
    // The recovered partition should start from next_offset.
    model::offset expected_start_offset(1000);
    model::offset expected_next_offset(2000);
    model::term_id expected_term(5);

    absl::btree_map<model::partition_id, cluster::partition_bootstrap_params>
      params;
    params[pid] = cluster::partition_bootstrap_params{
      expected_start_offset, expected_next_offset, expected_term};

    auto ec = recovery_manager
                .set_bootstrap_params(
                  tp_ns, std::move(params), model::timeout_clock::now() + 10s)
                .get();

    BOOST_REQUIRE(!ec);

    // Verify the params are stored in cluster_recovery_table
    auto stored_params = recovery_table.get_partition_bootstrap_params(ntp);
    BOOST_REQUIRE(stored_params.has_value());
    BOOST_REQUIRE_EQUAL(stored_params->start_offset, expected_start_offset);
    BOOST_REQUIRE_EQUAL(stored_params->next_offset, expected_next_offset);
    BOOST_REQUIRE_EQUAL(stored_params->initial_term, expected_term);

    // Now create the topic (1 partition, replication factor 1)
    auto result = topics_frontend
                    .create_topics(
                      cluster::without_custom_assignments(
                        {create_topic_cfg("test-topic", 1, 1)}),
                      model::timeout_clock::now() + 10s)
                    .get();

    BOOST_REQUIRE_EQUAL(result.size(), 1);
    BOOST_REQUIRE_EQUAL(result[0].ec, cluster::errc::success);

    // Wait for the partition to be created and find its shard
    auto& shard_table = node_0->controller->get_shard_table().local();
    auto& partition_manager = node_0->partition_manager;

    std::optional<ss::shard_id> partition_shard;
    tests::cooperative_spin_wait_with_timeout(
      10s,
      [&shard_table, &ntp, &partition_shard] {
          partition_shard = shard_table.shard_for(ntp);
          return partition_shard.has_value();
      })
      .get();

    BOOST_REQUIRE(partition_shard.has_value());

    // Wait for the partition to exist on the correct shard
    tests::cooperative_spin_wait_with_timeout(
      10s,
      [&partition_manager, &ntp, shard = partition_shard.value()] {
          return partition_manager
            .invoke_on(
              shard,
              [&ntp](cluster::partition_manager& pm) {
                  return pm.get(ntp) != nullptr;
              })
            .get();
      })
      .get();

    // The raft start offset should be the bootstrap next_offset since the
    // recovered partition starts from next_offset (empty, first batch will
    // have offset equal to next_offset).
    auto actual_start_offset = partition_manager
                                 .invoke_on(
                                   partition_shard.value(),
                                   [&ntp](cluster::partition_manager& pm) {
                                       auto p = pm.get(ntp);
                                       return p ? p->raft_start_offset()
                                                : model::offset{};
                                   })
                                 .get();

    BOOST_CHECK_EQUAL(actual_start_offset, expected_next_offset);
}
