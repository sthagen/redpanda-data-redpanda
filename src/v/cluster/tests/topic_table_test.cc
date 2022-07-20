// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/topic_table_fixture.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/types.h"

#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_map.h>

using namespace std::chrono_literals;

FIXTURE_TEST(test_happy_path_create, topic_table_fixture) {
    create_topics();
    auto md = table.local().all_topics_metadata();

    BOOST_REQUIRE_EQUAL(md.size(), 3);

    BOOST_REQUIRE(md.contains(make_tp_ns("test_tp_1")));
    BOOST_REQUIRE(md.contains(make_tp_ns("test_tp_2")));
    BOOST_REQUIRE(md.contains(make_tp_ns("test_tp_3")));

    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_1"))->second.get_assignments().size(), 1);
    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_2"))->second.get_assignments().size(), 12);
    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_3"))->second.get_assignments().size(), 8);

    // check delta
    auto d = table.local().wait_for_changes(as).get0();

    validate_delta(d, 21, 0);
}

FIXTURE_TEST(test_happy_path_delete, topic_table_fixture) {
    create_topics();
    // discard create delta
    table.local().wait_for_changes(as).get0();
    auto res_1 = table.local()
                   .apply(
                     cluster::delete_topic_cmd(
                       make_tp_ns("test_tp_2"), make_tp_ns("test_tp_2")),
                     model::offset(0))
                   .get0();
    auto res_2 = table.local()
                   .apply(
                     cluster::delete_topic_cmd(
                       make_tp_ns("test_tp_3"), make_tp_ns("test_tp_3")),
                     model::offset(0))
                   .get0();

    auto md = table.local().all_topics_metadata();
    BOOST_REQUIRE_EQUAL(md.size(), 1);
    BOOST_REQUIRE(md.contains(make_tp_ns("test_tp_1")));

    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_1"))->second.get_assignments().size(), 1);
    // check delta
    auto d = table.local().wait_for_changes(as).get0();

    validate_delta(d, 0, 20);
}

FIXTURE_TEST(test_conflicts, topic_table_fixture) {
    create_topics();
    // discard create delta
    table.local().wait_for_changes(as).get0();

    auto res_1 = table.local()
                   .apply(
                     cluster::delete_topic_cmd(
                       make_tp_ns("not_exists"), make_tp_ns("not_exists")),
                     model::offset(0))
                   .get0();
    BOOST_REQUIRE_EQUAL(res_1, cluster::errc::topic_not_exists);

    auto res_2 = table.local()
                   .apply(
                     make_create_topic_cmd("test_tp_1", 2, 3), model::offset(0))
                   .get0();
    BOOST_REQUIRE_EQUAL(res_2, cluster::errc::topic_already_exists);
    BOOST_REQUIRE_EQUAL(table.local().has_pending_changes(), false);
}

FIXTURE_TEST(get_getting_config, topic_table_fixture) {
    create_topics();
    auto cfg = table.local().get_topic_cfg(make_tp_ns("test_tp_1"));
    BOOST_REQUIRE(cfg.has_value());
    auto v = cfg.value();
    BOOST_REQUIRE_EQUAL(
      v.properties.compaction_strategy, model::compaction_strategy::offset);

    BOOST_REQUIRE_EQUAL(
      v.properties.cleanup_policy_bitflags,
      model::cleanup_policy_bitflags::compaction);
    BOOST_REQUIRE_EQUAL(v.properties.compression, model::compression::lz4);
    BOOST_REQUIRE_EQUAL(
      v.properties.retention_bytes, tristate(std::make_optional(2_GiB)));
    BOOST_REQUIRE_EQUAL(
      v.properties.retention_duration,
      tristate(std::make_optional(std::chrono::milliseconds(3600000))));
}

FIXTURE_TEST(test_wait_aborted, topic_table_fixture) {
    ss::abort_source local_as;
    ss::timer<> timer;
    timer.set_callback([&local_as] { local_as.request_abort(); });
    timer.arm(500ms);
    // discard create delta
    BOOST_REQUIRE_THROW(
      table.local().wait_for_changes(local_as).get0(),
      ss::abort_requested_exception);
}

FIXTURE_TEST(test_adding_partition, topic_table_fixture) {
    // discard create delta
    create_topics();
    table.local().wait_for_changes(as).get0();
    cluster::create_partitions_configuration cfg(make_tp_ns("test_tp_2"), 3);
    std::vector<cluster::partition_assignment> p_as{
      cluster::partition_assignment{
        raft::group_id(10),
        model::partition_id(0),
        {model::broker_shard{model::node_id(0), 0},
         model::broker_shard{model::node_id(1), 1},
         model::broker_shard{model::node_id(2), 2}},
      },
      cluster::partition_assignment{
        raft::group_id(11),
        model::partition_id(1),
        {model::broker_shard{model::node_id(0), 0},
         model::broker_shard{model::node_id(1), 1},
         model::broker_shard{model::node_id(2), 2}},
      },
      cluster::partition_assignment{
        raft::group_id(12),
        model::partition_id(2),
        {model::broker_shard{model::node_id(0), 0},
         model::broker_shard{model::node_id(1), 1},
         model::broker_shard{model::node_id(2), 2}},
      }};
    cluster::create_partitions_configuration_assignment pca(
      std::move(cfg), std::move(p_as));

    auto res_1 = table.local()
                   .apply(
                     cluster::create_partition_cmd(
                       make_tp_ns("test_tp_2"), std::move(pca)),
                     model::offset(0))
                   .get0();

    auto md = table.local().get_topic_metadata(make_tp_ns("test_tp_2"));

    BOOST_REQUIRE_EQUAL(md->get_assignments().size(), 15);
    // check delta
    auto d = table.local().wait_for_changes(as).get0();
    // require 3 partition additions
    validate_delta(d, 3, 0);
}

void validate_brokers_revisions(
  const model::ntp& ntp,
  const cluster::topic_table::underlying_t& all_metadata,
  const absl::flat_hash_map<model::node_id, model::revision_id>&
    expected_revisions) {
    // first check if initial revisions of brokers are valid

    auto tp_it = all_metadata.find(model::topic_namespace_view(ntp));
    BOOST_REQUIRE(tp_it != all_metadata.end());

    auto p_it = tp_it->second.metadata.get_assignments().find(ntp.tp.partition);
    BOOST_REQUIRE(p_it != tp_it->second.metadata.get_assignments().end());

    auto rev_it = tp_it->second.replica_revisions.find(ntp.tp.partition);
    BOOST_REQUIRE(rev_it != tp_it->second.replica_revisions.end());

    for (auto& bs : p_it->replicas) {
        fmt::print("replica: {}\n", bs);
    }
    for (auto& bs : expected_revisions) {
        fmt::print("expected_rev: {} = {}\n", bs.first, bs.second);
    }

    for (auto& bs : rev_it->second) {
        fmt::print("current_rev: {} = {}\n", bs.first, bs.second);
    }
    BOOST_REQUIRE_EQUAL(expected_revisions.size(), rev_it->second.size());
    for (auto& bs : p_it->replicas) {
        auto r = rev_it->second.find(bs.node_id);
        auto ex = expected_revisions.find(bs.node_id);

        BOOST_REQUIRE(r != rev_it->second.end());
        BOOST_REQUIRE(ex != expected_revisions.end());

        fmt::print("Checking {} == {}\n", r->second, ex->second);
        BOOST_REQUIRE_EQUAL(
          rev_it->second.find(bs.node_id)->second,
          expected_revisions.find(bs.node_id)->second);
    }
}

template<typename Cmd, typename Key, typename Val>
void apply_cmd(
  cluster::topic_table& table, Key k, Val v, model::offset revision) {
    auto ec = table.apply(Cmd(std::move(k), std::move(v)), revision).get();

    BOOST_REQUIRE_EQUAL(ec, cluster::errc::success);
}

FIXTURE_TEST(test_tracking_broker_revisions, topic_table_fixture) {
    auto& topics = table.local();
    static const model::node_id n_0(0);
    static const model::node_id n_1(1);
    static const model::node_id n_2(2);
    static const model::node_id n_3(3);
    static const model::node_id n_4(4);

    using rev_map_t = absl::flat_hash_map<model::node_id, model::revision_id>;

    model::topic_namespace tp_ns(
      model::kafka_namespace, model::topic("test_tp"));

    model::ntp ntp_0(tp_ns.ns, tp_ns.tp, model::partition_id(0));

    cluster::topic_configuration_assignment cfg(
      cluster::topic_configuration(tp_ns.ns, tp_ns.tp, 1, 3), {});

    std::vector<model::broker_shard> replicas;
    replicas.reserve(3);
    for (auto n = 0; n < 3; ++n) {
        replicas.push_back(
          model::broker_shard{.node_id = model::node_id(n), .shard = 0});
    }

    cfg.assignments.emplace_back(
      raft::group_id(0), ntp_0.tp.partition, std::move(replicas));

    apply_cmd<cluster::create_topic_cmd>(
      topics, tp_ns, std::move(cfg), model::offset(10));

    auto& topic_metadata = table.local().all_topics_metadata();

    // first check if initial revisions of brokers are valid
    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_1, model::revision_id(10)},
        {n_2, model::revision_id(10)}});

    // move one of the topic partitions to new replica set
    apply_cmd<cluster::move_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_1, 0},
        model::broker_shard{n_3, 0}, // new broker
      },
      model::offset(11));

    // validate that new broker was added with updated revision
    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_1, model::revision_id(10)},
        {n_3, model::revision_id(11)}});

    apply_cmd<cluster::finish_moving_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_1, 0},
        model::broker_shard{n_3, 0}, // new broker
      },
      model::offset(12));

    apply_cmd<cluster::move_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_4, 0}, // new broker
        model::broker_shard{n_3, 0},
      },
      model::offset(13));

    // finish before validating
    apply_cmd<cluster::finish_moving_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_4, 0}, // new broker
        model::broker_shard{n_3, 0},
      },
      model::offset(14));

    // validate that new broker was added with updated revision
    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_4, model::revision_id(13)},
        {n_3, model::revision_id(11)}});

    // x-core move should not update revision
    apply_cmd<cluster::move_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_4, 0},
        model::broker_shard{n_3, 1}, // updated shard
      },
      model::offset(15));

    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_4, model::revision_id(13)},
        {n_3, model::revision_id(11)}});

    // finish before validating
    apply_cmd<cluster::finish_moving_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_4, 0},
        model::broker_shard{n_3, 1}, // updated shard
      },
      model::offset(16));

    apply_cmd<cluster::move_partition_replicas_cmd>(
      topics,
      ntp_0,
      std::vector<model::broker_shard>{
        model::broker_shard{n_0, 0},
        model::broker_shard{n_1, 0}, // new broker
        model::broker_shard{n_3, 2}, // shard updated
      },
      model::offset(17));

    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_1, model::revision_id(17)},
        {n_3, model::revision_id(11)}});

    // cancel should revert replica revisions to previous state
    apply_cmd<cluster::cancel_moving_partition_replicas_cmd>(
      topics,
      ntp_0,
      cluster::cancel_moving_partition_replicas_cmd_data(
        cluster::force_abort_update::no),
      model::offset(18));
    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_4, model::revision_id(13)},
        {n_3, model::revision_id(11)}});

    // force cancel should not change the revision tracking
    apply_cmd<cluster::cancel_moving_partition_replicas_cmd>(
      topics,
      ntp_0,
      cluster::cancel_moving_partition_replicas_cmd_data(
        cluster::force_abort_update::yes),
      model::offset(19));

    validate_brokers_revisions(
      ntp_0,
      topic_metadata,
      rev_map_t{
        {n_0, model::revision_id(10)},
        {n_4, model::revision_id(13)},
        {n_3, model::revision_id(11)}});
}
