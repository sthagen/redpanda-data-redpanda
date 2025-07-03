// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/level_zero/ctp_stm.h"
#include "cloud_topics/level_zero/ctp_stm_api.h"
#include "cloud_topics/level_zero/ctp_stm_factory.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "cluster/state_machine_registry.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/test.h"

namespace ct = experimental::cloud_topics;

class ctp_stm_fixture : public raft::raft_fixture {
public:
    static constexpr auto node_count = 3;

    ~ctp_stm_fixture() override {
        for (auto& entry : api_by_vnode) {
            entry.second->stop().get();
        }
    };

    ss::future<> start() {
        for (auto i = 0; i < node_count; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }

        for (auto& [id, node] : nodes()) {
            co_await node->initialise(all_vnodes());

            raft::state_machine_manager_builder builder;

            experimental::cloud_topics::ctp_stm_factory stm_factory;
            stm_factory.create(
              builder, &*node->raft(), cluster::stm_instance_config{nullptr});

            vlog(ct::cd_log.info, "Starting node {}", id);

            co_await node->start(std::move(builder));

            stm_by_vnode[node->get_vnode()]
              = node->raft()->stm_manager()->get<ct::ctp_stm>();

            api_by_vnode.emplace(
              node->get_vnode(),
              ss::make_shared<ct::ctp_stm_api>(
                ct::cd_log, node->raft()->stm_manager()->get<ct::ctp_stm>()));
        }
    }

    ct::ctp_stm_api& api(raft::raft_node_instance& node) {
        return *api_by_vnode[node.get_vnode()];
    }

    absl::flat_hash_map<raft::vnode, ss::shared_ptr<ct::ctp_stm>> stm_by_vnode;
    absl::flat_hash_map<raft::vnode, ss::shared_ptr<ct::ctp_stm_api>>
      api_by_vnode;
};

TEST_F_CORO(ctp_stm_fixture, test_basic) {
    co_await start();

    co_await wait_for_leader(raft::default_timeout());

    auto snapshot_res = co_await api(node(*get_leader())).start_snapshot();
    ASSERT_FALSE_CORO(snapshot_res.has_error());

    ASSERT_TRUE_CORO(
      api(node(*get_leader())).read_snapshot(snapshot_res.value()).has_value());

    auto snapshot_res2 = co_await api(node(*get_leader())).start_snapshot();

    ASSERT_TRUE_CORO(api(node(*get_leader()))
                       .read_snapshot(snapshot_res2.value())
                       .has_value());

    auto remove_res = co_await api(node(*get_leader()))
                        .remove_snapshots_before(snapshot_res2.value().version);
    ASSERT_FALSE_CORO(remove_res.has_error());

    ASSERT_FALSE_CORO(
      api(node(*get_leader())).read_snapshot(snapshot_res.value()).has_value());

    ASSERT_TRUE_CORO(api(node(*get_leader()))
                       .read_snapshot(snapshot_res2.value())
                       .has_value());
}
