/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/notifier/level_zero_notifier.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/logger.h"
#include "model/fundamental.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/test.h"

#include <chrono>

namespace ct = cloud_topics;
using namespace std::chrono_literals;

namespace {
constexpr auto tiny_backoff = std::chrono::milliseconds(1);
const model::ntp
  test_ntp(model::ns("kafka"), model::topic("tp"), model::partition_id(0));
} // namespace

class level_zero_notifier_fixture : public raft::stm_raft_fixture<ct::ctp_stm> {
public:
    ss::future<> start() {
        enable_offset_translation();
        co_await initialize_state_machines();
    }

    stm_shptrs_t create_stms(
      raft::state_machine_manager_builder& builder,
      raft::raft_node_instance& node) override {
        return builder.create_stm<ct::ctp_stm>(ct::cd_log, node.raft().get());
    }

    ct::ctp_stm_api api(raft::raft_node_instance& node) {
        return ct::ctp_stm_api(get_stm<0>(node));
    }
};

// Replicating to the leader succeeds on the first attempt.
TEST_F_CORO(level_zero_notifier_fixture, replicate_to_leader_succeeds) {
    co_await start();
    co_await wait_for_leader(raft::default_timeout());

    ct::level_zero_notifier notifier(nullptr, nullptr, tiny_backoff);
    auto leader_api = api(node(*get_leader()));
    auto res = co_await notifier.replicate_with_retries(
      test_ntp, leader_api, kafka::offset(42));
    co_await notifier.stop();

    ASSERT_TRUE_CORO(res.has_value());
}

// Replicating to a follower hits not_leader, which the notifier treats as
// non-retriable: it gives up immediately and returns the error to the caller
// (not fire-and-forget).
TEST_F_CORO(level_zero_notifier_fixture, gives_up_on_follower) {
    co_await start();
    co_await wait_for_leader(raft::default_timeout());

    auto leader = *get_leader();
    raft::raft_node_instance* follower = nullptr;
    for (auto& [id, n] : nodes()) {
        if (id != leader) {
            follower = n.get();
            break;
        }
    }
    ASSERT_TRUE_CORO(follower != nullptr);

    ct::level_zero_notifier notifier(nullptr, nullptr, tiny_backoff);
    auto follower_api = api(*follower);
    auto res = co_await notifier.replicate_with_retries(
      test_ntp, follower_api, kafka::offset(7));
    co_await notifier.stop();

    ASSERT_FALSE_CORO(res.has_value());
}
