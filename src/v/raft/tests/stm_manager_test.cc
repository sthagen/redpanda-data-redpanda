// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/tests/stm_test_fixture.h"

using namespace raft;

inline ss::logger logger("stm-test-logger");
struct other_kv : simple_kv {
    using simple_kv::simple_kv;
    static constexpr std::string_view name = "other_simple_kv";
};
struct throwing_kv : public simple_kv {
    static constexpr std::string_view name = "throwing_kv";
    explicit throwing_kv(raft_node_instance& rn)
      : simple_kv(rn) {}

    ss::future<> apply(const model::record_batch& batch) override {
        if (tests::random_bool()) {
            throw std::runtime_error("runtime error from throwing stm");
        }
        vassert(
          batch.base_offset() == next(),
          "batch {} base offset is not the next to apply, expected base "
          "offset: {}",
          batch.header(),
          next());
        co_await simple_kv::apply(batch);
        co_return;
    }

    ss::future<> apply_raft_snapshot(const iobuf& buffer) override {
        if (!_tried_applying) {
            _tried_applying = true;
            throw std::runtime_error("Error from apply_snapshot...");
        }

        return simple_kv::apply_raft_snapshot(buffer);
    };

    bool _tried_applying = false;
};
/**
 * Local snapshot stm manages its own local snapshot.
 */
struct local_snapshot_stm : public simple_kv {
    static constexpr std::string_view name = "local_snapshot_kv";
    explicit local_snapshot_stm(raft_node_instance& rn)
      : simple_kv(rn) {}

    ss::future<> apply(const model::record_batch& batch) override {
        vassert(
          batch.base_offset() == next(),
          "batch {} base offset is not the next to apply, expected base "
          "offset: {}",
          batch.header(),
          next());
        co_await simple_kv::apply(batch);
    }

    ss::future<> apply_raft_snapshot(const iobuf& buffer) override {
        vassert(
          buffer.size_bytes() == 0,
          "Only empty buffer is expected to be applied to the local snapshot "
          "managing STM.");
        // reset state
        state = {};
        co_return;
    };
};

TEST_F_CORO(state_machine_fixture, test_basic_apply) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto other_kv_stm = builder.create_stm<other_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(other_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(state_machine_fixture, test_apply_throwing_exception) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(state_machine_fixture, test_recovery_without_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
    // stop one of the nodes and remove data
    model::node_id stopped_id(1);
    co_await stop_node(stopped_id, remove_data_dir::yes);
    auto& new_node = add_node(stopped_id, model::revision_id{0});

    // start the node back up
    raft::state_machine_manager_builder builder;
    auto kv_stm = builder.create_stm<simple_kv>(new_node);
    auto throwing_kv_stm = builder.create_stm<throwing_kv>(new_node);
    co_await new_node.init_and_start(all_vnodes(), std::move(builder));
    auto committed_offset = co_await with_leader(
      10s,
      [](raft_node_instance& node) { return node.raft()->committed_offset(); });

    co_await new_node.raft()->stm_manager()->wait(
      committed_offset, model::no_timeout);

    ASSERT_EQ_CORO(kv_stm->state, expected);
    ASSERT_EQ_CORO(throwing_kv_stm->state, expected);
}

TEST_F_CORO(state_machine_fixture, test_recovery_from_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
    // take snapshot at batch boundary
    auto snapshot_offset = co_await with_leader(
      10s, [](raft_node_instance& node) {
          auto committed = node.raft()->committed_offset();
          return node.raft()
            ->make_reader(storage::log_reader_config(
              node.raft()->start_offset(),
              model::offset(random_generators::get_int(
                node.raft()->start_offset()(), committed())),
              ss::default_priority_class()))
            .then([](auto rdr) {
                return model::consume_reader_to_memory(
                  std::move(rdr), model::no_timeout);
            })
            .then([](ss::circular_buffer<model::record_batch> batches) {
                return batches.back().last_offset();
            });
      });
    // take snapshots on all of the nodes
    co_await parallel_for_each_node([snapshot_offset](raft_node_instance& n) {
        return n.raft()
          ->stm_manager()
          ->take_snapshot(snapshot_offset)
          .then([raft = n.raft(), snapshot_offset](iobuf snapshot_data) {
              return raft->write_snapshot(raft::write_snapshot_cfg(
                snapshot_offset, std::move(snapshot_data)));
          });
    });

    auto committed_offset = co_await with_leader(
      10s,
      [](raft_node_instance& node) { return node.raft()->committed_offset(); });

    auto& new_node = add_node(model::node_id(4), model::revision_id{0});
    raft::state_machine_manager_builder builder;
    auto kv_stm = builder.create_stm<simple_kv>(new_node);
    auto throwing_kv_stm = builder.create_stm<throwing_kv>(new_node);
    co_await new_node.init_and_start({}, std::move(builder));

    co_await with_leader(
      10s, [vn = new_node.get_vnode()](raft_node_instance& node) {
          return node.raft()->add_group_member(vn, model::revision_id{0});
      });

    co_await new_node.raft()->stm_manager()->wait(
      committed_offset, model::timeout_clock::now() + 20s);

    ASSERT_EQ_CORO(kv_stm->state, expected);
    ASSERT_EQ_CORO(throwing_kv_stm->state, expected);
}

TEST_F_CORO(
  state_machine_fixture, test_recovery_from_backward_compatible_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<local_snapshot_stm>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        stms.push_back(builder.create_stm<local_snapshot_stm>(*node));

        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();
    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }

    // take snapshot at batch boundary
    auto snapshot_offset = co_await with_leader(
      10s, [](raft_node_instance& node) {
          auto committed = node.raft()->committed_offset();
          return node.raft()
            ->make_reader(storage::log_reader_config(
              node.raft()->start_offset(),
              model::offset(random_generators::get_int(
                node.raft()->start_offset()(), committed())),
              ss::default_priority_class()))
            .then([](auto rdr) {
                return model::consume_reader_to_memory(
                  std::move(rdr), model::no_timeout);
            })
            .then([](ss::circular_buffer<model::record_batch> batches) {
                return batches.back().last_offset();
            });
      });

    co_await parallel_for_each_node([snapshot_offset](raft_node_instance& n) {
        // create an empty snapshot, the same way Redpanda does in previous
        // versions

        return n.raft()->write_snapshot(
          write_snapshot_cfg(snapshot_offset, iobuf{}));
    });

    auto committed_offset = co_await with_leader(
      10s,
      [](raft_node_instance& node) { return node.raft()->committed_offset(); });

    auto& new_node = add_node(model::node_id(4), model::revision_id{0});
    raft::state_machine_manager_builder builder;
    auto new_stm = builder.create_stm<local_snapshot_stm>(new_node);

    co_await new_node.init_and_start({}, std::move(builder));

    co_await with_leader(
      10s, [vn = new_node.get_vnode()](raft_node_instance& node) {
          return node.raft()->add_group_member(vn, model::revision_id{0});
      });
    // wait for the state to be applied
    co_await new_node.raft()->stm_manager()->wait(
      committed_offset, model::timeout_clock::now() + 20s);

    simple_kv::state_t partial_expected_state;

    auto rdr = co_await new_node.raft()->make_reader(storage::log_reader_config(
      model::next_offset(snapshot_offset),
      committed_offset,
      ss::default_priority_class()));

    auto batches = co_await model::consume_reader_to_memory(
      std::move(rdr), model::no_timeout);

    for (auto const& b : batches) {
        simple_kv::apply_to_state(b, partial_expected_state);
    }

    ASSERT_EQ_CORO(new_stm->state, partial_expected_state);
}
