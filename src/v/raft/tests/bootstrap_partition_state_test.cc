// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "raft/consensus_utils.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/segment_utils.h"
#include "storage/snapshot.h"
#include "test_utils/test.h"
#include "test_utils/test_env.h"

#include <seastar/core/seastar.hh>

using namespace std::chrono_literals;

/// Fixture for testing bootstrap_partition_state functionality
struct bootstrap_partition_state_test : public seastar_test {
    ss::future<> SetUpAsync() override {
        _data_dir = test_env::random_dir_path();

        // Configure and start kvstore
        storage::kvstore_config kv_conf(
          8192,
          config::mock_binding(std::chrono::milliseconds(10)),
          _data_dir,
          storage::make_sanitized_file_config());

        co_await _storage.start(
          [kv_conf]() { return kv_conf; },
          [this]() { return default_log_cfg(); },
          std::ref(_feature_table));
        co_await _storage.invoke_on_all(&storage::api::start);

        co_await _feature_table.start();
        co_await _feature_table.invoke_on_all(
          [](features::feature_table& f) { f.testing_activate_all(); });
    }

    ss::future<> TearDownAsync() override {
        co_await _storage.stop();
        co_await _feature_table.stop();
    }

    storage::log_config default_log_cfg() {
        return storage::log_config(
          _data_dir,
          100_MiB,
          storage::with_cache::yes,
          storage::make_sanitized_file_config());
    }

    ss::sstring _data_dir;
    ss::sharded<storage::api> _storage;
    ss::sharded<features::feature_table> _feature_table;
};

TEST_F(bootstrap_partition_state_test, test_bootstrap_partition_state) {
    // Define test parameters
    model::ntp ntp(
      model::ns("test-ns"), model::topic("test-topic"), model::partition_id(0));

    storage::ntp_config ntp_cfg(ntp, _data_dir);

    raft::group_id group(1);
    model::offset start_offset(1000);
    model::term_id initial_term(5);

    std::vector<raft::vnode> initial_nodes{
      raft::vnode{model::node_id(0), model::revision_id(0)}};

    // Create the work directory
    ss::recursive_touch_directory(ntp_cfg.work_directory()).get();

    // Call bootstrap_partition_state
    raft::details::bootstrap_partition_state(
      _storage.local(),
      ntp_cfg,
      group,
      start_offset,
      initial_term,
      initial_nodes)
      .get();

    // Verify the start_offset was set in kvstore
    auto stored_offset = _storage.local().kvs().get(
      storage::kvstore::key_space::storage,
      storage::internal::start_offset_key(ntp));

    ASSERT_TRUE(stored_offset.has_value());

    auto retrieved_offset = reflection::from_iobuf<model::offset>(
      std::move(stored_offset.value()));
    EXPECT_EQ(retrieved_offset, start_offset);

    // Verify the latest_known_offset was set in kvstore
    auto key = raft::details::serialize_group_key(
      group, raft::metadata_key::config_latest_known_offset);
    auto stored_latest_offset = _storage.local().kvs().get(
      storage::kvstore::key_space::consensus, key);

    ASSERT_TRUE(stored_latest_offset.has_value());

    auto retrieved_latest_offset = reflection::from_iobuf<model::offset>(
      std::move(stored_latest_offset.value()));
    EXPECT_EQ(retrieved_latest_offset, start_offset);

    // Verify snapshot file was created
    auto snapshot_path
      = std::filesystem::path(ntp_cfg.work_directory())
        / storage::simple_snapshot_manager::default_snapshot_filename;

    EXPECT_TRUE(std::filesystem::exists(snapshot_path));
}
