/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/topic_manifest_downloader.h"
#include "cloud_topics/tests/cluster_fixture.h"
#include "cluster/types.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <gtest/gtest.h>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace {

ss::logger test_log("topic_manifest_uploader_test");
const model::topic test_topic{"tapioca"};
const model::ntp test_ntp{model::kafka_namespace, test_topic, 0};
const model::topic_namespace test_tp_ns{model::kafka_namespace, test_topic};

} // namespace

class TopicManifestUploaderTest
  : public cluster_fixture
  , public ::testing::Test {
public:
    using opt_manifest = std::optional<cloud_storage::topic_manifest>;

    void SetUp() override {
        cfg.get("enable_leader_balancer").set_value(false);
        for (int i = 0; i < 3; ++i) {
            add_node();
        }
        wait_for_all_members(5s).get();
    }

    ss::future<>
    create_cloud_topic(model::topic topic_name, int partitions = 1) {
        cluster::topic_properties props;
        props.storage_mode = model::redpanda_storage_mode::cloud;
        props.shadow_indexing = model::shadow_indexing_mode::disabled;
        co_await create_topic(
          {model::kafka_namespace, topic_name}, partitions, 3, props);
    }

    ss::future<opt_manifest>
    download_manifest(const model::topic_namespace& tp_ns) {
        auto& remote
          = instance(model::node_id{0})->app.cloud_storage_api.local();
        cloud_storage::topic_manifest_downloader downloader(
          bucket_name, /*hint=*/std::nullopt, tp_ns, remote);

        cloud_storage::topic_manifest manifest;
        ss::abort_source as;
        retry_chain_node retry(as, 30s, 100ms);
        auto res = co_await downloader.download_manifest(
          retry, ss::lowres_clock::now() + 30s, 100ms, &manifest);
        if (res.has_error()) {
            vlog(
              test_log.debug,
              "Manifest download error for {}: {}",
              tp_ns,
              res.error());
            co_return std::nullopt;
        }
        if (
          res.value() != cloud_storage::find_topic_manifest_outcome::success) {
            vlog(
              test_log.debug,
              "Manifest download outcome for {}: {}",
              tp_ns,
              int(res.value()));
            co_return std::nullopt;
        }
        co_return manifest;
    }

    ss::future<>
    wait_for_manifest(const model::topic_namespace& tp_ns, opt_manifest& out) {
        auto deadline = ss::lowres_clock::now() + 30s;
        while (ss::lowres_clock::now() < deadline) {
            auto m = co_await download_manifest(tp_ns);
            if (m.has_value()) {
                out = std::move(m);
                co_return;
            }
            co_await ss::sleep(100ms);
        }
        throw ss::timed_out_error();
    }

    ss::future<> wait_for_manifest_with_retention(
      const model::topic_namespace& tp_ns,
      size_t expected_retention,
      opt_manifest& out) {
        auto deadline = ss::lowres_clock::now() + 30s;
        while (ss::lowres_clock::now() < deadline) {
            auto m = co_await download_manifest(tp_ns);
            if (m.has_value()) {
                auto retention
                  = m->get_topic_config()->properties.retention_bytes;
                if (
                  retention.has_optional_value()
                  && retention.value() == expected_retention) {
                    out = std::move(m);
                    co_return;
                }
            }
            co_await ss::sleep(100ms);
        }
        throw ss::timed_out_error();
    }

    ss::future<> update_topic_retention_bytes(
      const model::topic_namespace& tp_ns, std::optional<size_t> retention) {
        cluster::incremental_topic_updates updates;
        updates.retention_bytes.op = cluster::incremental_update_operation::set;
        updates.retention_bytes.value = tristate<size_t>(retention);
        auto& topics_frontend = instance(model::node_id{0})
                                  ->app.controller->get_topics_frontend()
                                  .local();

        cluster::topic_properties_update update(tp_ns);
        update.properties = updates;
        cluster::topic_properties_update_vector updates_vec;
        updates_vec.push_back(std::move(update));

        auto results = co_await topics_frontend.update_topic_properties(
          std::move(updates_vec), model::no_timeout);

        EXPECT_EQ(results.size(), 1);
        EXPECT_EQ(results[0].ec, cluster::errc::success);
    }

    ss::future<> wait_for_leadership(
      const model::ntp& ntp, ss::lw_shared_ptr<cluster::partition>& out) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [&] {
            auto [leader_fx, leader_p] = get_leader(ntp);
            if (!leader_fx) {
                return false;
            }
            out = leader_p;
            return true;
        });
    }

    ss::future<> wait_for_more_requests(size_t initial_count) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(
          5s, [&] { return get_requests().size() > initial_count; });
    }

    scoped_config cfg;
};

TEST_F(TopicManifestUploaderTest, TestManifestUploadedOnTopicCreation) {
    create_cloud_topic(test_topic).get();

    opt_manifest manifest;
    ASSERT_NO_FATAL_FAILURE(wait_for_manifest(test_tp_ns, manifest).get());

    ASSERT_TRUE(manifest.has_value());
    ASSERT_TRUE(manifest->get_topic_config().has_value());
    EXPECT_EQ(manifest->get_topic_config()->tp_ns, test_tp_ns);
    EXPECT_EQ(manifest->get_topic_config()->partition_count, 1);
}

TEST_F(TopicManifestUploaderTest, TestManifestUpdatedOnPropertyChange) {
    create_cloud_topic(test_topic).get();

    opt_manifest initial_manifest;
    ASSERT_NO_FATAL_FAILURE(
      wait_for_manifest(test_tp_ns, initial_manifest).get());
    ASSERT_TRUE(initial_manifest.has_value());
    ASSERT_TRUE(initial_manifest->get_topic_config().has_value());

    constexpr size_t new_retention = 12345;
    update_topic_retention_bytes(test_tp_ns, new_retention).get();

    opt_manifest updated_manifest;
    ASSERT_NO_FATAL_FAILURE(wait_for_manifest_with_retention(
                              test_tp_ns, new_retention, updated_manifest)
                              .get());

    ASSERT_TRUE(updated_manifest.has_value());
    ASSERT_TRUE(updated_manifest->get_topic_config().has_value());
    auto updated_retention
      = updated_manifest->get_topic_config()->properties.retention_bytes;
    EXPECT_TRUE(updated_retention.has_optional_value());
    EXPECT_EQ(updated_retention.value(), new_retention);
}

TEST_F(TopicManifestUploaderTest, TestManifestUploadedOnLeadershipChange) {
    create_cloud_topic(test_topic).get();

    opt_manifest initial_manifest;
    ASSERT_NO_FATAL_FAILURE(
      wait_for_manifest(test_tp_ns, initial_manifest).get());
    ASSERT_TRUE(initial_manifest.has_value());
    ASSERT_TRUE(initial_manifest->get_topic_config().has_value());

    auto initial_requests = get_requests().size();

    shuffle_leadership(test_ntp).get();

    ss::lw_shared_ptr<cluster::partition> partition;
    ASSERT_NO_FATAL_FAILURE(wait_for_leadership(test_ntp, partition).get());
    ASSERT_TRUE(partition != nullptr);

    ASSERT_NO_FATAL_FAILURE(wait_for_more_requests(initial_requests).get());

    opt_manifest manifest_after_transfer;
    ASSERT_NO_FATAL_FAILURE(
      wait_for_manifest(test_tp_ns, manifest_after_transfer).get());
    ASSERT_TRUE(manifest_after_transfer.has_value());
    ASSERT_TRUE(manifest_after_transfer->get_topic_config().has_value());
    EXPECT_EQ(manifest_after_transfer->get_topic_config()->tp_ns, test_tp_ns);
}

TEST_F(TopicManifestUploaderTest, TestNoUploadOnNonZeroPartition) {
    create_cloud_topic(test_topic, 2).get();

    // Wait for initial manifest upload (triggered by partition 0 leader)
    opt_manifest initial_manifest;
    ASSERT_NO_FATAL_FAILURE(
      wait_for_manifest(test_tp_ns, initial_manifest).get());
    ASSERT_TRUE(initial_manifest.has_value());

    // Wait for partition 1 to have a leader
    model::ntp ntp1{model::kafka_namespace, test_topic, 1};
    ss::lw_shared_ptr<cluster::partition> p1;
    ASSERT_NO_FATAL_FAILURE(wait_for_leadership(ntp1, p1).get());

    // Transfer leadership on partition 1 and make sure that there are no new
    // topic manifest uploads.
    auto requests_before = get_requests().size();
    shuffle_leadership(ntp1).get();
    ASSERT_NO_FATAL_FAILURE(wait_for_leadership(ntp1, p1).get());
    ASSERT_TRUE(p1 != nullptr);

    // Give some time for any uploads to occur and validate that none did.
    ss::sleep(500ms).get();
    EXPECT_EQ(get_requests().size(), requests_before);
}
