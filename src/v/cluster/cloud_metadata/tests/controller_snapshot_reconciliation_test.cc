/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_label.h"
#include "cluster/cloud_metadata/tests/cluster_metadata_utils.h"
#include "cluster/cluster_recovery_reconciler.h"
#include "cluster/controller_snapshot.h"
#include "cluster/feature_manager.h"
#include "cluster/security_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "security/scram_credential.h"
#include "security/tests/license_utils.h"
#include "security/types.h"
#include "test_utils/async.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace cluster::cloud_metadata;

namespace {
ss::logger logger("reonciliation_test");
} // anonymous namespace

class controller_snapshot_reconciliation_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture
  , public ::testing::Test {
public:
    controller_snapshot_reconciliation_fixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{},
          httpd_port_number())
      , raft0(app.partition_manager.local().get(model::controller_ntp)->raft())
      , controller_stm(app.controller->get_controller_stm().local())
      , remote(app.cloud_storage_api.local())
      , bucket(cloud_storage_clients::bucket_name("test-bucket"))
      , reconciler(
          app.controller->get_cluster_recovery_table().local(),
          app.feature_table.local(),
          app.controller->get_credential_store().local(),
          app.controller->get_role_store().local(),
          app.controller->get_topics_state().local()) {}

    void SetUp() override {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
        RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
            return app.storage.local().get_cluster_uuid().has_value();
        });
        cluster_uuid = app.storage.local().get_cluster_uuid().value();
    }

protected:
    cluster::consensus_ptr raft0;
    cluster::controller_stm& controller_stm;
    cloud_storage::remote& remote;
    const cloud_storage_clients::bucket_name bucket;
    model::cluster_uuid cluster_uuid;

    controller_snapshot_reconciler reconciler;
};

namespace {

bool actions_contain(
  const controller_snapshot_reconciler::controller_actions& actions,
  cluster::recovery_stage stage) {
    for (const auto& s : actions.stages) {
        if (s == stage) {
            return true;
        }
    }
    return false;
}

void validate_actions(
  const controller_snapshot_reconciler::controller_actions& actions) {
    // Each recovery stage should come with a corresponding metadata with which
    // to restore.
    ASSERT_EQ(
      actions.license.has_value(),
      actions_contain(actions, cluster::recovery_stage::recovered_license));

    ASSERT_EQ(
      !actions.config.upsert.empty(),
      actions_contain(
        actions, cluster::recovery_stage::recovered_cluster_config));

    ASSERT_EQ(
      !actions.users.empty(),
      actions_contain(actions, cluster::recovery_stage::recovered_users));

    ASSERT_EQ(
      !actions.acls.empty() || !actions.roles.empty(),
      actions_contain(actions, cluster::recovery_stage::recovered_acls));

    ASSERT_EQ(
      !actions.remote_topics.empty(),
      actions_contain(
        actions, cluster::recovery_stage::recovered_remote_topic_data));

    ASSERT_EQ(
      !actions.local_topics.empty(),
      actions_contain(actions, cluster::recovery_stage::recovered_topic_data));

    ASSERT_EQ(
      actions.ct_metastore_topic.has_value(),
      actions_contain(
        actions, cluster::recovery_stage::recovered_cloud_topics_metastore));

    ASSERT_EQ(
      !actions.cloud_topics.empty(),
      actions_contain(
        actions, cluster::recovery_stage::recovered_cloud_topic_data));
}

} // anonymous namespace

TEST_F(controller_snapshot_reconciliation_fixture, test_reocnciler_license) {
    auto opt_license = security::testing::get_test_license();
    if (!opt_license.has_value()) {
        GTEST_SKIP() << security::testing::skip_no_license_msg;
        return;
    }
    cluster::controller_snapshot snap;
    snap.features.snap.license = opt_license;

    auto actions = reconciler.get_actions(snap);
    ASSERT_TRUE(
      actions_contain(actions, cluster::recovery_stage::recovered_license));
    validate_actions(actions);

    // Once we have a license, we shouldn't need to action it anymore.
    auto err = app.controller->get_feature_manager()
                 .local()
                 .update_license(std::move(*opt_license))
                 .get();
    ASSERT_TRUE(!err);
    actions = reconciler.get_actions(snap);
    ASSERT_TRUE(
      !actions_contain(actions, cluster::recovery_stage::recovered_license));
    validate_actions(actions);
}

TEST(cluster_recovery_reconciler_test, test_config_ignore_list) {
    const chunked_hash_set<ss::sstring> expected_ignore_list = {
      "cloud_storage_cache_size",
      "cluster_id",
      "cloud_storage_access_key",
      "cloud_storage_secret_key",
      "cloud_storage_region",
      "cloud_storage_bucket",
      "cloud_storage_api_endpoint",
      "cloud_storage_credentials_source",
      "cloud_storage_trust_file",
      "cloud_storage_backend",
      "cloud_storage_credentials_host",
      "cloud_storage_azure_storage_account",
      "cloud_storage_azure_container",
      "cloud_storage_azure_shared_key",
      "cloud_storage_azure_adls_endpoint",
      "cloud_storage_azure_adls_port",
      "cloud_storage_cluster_name",
    };
    auto ignore_list = controller_snapshot_reconciler::properties_ignore_list();
    ASSERT_EQ(ignore_list, expected_ignore_list);
}

TEST_F(
  controller_snapshot_reconciliation_fixture, test_reconcile_cluster_config) {
    auto d = ss::defer(
      [] { config::shard_local_cfg().for_each([](auto& p) { p.reset(); }); });
    cluster::controller_snapshot snap;

    // First set a config that's on the blacklist, and a config that doesn't
    // exist. These will be ignored.
    snap.config.values.emplace("cluster_id", "foo");
    snap.config.values.emplace("whats_this", "magic_in_the_air");
    auto actions = reconciler.get_actions(snap);
    ASSERT_TRUE(actions.empty());

    // Pick some arbitrary property to change. It should result in some action.
    snap.config.values.emplace("log_segment_size_jitter_percent", "1");
    actions = reconciler.get_actions(snap);
    ASSERT_TRUE(actions_contain(
      actions, cluster::recovery_stage::recovered_cluster_config));
    validate_actions(actions);

    // Even if the value is set, it results in an action.
    config::shard_local_cfg().log_segment_size_jitter_percent.set_value(1);
    ASSERT_TRUE(actions_contain(
      actions, cluster::recovery_stage::recovered_cluster_config));
    validate_actions(actions);
}

TEST_F(controller_snapshot_reconciliation_fixture, test_reconcile_users) {
    cluster::controller_snapshot snap;
    auto& security_snap = snap.security;
    security_snap.user_credentials.emplace_back(
      cluster::user_and_credential{
        security::credential_user{"userguy"}, security::scram_credential{}});

    auto actions = reconciler.get_actions(snap);
    ASSERT_TRUE(
      actions_contain(actions, cluster::recovery_stage::recovered_users));
    validate_actions(actions);

    app.controller->get_security_frontend()
      .local()
      .create_user(
        security::credential_user{"userguy"},
        {},
        model::timeout_clock::now() + 30s)
      .get();

    actions = reconciler.get_actions(snap);
    ASSERT_TRUE(actions.empty());
}

TEST_F(controller_snapshot_reconciliation_fixture, test_reconciler_acls) {
    cluster::controller_snapshot snap;
    auto binding = binding_for_user("__pandaproxy");
    snap.security.acls.emplace_back(binding);

    // When the snapshot contains some ACLs, we should see actions.
    auto actions = reconciler.get_actions(snap);
    ASSERT_TRUE(
      actions_contain(actions, cluster::recovery_stage::recovered_acls));
    validate_actions(actions);

    // The current implementation doesn't dedupe.
    app.controller->get_security_frontend()
      .local()
      .create_acls({binding}, 5s)
      .get();
    actions = reconciler.get_actions(snap);
    ASSERT_TRUE(
      actions_contain(actions, cluster::recovery_stage::recovered_acls));
    validate_actions(actions);
}

TEST_F(controller_snapshot_reconciliation_fixture, test_reconciler_roles) {
    cluster::controller_snapshot snap;
    auto& security_snap = snap.security;
    security_snap.roles.emplace_back(
      security::role_name("role_name"),
      security::role({{security::role_member_type::user, "test_user"}}));

    auto actions = reconciler.get_actions(snap);
    ASSERT_TRUE(
      actions_contain(actions, cluster::recovery_stage::recovered_acls));
    validate_actions(actions);
}

TEST_F(
  controller_snapshot_reconciliation_fixture,
  test_reconciler_roles_with_group_members) {
    // Test that roles with Group members are properly recovered.
    cluster::controller_snapshot snap;
    auto& security_snap = snap.security;
    security_snap.roles.emplace_back(
      security::role_name("role_with_groups"),
      security::role({{security::role_member_type::group, "admin_group"}}));

    auto actions = reconciler.get_actions(snap);
    ASSERT_TRUE(
      actions_contain(actions, cluster::recovery_stage::recovered_acls));
    validate_actions(actions);

    // Verify the role with group member is in the actions.
    ASSERT_EQ(actions.roles.size(), 1);
    ASSERT_EQ(actions.roles[0].name, security::role_name("role_with_groups"));
    ASSERT_EQ(actions.roles[0].role.members().size(), 1);
    ASSERT_TRUE(actions.roles[0].role.members().contains(
      security::role_member{security::role_member_type::group, "admin_group"}));
}

TEST_F(
  controller_snapshot_reconciliation_fixture,
  test_reconciler_roles_with_mixed_members) {
    // Test that roles with both user and group members are recovered.
    cluster::controller_snapshot snap;
    auto& security_snap = snap.security;

    // Create a role with both user and group members
    security::role mixed_role{{
      {security::role_member_type::user, "test_user"},
      {security::role_member_type::group, "test_group"},
      {security::role_member_type::group, "admin_group"},
    }};
    security_snap.roles.emplace_back(
      security::role_name("mixed_member_role"), std::move(mixed_role));

    auto actions = reconciler.get_actions(snap);
    ASSERT_TRUE(
      actions_contain(actions, cluster::recovery_stage::recovered_acls));
    validate_actions(actions);

    // Verify the role with mixed members is in the actions.
    ASSERT_EQ(actions.roles.size(), 1);
    ASSERT_EQ(actions.roles[0].name, security::role_name("mixed_member_role"));

    const auto& members = actions.roles[0].role.members();
    ASSERT_EQ(members.size(), 3);

    // Verify user member
    ASSERT_TRUE(members.contains(
      security::role_member{security::role_member_type::user, "test_user"}))
      << "User member not found in role";

    // Verify group members
    ASSERT_TRUE(members.contains(
      security::role_member{security::role_member_type::group, "test_group"}))
      << "test_group member not found in role";
    ASSERT_TRUE(members.contains(
      security::role_member{security::role_member_type::group, "admin_group"}))
      << "admin_group member not found in role";
}

TEST_F(
  controller_snapshot_reconciliation_fixture, test_reconcile_remote_topics) {
    cluster::controller_snapshot snap;
    model::topic_namespace tp_ns{model::kafka_namespace, model::topic{"foo"}};
    auto& tps = snap.topics.topics[tp_ns];
    tps.metadata.configuration.properties = uploadable_topic_properties();

    auto actions = reconciler.get_actions(snap);
    ASSERT_TRUE(actions_contain(
      actions, cluster::recovery_stage::recovered_remote_topic_data));
    ASSERT_TRUE(
      !actions_contain(actions, cluster::recovery_stage::recovered_topic_data));
    validate_actions(actions);

    // Topic creation is deduped, even if the exact properties don't match.
    add_topic(tp_ns, 1, uploadable_topic_properties()).get();
    actions = reconciler.get_actions(snap);
    ASSERT_TRUE(actions.empty());
}

TEST_F(
  controller_snapshot_reconciliation_fixture,
  test_reconcile_non_remote_topics) {
    cluster::controller_snapshot snap;
    model::topic_namespace tp_ns{model::kafka_namespace, model::topic{"foo"}};
    auto& tps = snap.topics.topics[tp_ns];
    tps.metadata.configuration.properties = non_remote_topic_properties();

    auto actions = reconciler.get_actions(snap);
    ASSERT_TRUE(!actions_contain(
      actions, cluster::recovery_stage::recovered_remote_topic_data));
    ASSERT_TRUE(
      actions_contain(actions, cluster::recovery_stage::recovered_topic_data));
    validate_actions(actions);

    // Topic creation is deduped, even if the exact properties don't match.
    add_topic(tp_ns, 1, non_remote_topic_properties()).get();
    actions = reconciler.get_actions(snap);
    ASSERT_TRUE(actions.empty());
}

TEST_F(controller_snapshot_reconciliation_fixture, test_reconciler_group_acls) {
    cluster::controller_snapshot snap;
    auto binding = binding_for_group("test_group");
    snap.security.acls.emplace_back(binding);

    // When the snapshot contains Group ACLs, we should see actions.
    auto actions = reconciler.get_actions(snap);
    ASSERT_TRUE(
      actions_contain(actions, cluster::recovery_stage::recovered_acls));
    validate_actions(actions);

    // Verify the Group ACL is included in the actions.
    ASSERT_EQ(actions.acls.size(), 1);
    ASSERT_EQ(
      actions.acls[0].entry().principal().type(),
      security::principal_type::group);
    ASSERT_EQ(actions.acls[0].entry().principal().name_view(), "test_group");

    // The current implementation doesn't dedupe.
    app.controller->get_security_frontend()
      .local()
      .create_acls({binding}, 5s)
      .get();
    actions = reconciler.get_actions(snap);
    ASSERT_TRUE(
      actions_contain(actions, cluster::recovery_stage::recovered_acls));
    validate_actions(actions);
}

TEST_F(
  controller_snapshot_reconciliation_fixture,
  test_reconciler_mixed_principal_acls) {
    // Test that recovery handles a mix of user, role, and group ACLs.
    cluster::controller_snapshot snap;

    auto user_binding = binding_for_user("test_user");
    auto role_binding = binding_for_role("test_role");
    auto group_binding = binding_for_group("test_group");

    snap.security.acls.emplace_back(user_binding);
    snap.security.acls.emplace_back(role_binding);
    snap.security.acls.emplace_back(group_binding);

    auto actions = reconciler.get_actions(snap);
    ASSERT_TRUE(
      actions_contain(actions, cluster::recovery_stage::recovered_acls));
    validate_actions(actions);

    // Verify all three ACL types are included in the actions.
    ASSERT_EQ(actions.acls.size(), 3);

    bool found_user = false, found_role = false, found_group = false;
    for (const auto& acl : actions.acls) {
        switch (acl.entry().principal().type()) {
        case security::principal_type::ephemeral_user:
            found_user = true;
            ASSERT_EQ(acl.entry().principal().name_view(), "test_user");
            break;
        case security::principal_type::role:
            found_role = true;
            ASSERT_EQ(acl.entry().principal().name_view(), "test_role");
            break;
        case security::principal_type::group:
            found_group = true;
            ASSERT_EQ(acl.entry().principal().name_view(), "test_group");
            break;
        default:
            break;
        }
    }

    ASSERT_TRUE(found_user) << "User ACL not found in actions";
    ASSERT_TRUE(found_role) << "Role ACL not found in actions";
    ASSERT_TRUE(found_group) << "Group ACL not found in actions";
}

TEST_F(
  controller_snapshot_reconciliation_fixture, test_reconcile_metastore_topic) {
    auto snapshot_label = cloud_storage::remote_label(
      model::cluster_uuid{uuid_t::create()});

    // Helper to check metastore topic reconciliation behavior.
    const auto check_metastore_action =
      [&](const cloud_storage::remote_label& label, bool expect_action) {
          cluster::controller_snapshot snap;
          auto& tps = snap.topics.topics[model::l1_metastore_nt];
          tps.metadata.configuration.tp_ns = model::l1_metastore_nt;
          tps.metadata.configuration.properties.remote_label = label;

          auto actions = reconciler.get_actions(snap);
          ASSERT_EQ(
            actions_contain(
              actions,
              cluster::recovery_stage::recovered_cloud_topics_metastore),
            expect_action);
          validate_actions(actions);

          ASSERT_EQ(actions.ct_metastore_topic.has_value(), expect_action);
          if (expect_action) {
              ASSERT_EQ(
                actions.ct_metastore_topic->properties.remote_label, label);
          }
      };

    // Case 1: Topic doesn't exist - should create with snapshot's label.
    check_metastore_action(snapshot_label, true);

    // Case 2: Topic exists with different label - should update to snapshot's
    // label.
    auto different_label = cloud_storage::remote_label(
      model::cluster_uuid{uuid_t::create()});
    cluster::topic_properties props;
    props.remote_label = different_label;
    add_topic(model::l1_metastore_nt, 1, props).get();

    check_metastore_action(snapshot_label, true);

    // Case 3: Topic exists with same label - no action needed.
    check_metastore_action(different_label, false);
}

TEST_F(
  controller_snapshot_reconciliation_fixture, test_reconcile_cloud_topics) {
    // Helper to check cloud topic reconciliation behavior.
    const auto check_cloud_topic_action =
      [&](
        const model::topic_namespace& tp_ns,
        const cluster::topic_properties& props,
        bool expect_action) {
          cluster::controller_snapshot snap;
          auto& tps = snap.topics.topics[tp_ns];
          tps.metadata.configuration.tp_ns = tp_ns;
          tps.metadata.configuration.properties = props;
          tps.metadata.revision = model::revision_id{42};

          auto actions = reconciler.get_actions(snap);
          ASSERT_EQ(
            actions_contain(
              actions, cluster::recovery_stage::recovered_cloud_topic_data),
            expect_action);
          // Cloud topics should NOT trigger remote_topic_data or topic_data
          // stages.
          ASSERT_FALSE(actions_contain(
            actions, cluster::recovery_stage::recovered_remote_topic_data));
          ASSERT_FALSE(actions_contain(
            actions, cluster::recovery_stage::recovered_topic_data));
          validate_actions(actions);

          if (expect_action) {
              ASSERT_EQ(actions.cloud_topics.size(), 1);
              ASSERT_EQ(actions.cloud_topics[0].tp_ns, tp_ns);
              // Verify that remote_topic_properties is set with the correct
              // revision for cloud topics.
              ASSERT_TRUE(actions.cloud_topics[0]
                            .properties.remote_topic_properties.has_value());
              ASSERT_EQ(
                actions.cloud_topics[0]
                  .properties.remote_topic_properties->remote_revision,
                model::initial_revision_id{42});
          } else {
              ASSERT_TRUE(actions.cloud_topics.empty());
          }
      };

    model::topic_namespace tp_ns{model::kafka_namespace, model::topic{"foo"}};

    // Case 1: Cloud topic doesn't exist - should create and set
    // remote_topic_properties.
    check_cloud_topic_action(tp_ns, cloud_topic_properties(), true);

    // Case 2: Read-replica cloud topic - should create and set
    // remote_topic_properties.
    model::topic_namespace rr_tp_ns{
      model::kafka_namespace, model::topic{"read_replica"}};
    check_cloud_topic_action(
      rr_tp_ns, read_replica_cloud_topic_properties(), true);

    // Case 3: Topic already exists - no action needed.
    // Create a topic in the cluster. The reconciler only checks for topic
    // existence by name, so we use a non-cloud topic since cloud topics
    // require development feature flags.
    model::topic_namespace existing_tp_ns{
      model::kafka_namespace, model::topic{"existing"}};
    add_topic(existing_tp_ns, 1, non_remote_topic_properties()).get();
    check_cloud_topic_action(existing_tp_ns, cloud_topic_properties(), false);
}

TEST_F(
  controller_snapshot_reconciliation_fixture,
  test_reconcile_mixed_topic_types) {
    using cluster::recovery_stage;
    cluster::controller_snapshot snap;

    // Metastore topic.
    auto metastore_label = cloud_storage::remote_label(
      model::cluster_uuid{uuid_t::create()});
    auto& metastore_tps = snap.topics.topics[model::l1_metastore_nt];
    metastore_tps.metadata.configuration.tp_ns = model::l1_metastore_nt;
    metastore_tps.metadata.configuration.properties.remote_label
      = metastore_label;

    // Cloud topic.
    model::topic_namespace cloud_tp_ns{
      model::kafka_namespace, model::topic{"cloud_topic"}};
    auto& cloud_tps = snap.topics.topics[cloud_tp_ns];
    cloud_tps.metadata.configuration.tp_ns = cloud_tp_ns;
    cloud_tps.metadata.configuration.properties = cloud_topic_properties();

    // Remote topic (tiered storage).
    model::topic_namespace remote_tp_ns{
      model::kafka_namespace, model::topic{"remote_topic"}};
    auto& remote_tps = snap.topics.topics[remote_tp_ns];
    remote_tps.metadata.configuration.tp_ns = remote_tp_ns;
    remote_tps.metadata.configuration.properties
      = uploadable_topic_properties();
    remote_tps.metadata.revision = model::revision_id{1};

    // Local topic.
    model::topic_namespace local_tp_ns{
      model::kafka_namespace, model::topic{"local_topic"}};
    auto& local_tps = snap.topics.topics[local_tp_ns];
    local_tps.metadata.configuration.tp_ns = local_tp_ns;
    local_tps.metadata.configuration.properties = non_remote_topic_properties();

    auto actions = reconciler.get_actions(snap);
    validate_actions(actions);

    // Validate the stages are present in the expected order.
    EXPECT_THAT(
      actions.stages,
      testing::ElementsAre(
        recovery_stage::recovered_cloud_topics_metastore,
        recovery_stage::recovered_cloud_topic_data,
        recovery_stage::recovered_remote_topic_data,
        recovery_stage::recovered_topic_data,
        recovery_stage::recovered_controller_snapshot));

    ASSERT_TRUE(actions.ct_metastore_topic.has_value());
    ASSERT_EQ(
      actions.ct_metastore_topic->properties.remote_label, metastore_label);
    ASSERT_EQ(actions.cloud_topics.size(), 1);
    ASSERT_EQ(actions.remote_topics.size(), 1);
    ASSERT_EQ(actions.local_topics.size(), 1);
}
