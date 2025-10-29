# Copyright 2023 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import random
import string
import time
import pytest
from typing import Any

import requests

import ducktape
import ducktape.errors
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.services.admin import RoleMember
from rptest.clients.rpk import RPKACLInput, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.si_utils import quiesce_uploads

KiB = 1024
MiB = KiB * KiB


def random_string(length):
    return "".join([random.choice(string.ascii_lowercase) for i in range(0, length)])


class ClusterRecoveryTest(RedpandaTest):
    segment_size = 1 * MiB
    message_size = 16 * KiB
    topics = [
        TopicSpec(
            name=f"topic-{n}",
            partition_count=3,
            replication_factor=1,
            redpanda_remote_write=True,
            redpanda_remote_read=True,
            retention_bytes=-1,
            retention_ms=-1,
            segment_bytes=1 * MiB,
        )
        for n in range(3)
    ]

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context, log_segment_size=self.segment_size, fast_uploads=True
        )
        extra_rp_conf = {
            # Test takes too much time otherwise.
            "group_topic_partitions": 2,
            "controller_snapshot_max_age_sec": 1,
            "cloud_storage_cluster_metadata_upload_interval_ms": 1000,
            "enable_cluster_metadata_upload_loop": True,
        }
        si_settings = SISettings(
            test_context,
            log_segment_size=self.segment_size,
            fast_uploads=True,
        )
        self.s3_bucket = si_settings.cloud_storage_bucket
        super(ClusterRecoveryTest, self).__init__(
            test_context=test_context,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
        )

    @cluster(num_nodes=4)
    def test_basic_controller_snapshot_restore(self):
        """
        Tests that recovery of some fixed pieces of controller metadata get
        restored by cluster recovery.
        """
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("log_segment_size_max", 1000000)

        for t in self.topics:
            KgoVerifierProducer.oneshot(
                self.test_context,
                self.redpanda,
                t.name,
                self.message_size,
                100,
                batch_max_bytes=self.message_size * 8,
                timeout_sec=60,
            )
        quiesce_uploads(self.redpanda, [t.name for t in self.topics], timeout_sec=60)

        algorithm = "SCRAM-SHA-256"
        users = dict()
        users["admin"] = None  # Created by the RedpandaService.
        roles: dict[str, set[RoleMember]] = dict()
        for _ in range(3):
            user = f"user-{random_string(6)}"
            password = f"user-{random_string(6)}"
            users[user] = password
            self.redpanda._admin.create_user(user, password, algorithm)
            rpk.acl_create_allow_cluster(user, op="describe")

            role_name: str = f"role-{random_string(6)}"
            role_members = [RoleMember.User(user)]
            roles[role_name] = set(role_members)
            self.redpanda._admin.create_role(role_name)
            self.redpanda._admin.update_role_members(role_name, add=role_members)

            role_acl = RPKACLInput()
            role_acl.allow_role = [role_name]
            role_acl.cluster = True
            role_acl.operation = ["ALL"]
            rpk.acl_create(role_acl)
        rpk.acl_create_allow_cluster("admin", op="describe")

        time.sleep(5)

        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)
        self.redpanda.restart_nodes(
            self.redpanda.nodes, auto_assign_node_id=True, omit_seeds_on_idx_one=False
        )
        self.redpanda._admin.await_stable_leader(
            "controller", partition=0, namespace="redpanda", timeout_s=60, backoff_s=2
        )

        self.logger.info("Verifying that no data is present before recovery")

        assert len(rpk.list_topics()) == 0, "Expected no topics before recovery"
        assert len(self.redpanda._admin.list_users()) == 0, (
            "Expected no users before recovery"
        )
        # Expecting 1 line for the header only.
        assert len(rpk.acl_list().splitlines()) == 1, "Expected no ACLs before recovery"
        assert len(rpk.list_roles().get("roles", [])) == 0, (
            "Expected no roles before recovery"
        )

        self.logger.info("Initializing cluster recovery")

        self.redpanda._admin.initialize_cluster_recovery()

        def cluster_recovery_complete():
            return (
                "inactive"
                in self.redpanda._admin.get_cluster_recovery_status().json()["state"]
            )

        wait_until(cluster_recovery_complete, timeout_sec=30, backoff_sec=1)

        assert len(set(rpk.list_topics())) == 3, "Incorrect number of topics restored"
        segment_size_max_restored = rpk.cluster_config_get("log_segment_size_max")
        assert "1000000" == segment_size_max_restored, (
            f"1000000 vs {segment_size_max_restored}"
        )
        restored_users = self.redpanda._admin.list_users()
        assert set(restored_users) == set(users.keys()), (
            f"{restored_users} vs {users.keys()}"
        )

        acls = rpk.acl_list()
        acls_lines = acls.splitlines()
        for u in users:
            found = False
            for l in acls_lines:
                if u in l and "ALLOW" in l and "DESCRIBE" in l:
                    found = True
            assert found, f"Couldn't find {u} in {acls_lines}"

        self.logger.info("Verifying roles")

        restored_roles = rpk.list_roles().get("roles", [])
        assert set(restored_roles) == set(roles.keys()), (
            f"{restored_roles} vs {roles.keys()}"
        )

        for role_name, members in roles.items():
            res = rpk.describe_role(role_name)
            restored_role_members = set(
                RoleMember.User(member["name"])
                for member in res.get("members", [])
                if member["principal_type"] == RoleMember.PrincipalType.USER.value
            )

            assert restored_role_members == members, (
                f"Role {role_name} members mismatch: {restored_role_members} vs {members}"
            )

            found_role_acl = False
            for l in acls_lines:
                if role_name in l and "ALLOW" in l and "ALL" in l:
                    found_role_acl = True
            assert found_role_acl, f"Couldn't find {role_name} in {acls_lines}"

    @cluster(num_nodes=4)
    def test_bootstrap_with_recovery(self):
        """
        Smoke test that configuring automated recovery at bootstrap will kick
        in as appropriate.
        """
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set(
            "cloud_storage_attempt_cluster_restore_on_bootstrap", True
        )
        for t in self.topics:
            KgoVerifierProducer.oneshot(
                self.test_context,
                self.redpanda,
                t.name,
                self.message_size,
                100,
                batch_max_bytes=self.message_size * 8,
                timeout_sec=60,
            )
        quiesce_uploads(self.redpanda, [t.name for t in self.topics], timeout_sec=60)
        time.sleep(5)

        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)

        # Restart the nodes, overriding the recovery bootstrap config.
        extra_rp_conf = dict(cloud_storage_attempt_cluster_restore_on_bootstrap=True)
        self.redpanda.set_extra_rp_conf(extra_rp_conf)
        self.redpanda.write_bootstrap_cluster_config()
        self.redpanda.restart_nodes(
            self.redpanda.nodes, override_cfg_params=extra_rp_conf
        )

        # We should see a recovery begin automatically.
        self.redpanda._admin.await_stable_leader(
            "controller", partition=0, namespace="redpanda", timeout_s=60, backoff_s=2
        )

        def cluster_recovery_complete():
            return (
                "inactive"
                in self.redpanda._admin.get_cluster_recovery_status().json()["state"]
            )

        wait_until(cluster_recovery_complete, timeout_sec=60, backoff_sec=1)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        assert len(set(rpk.list_topics())) == len(self.topics), (
            "Incorrect number of topics restored"
        )


class ClusterRecoveryWithNameTest(RedpandaTest):
    """
    Tests for Whole Cluster Recovery (WCR) with custom name. I.e. when bucket
    contains data and metadata from multiple clusters.

    This test covers only the name matching logic. The actual recovery
    functionality is covered by ClusterRecoveryTest.
    """

    topics = [
        TopicSpec(
            name=f"topic-{n}",
            replication_factor=1,
        )
        for n in range(3)
    ]

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(test_context, fast_uploads=True)
        self.base_rp_conf = {
            # Test takes too much time otherwise.
            "group_topic_partitions": 2,
            "controller_snapshot_max_age_sec": 1,
            "cloud_storage_cluster_metadata_upload_interval_ms": 1000,
        }
        self.s3_bucket = si_settings.cloud_storage_bucket
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            si_settings=si_settings,
            extra_rp_conf=self.base_rp_conf,
        )

    @cluster(
        num_nodes=1,
        log_allow_list=[
            "Error starting cluster recovery request. Check logs for details.",
            "Error starting cluster recovery request: No matching metadata",
        ],
    )
    def test_admin_recovery(self):
        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "test-rpk-restore-cluster",
            }
        )
        self._wait_for_metadata_upload()

        self.logger.info("Recreating a brand new cluster without a name")
        self._stop_and_recreate_cluster({})

        with pytest.raises(requests.exceptions.HTTPError) as excinfo:
            self.redpanda._admin.initialize_cluster_recovery()
        assert excinfo.value.response.status_code == 500
        assert (
            excinfo.value.response.json()["message"]
            == "Error starting cluster recovery request. Check logs for details."
        ), excinfo.value.response.json()["message"]

        self.logger.info(
            "Updating cluster name to a random value and attempting recovery"
        )
        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "some-random-name",
            }
        )

        with pytest.raises(requests.exceptions.HTTPError) as excinfo:
            self.redpanda._admin.initialize_cluster_recovery()
        assert excinfo.value.response.status_code == 500
        assert (
            excinfo.value.response.json()["message"]
            == "Error starting cluster recovery request: No matching metadata"
        ), excinfo.value.response.json()["message"]

        self.logger.info("Setting the correct cluster name and attempting recovery")
        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "test-rpk-restore-cluster",
            }
        )
        self.redpanda._admin.initialize_cluster_recovery()
        wait_until(self._cluster_recovery_complete, timeout_sec=60, backoff_sec=1)

    @cluster(num_nodes=1)
    def test_admin_recovery_uuid_override(self):
        rpk = RpkTool(self.redpanda)

        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "test-my-cluster",
            }
        )
        self._wait_for_metadata_upload()
        initial_cluster_uuid = self.redpanda._admin.get_cluster_uuid()
        self.logger.debug(f"Initial cluster uuid: {initial_cluster_uuid}")

        self.logger.info(
            "Recreating the cluster with the same name but do not attempt recovery"
        )
        self._stop_and_recreate_cluster(
            {
                "cloud_storage_cluster_name": "test-my-cluster",
            }
        )
        self._wait_for_metadata_upload()

        self.logger.info("Verifying that no recovery happened")
        topics_on_cluster = set(rpk.list_topics())
        assert len(topics_on_cluster) == 0, (
            f"Expected no topics to be restored but have: {topics_on_cluster}"
        )

        self.logger.info("Recreate the cluster and attempt recovery from the first one")
        self._stop_and_recreate_cluster(
            {
                # Do not set the name yet, we want to validate that recovery fails without it first.
                "cloud_storage_cluster_name": None,
            }
        )
        self.logger.info(
            "Verifying that recovery without name fails if uuid override is provided, name is not set but bucket contains multiple clusters"
        )
        with pytest.raises(requests.exceptions.HTTPError) as excinfo:
            self.redpanda._admin.initialize_cluster_recovery(
                cluster_uuid_override=initial_cluster_uuid
            )
        assert excinfo.value.response.status_code == 400, (
            f"Status: {excinfo.value.response.status_code}"
        )
        assert (
            excinfo.value.response.json()["message"]
            == "Cluster is misconfigured for recovery. Check logs for details."
        ), excinfo.value.response.json()["message"]

        self.logger.info("Set a different cluster name and attempt to recovery")
        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "some-random-name",
            }
        )
        with pytest.raises(requests.exceptions.HTTPError) as excinfo:
            self.redpanda._admin.initialize_cluster_recovery(
                cluster_uuid_override=initial_cluster_uuid
            )
        assert excinfo.value.response.status_code == 400, (
            f"Status: {excinfo.value.response.status_code}"
        )
        assert (
            excinfo.value.response.json()["message"]
            == "Cluster is misconfigured for recovery. Check logs for details."
        ), excinfo.value.response.json()["message"]

        self.logger.info("Set the correct cluster name and attempt to recovery")
        self.redpanda._admin.patch_cluster_config(
            {
                "cloud_storage_cluster_name": "test-my-cluster",
            }
        )
        self.redpanda._admin.initialize_cluster_recovery(
            cluster_uuid_override=initial_cluster_uuid
        )
        wait_until(self._cluster_recovery_complete, timeout_sec=60, backoff_sec=1)

        topics_on_cluster = set(rpk.list_topics())
        assert len(topics_on_cluster) == len(self.topics), (
            f"Incorrect topics restored {topics_on_cluster} but expected {[t.name for t in self.topics]}"
        )

    @cluster(num_nodes=1)
    def test_bootstrap_with_recovery(self):
        bootstrap_wcr_conf = {
            "cloud_storage_attempt_cluster_restore_on_bootstrap": True,
            "cloud_storage_cluster_name": "the-cool-cluster",
        }

        self.redpanda._admin.patch_cluster_config(bootstrap_wcr_conf)

        # Wait for eventual metadata upload.
        self._wait_for_metadata_upload()

        self.logger.info("Recreating cluster to trigger recovery")
        self._stop_and_recreate_cluster(bootstrap_wcr_conf)

        def _assert_restore_original_cluster():
            rpk = RpkTool(self.redpanda)
            wait_until(self._cluster_recovery_complete, timeout_sec=60, backoff_sec=1)
            topics_on_cluster = set(rpk.list_topics())
            assert len(topics_on_cluster) == len(self.topics), (
                f"Incorrect topics restored {topics_on_cluster} but expected {[t.name for t in self.topics]}"
            )

        _assert_restore_original_cluster()

        # Give it time to upload metadata again so that we can restore it later.
        self._wait_for_metadata_upload()

        # It should be possible to create a new cluster explicitly with different name on the same bucket.
        self.logger.info("Creating a new cluster with different name")
        self._stop_and_recreate_cluster(
            {
                "cloud_storage_attempt_cluster_restore_on_bootstrap": True,
                "cloud_storage_cluster_name": "some-other-name",
            }
        )
        assert (
            "inactive"
            in self.redpanda._admin.get_cluster_recovery_status().json()["state"]
        )

        # Wait for eventual metadata upload.
        self._wait_for_metadata_upload()

        rpk = RpkTool(self.redpanda)
        topics_on_cluster = set(rpk.list_topics())
        assert len(topics_on_cluster) == 0, (
            f"Incorrect topics restored {topics_on_cluster} but expected none"
        )

        # Restore original cluster again.
        self.logger.info("Restoring original cluster again")
        self._stop_and_recreate_cluster(bootstrap_wcr_conf)
        _assert_restore_original_cluster()

    @cluster(
        num_nodes=1,
        log_allow_list=["Error looking for cluster recovery material in cloud"],
    )
    def test_bootstrap_with_recovery_mixed(self):
        """
        Test that recovery fails when cluster name is not set but bucket
        contains data from multiple clusters.
        """
        bootstrap_wcr_conf = {
            "cloud_storage_attempt_cluster_restore_on_bootstrap": True,
            "cloud_storage_cluster_name": "foo-cluster",
        }

        self.redpanda._admin.patch_cluster_config(bootstrap_wcr_conf)
        self._wait_for_metadata_upload()

        self.logger.info("Recreating cluster to trigger recovery and expect failure")
        self._stop_and_recreate_cluster(
            {
                "cloud_storage_attempt_cluster_restore_on_bootstrap": True,
                # No cluster name configured.
            },
            expect_fail=True,
        )
        assert self.redpanda.search_log_any(
            "Error looking for cluster recovery material in cloud, retrying: Cluster misconfiguration"
        )

    def _wait_for_metadata_upload(self):
        self.redpanda.wait_for_controller_snapshot(self.redpanda.nodes[0])
        time.sleep(5)

    def _stop_and_recreate_cluster(
        self, config: dict[str, Any], *, expect_fail: bool = False
    ):
        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)

        # Restart the nodes, overriding the recovery bootstrap config.
        self.redpanda.set_extra_rp_conf(
            {
                **self.base_rp_conf,
                **config,
            }
        )
        self.redpanda.write_bootstrap_cluster_config()
        try:
            self.redpanda.restart_nodes(self.redpanda.nodes)
        except ducktape.errors.TimeoutError:
            if expect_fail:
                self.logger.info(
                    "Cluster failed to start as expected due to recovery failure"
                )
                return
        assert not expect_fail, "Cluster restart was expected to fail but didn't"

        self.redpanda._admin.await_stable_leader(
            "controller",
            partition=0,
            namespace="redpanda",
            timeout_s=60,
            backoff_s=2,
        )

    def _cluster_recovery_complete(self):
        state = self.redpanda._admin.get_cluster_recovery_status().json()["state"]
        self.logger.debug(f"Cluster recovery state: {state}")
        if "failed" in state:
            raise RuntimeError(f"Cluster recovery failed with state: {state}")
        return "inactive" in state
