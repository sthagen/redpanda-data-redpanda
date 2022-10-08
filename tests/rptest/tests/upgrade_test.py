# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
from packaging.version import Version

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
    KgoVerifierRandomConsumer,
    KgoVerifierConsumerGroupConsumer,
)
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import InstallOptions, RedpandaInstaller, wait_for_num_versions


class UpgradeFromSpecificVersion(RedpandaTest):
    """
    Basic test that upgrading software works as expected.
    """
    def __init__(self, test_context):
        super(UpgradeFromSpecificVersion,
              self).__init__(test_context=test_context, num_brokers=3)
        self.installer = self.redpanda._installer

    def setUp(self):
        # NOTE: `rpk redpanda admin brokers list` requires versions v22.1.x and
        # above.
        self.installer.install(self.redpanda.nodes, (22, 1, 3))
        super(UpgradeFromSpecificVersion, self).setUp()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_basic_upgrade(self):
        first_node = self.redpanda.nodes[0]

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert "v22.1.3" in unique_versions, unique_versions

        # Upgrade one node to the head version.
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 2)
        assert "v22.1.3" in unique_versions, unique_versions

        # Rollback the partial upgrade and ensure we go back to the original
        # state.
        self.installer.install([first_node], (22, 1, 3))
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert "v22.1.3" in unique_versions, unique_versions

        # Only once we upgrade the rest of the nodes do we converge on the new
        # version.
        self.installer.install([first_node], RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert "v22.1.3" not in unique_versions, unique_versions


class UpgradeFromPriorFeatureVersionTest(RedpandaTest):
    """
    Basic test that installs the previous feature version and performs an
    upgrade.
    """
    def __init__(self, test_context):
        super(UpgradeFromPriorFeatureVersionTest,
              self).__init__(test_context=test_context, num_brokers=1)
        self.installer = self.redpanda._installer

    def setUp(self):
        self.prev_version = \
            self.installer.highest_from_prior_feature_version(RedpandaInstaller.HEAD)
        self.installer.install(self.redpanda.nodes, self.prev_version)
        super(UpgradeFromPriorFeatureVersionTest, self).setUp()

    @cluster(num_nodes=1,
             log_allow_list=RESTART_LOG_ALLOW_LIST +
             [re.compile("cluster - .*Error while reconciling topic.*")])
    def test_basic_upgrade(self):
        node = self.redpanda.nodes[0]
        initial_version = Version(self.redpanda.get_version(node))
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)

        self.redpanda.restart_nodes([node])
        head_version_str = self.redpanda.get_version(node)
        head_version = Version(head_version_str)
        assert initial_version < head_version, f"{initial_version} vs {head_version}"

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert head_version_str in unique_versions, unique_versions


PREV_VERSION_LOG_ALLOW_LIST = [
    # e.g. cluster - controller_backend.cc:400 - Error while reconciling topics - seastar::abort_requested_exception (abort requested)
    "cluster - .*Error while reconciling topic.*",
    # Typo fixed in recent versions.
    # e.g.  raft - [follower: {id: {1}, revision: {10}}] [group_id:3, {kafka/topic/2}] - recovery_stm.cc:422 - recovery append entries error: raft group does not exists on target broker
    "raft - .*raft group does not exists on target broker",
    # e.g. rpc - Service handler thrown an exception - seastar::gate_closed_exception (gate closed)
    "rpc - .*gate_closed_exception.*",
    # Tests on mixed versions will start out with an unclean restart before
    # starting a workload.
    "(raft|rpc) - .*(disconnected_endpoint|Broken pipe|Connection reset by peer)",
]


class UpgradeBackToBackTest(PreallocNodesTest):
    """
    Test that runs through two rolling upgrades while running through workloads.
    """
    MSG_SIZE = 100
    PRODUCE_COUNT = 100000
    RANDOM_READ_COUNT = 100
    RANDOM_READ_PARALLEL = 4
    CONSUMER_GROUP_READERS = 4
    topics = (TopicSpec(partition_count=3, replication_factor=3), )

    def __init__(self, test_context):
        if self.debug_mode:
            self.MSG_SIZE = 10
            self.RANDOM_READ_COUNT = 10
            self.RANDOM_READ_PARALLEL = 1
            self.CONSUMER_GROUP_READERS = 1
            self.topics = (TopicSpec(partition_count=1,
                                     replication_factor=3), )
        super(UpgradeBackToBackTest, self).__init__(test_context,
                                                    num_brokers=3,
                                                    node_prealloc_count=1)
        self.installer = self.redpanda._installer
        self.intermediate_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)
        self.initial_version = self.installer.highest_from_prior_feature_version(
            self.intermediate_version)

        self._producer = KgoVerifierProducer(test_context, self.redpanda,
                                             self.topic, self.MSG_SIZE,
                                             self.PRODUCE_COUNT,
                                             self.preallocated_nodes)
        self._seq_consumer = KgoVerifierSeqConsumer(test_context,
                                                    self.redpanda,
                                                    self.topic,
                                                    self.MSG_SIZE,
                                                    self.preallocated_nodes,
                                                    debug_logs=True)
        self._rand_consumer = KgoVerifierRandomConsumer(
            test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self.RANDOM_READ_COUNT, self.RANDOM_READ_PARALLEL,
            self.preallocated_nodes)
        self._cg_consumer = KgoVerifierConsumerGroupConsumer(
            test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self.CONSUMER_GROUP_READERS, self.preallocated_nodes)

        self._consumers = [
            self._seq_consumer, self._rand_consumer, self._cg_consumer
        ]

    def setUp(self):
        self.installer.install(self.redpanda.nodes, self.initial_version)
        super(UpgradeBackToBackTest, self).setUp()

    @cluster(num_nodes=4, log_allow_list=PREV_VERSION_LOG_ALLOW_LIST)
    @parametrize(single_upgrade=True)
    @parametrize(single_upgrade=False)
    def test_upgrade_with_all_workloads(self, single_upgrade):
        if single_upgrade:
            # If the test should exercise workloads with just a single upgrade,
            # start at the intermediate version -- this test will just test a
            # rolling restart followed by a rolling upgrade.
            self.initial_version = self.intermediate_version
            self.installer.install(self.redpanda.nodes, self.initial_version)
            self.redpanda.restart_nodes(self.redpanda.nodes,
                                        start_timeout=90,
                                        stop_timeout=90)
        self._producer.start(clean=False)
        self._producer.wait_for_offset_map()
        wrote_at_least = self._producer.produce_status.acked
        for consumer in self._consumers:
            consumer.start(clean=False)

        def stop_producer():
            self._producer.wait()
            assert self._producer.produce_status.acked == self.PRODUCE_COUNT

        produce_during_upgrade = self.initial_version >= (22, 1, 0)
        if produce_during_upgrade:
            # Give ample time to restart, given the running workload.
            self.installer.install(self.redpanda.nodes,
                                   self.intermediate_version)
            self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                                start_timeout=90,
                                                stop_timeout=90)
        else:
            # If there's no maintenance mode, write workloads during the
            # restart may be affected, so stop our writes up front.
            stop_producer()
            self.installer.install(self.redpanda.nodes,
                                   self.intermediate_version)
            self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                                start_timeout=90,
                                                stop_timeout=90,
                                                use_maintenance_mode=False)

            # When upgrading from versions that don't support maintenance mode
            # (v21.11 and below), there is a migration of the consumer offsets
            # topic to be mindful of.
            rpk = RpkTool(self.redpanda)

            def _consumer_offsets_present():
                try:
                    rpk.describe_topic("__consumer_offsets")
                except Exception as e:
                    if "Topic not found" in str(e):
                        return False
                return True

            wait_until(_consumer_offsets_present,
                       timeout_sec=90,
                       backoff_sec=3)

        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            start_timeout=90,
                                            stop_timeout=90)

        for consumer in self._consumers:
            consumer.wait()

        assert self._seq_consumer.consumer_status.validator.valid_reads >= wrote_at_least
        assert self._rand_consumer.consumer_status.validator.total_reads >= self.RANDOM_READ_COUNT * self.RANDOM_READ_PARALLEL
        assert self._cg_consumer.consumer_status.validator.valid_reads >= wrote_at_least


class UpgradeWithWorkloadTest(EndToEndTest):
    """
    Test class that performs upgrades while verifying a concurrently running
    workload is making progress.
    """
    def setUp(self):
        super(UpgradeWithWorkloadTest, self).setUp()
        # Start at a version that supports rolling restarts.
        self.initial_version = (22, 1, 3)

        # Use a relatively low throughput to give the restarted node a chance
        # to catch up. If the node is particularly slow compared to the others
        # (e.g. a locally-built debug binary), catching up can take a while.
        self.producer_msgs_per_sec = 10
        install_opts = InstallOptions(version=self.initial_version)
        self.start_redpanda(num_nodes=3, install_opts=install_opts)
        self.installer = self.redpanda._installer

        # Start running a workload.
        spec = TopicSpec(name="topic", partition_count=2, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(num_nodes=1, throughput=self.producer_msgs_per_sec)
        self.start_consumer(num_nodes=1)
        self.await_startup(min_records=self.producer_msgs_per_sec)

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_rolling_upgrade(self):
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        # Give ample time to restart, given the running workload.
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            start_timeout=90,
                                            stop_timeout=90)

        post_upgrade_num_msgs = self.producer.num_acked
        self.run_validation(min_records=post_upgrade_num_msgs +
                            (self.producer_msgs_per_sec * 3))

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(upgrade_after_rollback=True)
    @parametrize(upgrade_after_rollback=False)
    def test_rolling_upgrade_with_rollback(self, upgrade_after_rollback):
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)

        # Upgrade one node.
        first_node = self.redpanda.nodes[0]
        # Give ample time to restart, given the running workload.
        self.redpanda.rolling_restart_nodes([first_node],
                                            start_timeout=90,
                                            stop_timeout=90)

        def await_progress():
            num_msgs = self.producer.num_acked
            self.await_num_produced(num_msgs +
                                    (self.producer_msgs_per_sec * 3))
            self.await_num_consumed(num_msgs +
                                    (self.producer_msgs_per_sec * 3))

        # Ensure that after we upgrade a node, we're still able to make
        # progress.
        await_progress()

        # Then roll it back; we should still be able to make progress.
        self.installer.install([first_node], self.initial_version)
        self.redpanda.rolling_restart_nodes([first_node],
                                            start_timeout=90,
                                            stop_timeout=90)
        await_progress()

        if upgrade_after_rollback:
            self.installer.install([first_node], RedpandaInstaller.HEAD)
            self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                                start_timeout=90,
                                                stop_timeout=90)

        post_rollback_num_msgs = self.producer.num_acked
        self.run_validation(min_records=post_rollback_num_msgs +
                            (self.producer_msgs_per_sec * 3),
                            enable_idempotence=True)
