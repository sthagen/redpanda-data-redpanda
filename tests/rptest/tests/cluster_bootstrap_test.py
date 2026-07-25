# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import concurrent.futures
import threading

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from requests.exceptions import ConnectionError

from rptest.clients.rpk import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService, RpkTool, SISettings
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.services.utils import NodeCrash
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception


def set_seeds_for_cluster(redpanda, num_seeds):
    seeds = [redpanda.nodes[i] for i in range(num_seeds)]
    redpanda.set_seed_servers(seeds)


class ClusterBootstrapNew(RedpandaTest):
    """
    Tests verifying new cluster bootstrap in Seed Driven Cluster Bootstrap mode
    """

    def __init__(self, test_context):
        super(ClusterBootstrapNew, self).__init__(
            test_context=test_context, num_brokers=3
        )
        self.admin = self.redpanda._admin

    def setUp(self):
        # Defer startup to test body.
        pass

    @cluster(num_nodes=3, log_allow_list=["seed_servers cannot be empty"])
    def test_misconfigured_root_driven_bootstrap(self):
        """
        Test that empty_seed_starts_cluster=False prevents root-driven
        bootstrap from occurring.
        """
        for node in self.redpanda.nodes:
            self.redpanda.set_extra_node_conf(
                node, {"empty_seed_starts_cluster": False}
            )

        # setup seed servers on the other two nodes to prevent them from joining
        # cluster point the nodes to node 0
        for node in self.redpanda.nodes[1:]:
            self.redpanda.set_seed_servers([self.redpanda.nodes[0]])

        try:
            self.redpanda.start(omit_seeds_on_idx_one=True)
            assert False, "Should have been unable to start"
        except NodeCrash as e:
            # The cluster should be unable to start, and node 0 should shut down during startup
            assert len(e.crashes) == 1, f"Unexpected crashes: {e.crashes}"
            assert e.crashes[0][0] == self.redpanda.nodes[0], (
                f"Unexpected crashes: {e.crashes}"
            )
            pass

        for node in self.redpanda.nodes:
            # None of the nodes was configured in a way that could get past attempting
            # to join a cluster: node 1 has no seed servers, and nodes 2,3 are not in
            # their seed servers so do not self-identify as founders
            with expect_exception(ConnectionError, lambda _: True):
                # Try connecting to the admin API
                self.redpanda._admin.get_cluster_uuid(node)

    @cluster(num_nodes=3)
    @matrix(
        num_seeds=[1, 2, 3],
        auto_assign_node_ids=[False, True],
        empty_seed_starts_cluster=[False, True],
        with_enterprise_features=[False, True],
    )
    def test_three_node_bootstrap(
        self,
        num_seeds,
        auto_assign_node_ids,
        empty_seed_starts_cluster,
        with_enterprise_features,
    ):
        if with_enterprise_features:
            self.redpanda.add_extra_rp_conf(
                {"partition_autobalancing_mode": "continuous"}
            )

        set_seeds_for_cluster(self.redpanda, num_seeds)
        for node in self.redpanda.nodes:
            self.redpanda.set_extra_node_conf(
                node, {"empty_seed_starts_cluster": empty_seed_starts_cluster}
            )
        self.redpanda.start(
            auto_assign_node_id=auto_assign_node_ids, omit_seeds_on_idx_one=False
        )
        node_ids_per_idx = {}
        for n in self.redpanda.nodes:
            idx = self.redpanda.idx(n)
            node_ids_per_idx[idx] = self.redpanda.node_id(n)

        brokers = self.admin.get_brokers()
        assert 3 == len(brokers), f"Got {len(brokers)} brokers"

        # Restart our nodes and make sure our node IDs persist across restarts.
        self.redpanda.restart_nodes(
            self.redpanda.nodes,
            auto_assign_node_id=auto_assign_node_ids,
            omit_seeds_on_idx_one=False,
        )
        for idx in node_ids_per_idx:
            n = self.redpanda.get_node(idx)
            expected_node_id = node_ids_per_idx[idx]
            node_id = self.redpanda.node_id(n)
            assert expected_node_id == node_id, (
                f"Expected {expected_node_id} but got {node_id}"
            )


class ClusterBootstrapFiveNodes(RedpandaTest):
    """
    Tests verifying new cluster bootstrap in Seed Driven Cluster Bootstrap mode
    """

    def __init__(self, test_context):
        super(ClusterBootstrapFiveNodes, self).__init__(
            test_context=test_context, num_brokers=5
        )
        self.admin = self.redpanda._admin

    def setUp(self):
        # Defer startup to test body.
        pass

    @cluster(num_nodes=5)
    def test_topic_creation_during_bootstrap(self):
        """
        The test validates if the cluster is able to correctly bootstrap while
        executing operations during the bootstrap process.
        """
        stop_ev = threading.Event()
        set_seeds_for_cluster(self.redpanda, 5)

        def describe_group():
            rpk = RpkTool(self.redpanda)
            while not stop_ev.is_set():
                try:
                    topic = TopicSpec(partition_count=1, replication_factor=3)
                    rpk.create_topic(topic.name, partitions=1, replicas=3)
                    rpk.group_describe("test_group")
                except Exception:
                    pass

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            fut = executor.submit(lambda: describe_group())
            try:
                for node in self.redpanda.nodes:
                    self.redpanda.set_extra_node_conf(
                        node, {"empty_seed_starts_cluster": False}
                    )

                # storage init check is skipped because this test runs
                # operations against the cluster during bootup which means we
                # can't know exactly what to expect the state of storage to be
                # after a broker starts.
                self.redpanda.start(
                    auto_assign_node_id=True,
                    omit_seeds_on_idx_one=False,
                    skip_storage_init_check=True,
                )
            finally:
                stop_ev.set()
            fut.result(timeout=10)


class ClusterBootstrapUpgrade(RedpandaTest):
    """
    Tests verifying upgrade of cluster from a pre-Seed-Driven-Bootstrap version
    """

    def __init__(self, test_context):
        super(ClusterBootstrapUpgrade, self).__init__(
            test_context=test_context, num_brokers=3
        )
        self.installer = self.redpanda._installer
        self.admin = self.redpanda._admin

    def setUp(self):
        prev_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD
        )
        # NOTE: `rpk redpanda admin brokers list` requires versions v22.1.x and
        # above.
        _, self.oldversion_str = self.installer.install(
            self.redpanda.nodes, prev_version
        )
        set_seeds_for_cluster(self.redpanda, 3)
        super(ClusterBootstrapUpgrade, self).setUp()

    @cluster(num_nodes=3)
    @matrix(empty_seed_starts_cluster=[False, True])
    def test_change_bootstrap_configs_after_upgrade(self, empty_seed_starts_cluster):
        # Upgrade the cluster to begin using the new binary, but don't change
        # any configs yet.
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes)

        # Now update the configs.
        self.redpanda.rolling_restart_nodes(
            self.redpanda.nodes,
            override_cfg_params={
                "empty_seed_starts_cluster": empty_seed_starts_cluster
            },
            omit_seeds_on_idx_one=False,
        )

    @cluster(num_nodes=3)
    @matrix(empty_seed_starts_cluster=[False, True])
    def test_change_bootstrap_configs_during_upgrade(self, empty_seed_starts_cluster):
        # Upgrade the cluster as we change the configs node-by-node.
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.rolling_restart_nodes(
            self.redpanda.nodes,
            override_cfg_params={
                "empty_seed_starts_cluster": empty_seed_starts_cluster
            },
            omit_seeds_on_idx_one=False,
        )


class ClusterBootstrapJoinerConfigPriming(RedpandaTest):
    """
    Ensures that a first time joiner (non-seed) node correctly recieves a cluster
    wide view of the config before initializing its subsystems (just as a restarting node would).
    """

    def __init__(self, test_context):
        super().__init__(
            test_context=test_context,
            num_brokers=4,
            si_settings=SISettings(test_context=test_context),
        )
        self.admin = self.redpanda._admin

    def setUp(self):
        # Defer startup to the test body so we can add the joiner separately.
        pass

    @cluster(num_nodes=4)
    def test_joiner_primes_config_before_sizing_memory_groups(self):
        seeds = self.redpanda.nodes[:3]
        joiner = self.redpanda.nodes[3]

        # Form the cluster (with cloud storage enabled as a cluster-level
        # bootstrap config) using only the seed nodes.
        set_seeds_for_cluster(self.redpanda, num_seeds=3)
        self.redpanda.start(nodes=seeds)

        # Make `joiner` a genuine first-time joiner that must learn the cluster's
        # cloud-storage configuration from its join snapshot rather than from a
        # local file. ducktape pre-writes .bootstrap.yaml to every node; remove
        # it so the joiner starts with cloud_storage_enabled at its default
        # (false), exactly as a freshly-(re)added node does in production.
        joiner.account.ssh(f"rm -f {RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE}")

        # Add the joiner and wait for it to fully register with the cluster.
        self.redpanda.start_node(joiner)
        wait_until(
            lambda: self.redpanda.registered(joiner),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="joiner failed to register with the cluster",
        )

        # The joiner logs its per-shard memory-group allocations once at
        # startup. With cloud storage enabled cluster-wide, its cloud-topics
        # compaction reservation must be non-zero; a zero reservation means it
        # sized memory groups before applying its join snapshot.
        assert self.redpanda.search_log_node(
            joiner, "Per shard memory group allocations"
        ), "joiner did not log its per-shard memory-group allocations"
        assert not self.redpanda.search_log_node(
            joiner, "cloud topics compaction: 0.000bytes"
        ), (
            "joiner sized its memory groups before its cloud storage "
            "configuration was primed (cloud-topics reservation was 0)"
        )
        assert self.redpanda.search_log_node(
            joiner, "Resolved node identity early as node_id"
        ), "joiner did not take the early first-time-join prime path"


class ClusterBootstrapWipedSeedRejoin(RedpandaTest):
    """
    Regression test for a wiped seed node rejoining a cloud-storage cluster.
    """

    def __init__(self, test_context):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=SISettings(test_context=test_context),
        )

    def setUp(self):
        # Defer startup so we can wipe and re-add a seed from the test body.
        pass

    PRIMED_EARLY = "Resolved node identity early as node_id"
    ZERO_CLOUD_RESERVATION = "cloud topics compaction: 0.000bytes"

    @cluster(num_nodes=3)
    def test_wiped_seed_rejoin_primes_config_early(self):
        # All three nodes are seeds; cloud storage is enabled cluster-wide.
        set_seeds_for_cluster(self.redpanda, num_seeds=3)
        self.redpanda.start()

        victim = self.redpanda.nodes[2]

        # Wipe the seed's local state and its .bootstrap.yaml so it rejoins as a
        # wiped seed with cloud_storage_enabled at its default (false) locally,
        # forcing it to learn cloud storage from the cluster. Truncate the log
        # too so the assertions below only see the rejoin boot, not the original
        # one that had cloud storage enabled from .bootstrap.yaml.
        self.redpanda.stop_node(victim)
        self.redpanda.remove_local_data(victim)
        victim.account.ssh(f"rm -f {RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE}")
        victim.account.ssh(f"truncate -s 0 {RedpandaService.STDOUT_STDERR_CAPTURE}")

        self.redpanda.start_node(victim)
        wait_until(
            lambda: self.redpanda.registered(victim),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="wiped seed failed to rejoin the cluster",
        )

        # The wiped seed should have registered with the existing cluster via
        # the bounded require_existing_cluster path and primed its identity
        # early, rather than falling through to the late founder/resolve path.
        assert self.redpanda.search_log_node(victim, self.PRIMED_EARLY), (
            "wiped seed did not take the early prime path "
            "(require_existing_cluster registration)"
        )

        # Memory groups should be sized *with* cloud storage enabled, so the
        # running config and the sizing agree.
        assert not self.redpanda.search_log_node(victim, self.ZERO_CLOUD_RESERVATION), (
            "wiped seed sized its memory groups before priming cloud storage "
            "(cloud-topics reservation was 0)"
        )
