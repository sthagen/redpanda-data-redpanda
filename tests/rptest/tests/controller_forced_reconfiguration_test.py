# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass
from random import shuffle
from typing import Any, Optional

from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import PartitionDetails, Replica
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.redpanda_test import RedpandaTest

from rptest.util import wait_until_result

from connectrpc.unary import UnaryOutput
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.admin.proto.redpanda.core.admin.internal.v1 import (
    breakglass_pb2,
    breakglass_pb2_connect,
)


@dataclass
class NTP:
    namespace: str = "kafka"
    topic: str = "topic"
    partition: int = 0


@dataclass
class TimeoutConfig:
    timeout_s: int
    backoff_s: int


class ControllerForceReconfigurationTest(RedpandaTest):
    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super(ControllerForceReconfigurationTest, self).__init__(
            test_context,
            num_brokers=5,  # TODO this test is hardcoded to a test cluster size of 3
            *args,
            **kwargs,
        )

    def setUp(self):
        """done so we can custom start rp in each test"""
        pass

    def _start_redpanda(self, cluster_size: int) -> list[ClusterNode]:
        seed_nodes = self.redpanda.nodes[0:cluster_size]
        joiner_nodes = self.redpanda.nodes[cluster_size:]

        self.redpanda.set_seed_servers(seed_nodes)
        self.redpanda.start(nodes=seed_nodes, omit_seeds_on_idx_one=False)
        return joiner_nodes

    def _setup_topic(self, topic_spec: TopicSpec, timeout: TimeoutConfig):
        self.client().create_topic(topic_spec)
        # Wait for initial leader
        self.redpanda._admin.await_stable_leader(
            topic=topic_spec.name,
            replication=topic_spec.replication_factor,
            timeout_s=timeout.timeout_s,
            backoff_s=timeout.backoff_s,
        )

    """ start to be extracted section """

    def _living_nodes(self) -> list[ClusterNode]:
        return self.redpanda.started_nodes()

    def _living_hostnames(self) -> list[str]:
        hostnames: list[str] = []
        node: ClusterNode
        for node in self.redpanda.started_nodes():
            hostname = node.account.hostname
            assert hostname is not None
            hostnames.append(hostname)
        return hostnames

    def _wait_until_no_leader(self, ntp: NTP, timeout: TimeoutConfig):
        """Scrapes the debug endpoints of all replicas and checks if any of the replicas think they are the leader"""

        def no_leader():
            living_nodes = self._living_nodes()
            for living_node in living_nodes:
                state = self.redpanda._admin.get_partition_state(
                    ntp.namespace, ntp.topic, ntp.partition, node=living_node
                )
                if "replicas" not in state.keys() or len(state["replicas"]) == 0:
                    continue
                for r in state["replicas"]:
                    assert "raft_state" in r.keys()
                    if r["raft_state"]["is_leader"]:
                        return False
            return True

        wait_until(
            no_leader,
            timeout_sec=timeout.timeout_s,
            backoff_sec=timeout.backoff_s,
            err_msg="Partition has a leader",
        )

    def _stop_majority_nodes(
        self, ntp: NTP, timeout: TimeoutConfig, replication: int = 5
    ) -> tuple[list[Replica], list[Replica]]:
        """
        Stops a random majority of nodes hosting partition 0 of test topic.
        """
        assert self.redpanda

        def _get_details() -> tuple[bool, Optional[PartitionDetails]]:
            d = self.redpanda._admin._get_stable_configuration(
                hosts=self._living_hostnames(),
                namespace=ntp.namespace,
                topic=ntp.topic,
                partition=ntp.partition,
                replication=replication,
            )
            if d is None:
                return (False, None)
            return (True, d)

        partition_details: PartitionDetails = wait_until_result(
            _get_details, timeout_sec=timeout.timeout_s, backoff_sec=timeout.backoff_s
        )

        replicas = partition_details.replicas
        shuffle(replicas)
        mid = len(replicas) // 2 + 1
        (killed, alive) = (replicas[0:mid], replicas[mid:])
        for replica in killed:
            node = self.redpanda.get_node_by_id(replica.node_id)
            assert node
            self.logger.debug(f"Stopping node with node_id: {replica.node_id}")
            self.redpanda.stop_node(node)
        # The partition should be leaderless.
        self._wait_until_no_leader(ntp=ntp, timeout=timeout)
        return (killed, alive)

    def _toggle_recovery_mode(
        self, node: ClusterNode, timeout: TimeoutConfig, recovery_mode_enabled: bool
    ):
        self.logger.info(f"stopping node: {node.name}")
        self.redpanda.stop_node(node, timeout=timeout.timeout_s)

        self.logger.info(f"restarting node: {node.name}")
        self.redpanda.start_node(
            node,
            timeout=timeout.timeout_s,
            override_cfg_params={"recovery_mode_enabled": recovery_mode_enabled},
        )

    def _kill_node(self, node_id: int):
        node = self.redpanda.get_node_by_id(node_id)
        assert node
        self.logger.info(f"Stopping node with node_id: {node_id}")
        self.redpanda.stop_node(node)

    def _do_request(
        self,
        client: breakglass_pb2_connect.BreakglassServiceClient,
        request: breakglass_pb2.ControllerForcedReconfigurationRequest,
    ) -> UnaryOutput[breakglass_pb2.ControllerForcedReconfigurationResponse]:
        return client.call_controller_forced_reconfiguration(request)

    def _join_new_node(self, joiner_node: ClusterNode):
        self.redpanda.clean_node(joiner_node)
        self.redpanda.start_node(joiner_node)
        wait_until(
            lambda: self.redpanda.registered(joiner_node), timeout_sec=60, backoff_sec=5
        )

    def _check_topic_recovered(
        self, topic: TopicSpec, cluster_size: int, killed_node_ids: list[int]
    ):
        for partition in range(0, topic.partition_count):
            test_ntp = NTP(topic=topic.name, partition=partition)
            living_nodes = self._living_nodes()
            for living_node in living_nodes:
                state = self.redpanda._admin.get_partition_state(
                    test_ntp.namespace,
                    test_ntp.topic,
                    test_ntp.partition,
                    node=living_node,
                )
                leader_raft_state: Any = None
                for replica in state["replicas"]:
                    raft_state = replica["raft_state"]
                    if raft_state["is_leader"]:
                        leader_raft_state = raft_state
                        break

                assert leader_raft_state, "no leader found"

                nodes: list[int] = []
                nodes.append(leader_raft_state["node_id"])

                # get all followers that are NOT learners
                for follower in leader_raft_state["followers"]:
                    if not follower["is_learner"]:
                        nodes.append(follower["id"])

                assert len(nodes) == cluster_size, (
                    f"expected group of size: {cluster_size}, but found {len(nodes)}"
                )
                for killed_node_id in killed_node_ids:
                    assert killed_node_id not in nodes, (
                        f"dead node: {killed_node_id} still in configuration"
                    )

    def _wait_for_no_reconfigurations(self):
        def no_pending_force_reconfigurations():
            status = self.redpanda._admin.get_partition_balancer_status()
            return status["partitions_pending_force_recovery_count"] == 0

        wait_until(
            no_pending_force_reconfigurations,
            timeout_sec=120,
            backoff_sec=3,
            err_msg="reported force recovery count is non zero",
            retry_on_exc=True,
        )

    @cluster(num_nodes=6)
    def test_smoke_cfr(self):
        """
        1. create a cluster of size three
        2. add a topic and produce to it
        3. fail the majority of nodes in the cluster
        4. reboot into recovery mode
        5. force reconfigure the cluster to the remaining survivor
        6. add new brokers back to three
        7. reboot into normal mode
        8. produce to topic
        9. check that all partitions on topic have voter set of 3
        """
        admin = AdminV2(self.redpanda)

        # will start a cluster of 3 nodes on 1, 2, 3
        cluster_size: int = 3
        joiner_nodes = self._start_redpanda(cluster_size=cluster_size)

        controller_ntp = NTP(namespace="redpanda", topic="controller", partition=0)
        short_timeout = TimeoutConfig(timeout_s=30, backoff_s=2)

        topic = TopicSpec(
            replication_factor=3,
            partition_count=1,
            redpanda_remote_read=True,
            redpanda_remote_write=True,
        )

        self.client().create_topic(topic)

        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=10000,
            msg_count=1000,
        )

        killed, living = self._stop_majority_nodes(
            ntp=controller_ntp, timeout=short_timeout, replication=cluster_size
        )
        killed_node_ids = [dead_node.node_id for dead_node in killed]

        self.redpanda.logger.debug(f"killed nodes: {killed}, living nodes: {living}")

        designated_survivors = self._living_nodes()
        assert len(designated_survivors) == 1, (
            f"found too many living expected 1 found: {len(designated_survivors)}"
        )
        designated_survivor = designated_survivors[0]

        self._toggle_recovery_mode(
            node=designated_survivor,
            timeout=TimeoutConfig(timeout_s=60, backoff_s=2),
            recovery_mode_enabled=True,
        )

        self.redpanda.logger.debug("beginning CFR request")
        breakgass_client = admin.breakglass(node=designated_survivor)
        request = breakglass_pb2.ControllerForcedReconfigurationRequest(
            dead_node_ids=killed_node_ids, surviving_node_count=1
        )
        result = self._do_request(breakgass_client, request)
        self.redpanda.logger.debug("CFR request finished")

        error = result.error()
        assert error is None, f"CFR request failed with error {result.error()}"

        def controller_available():
            controller = self.redpanda.controller()
            return (
                controller is not None
                and self.redpanda.node_id(controller) not in killed_node_ids
            )

        self.redpanda.logger.debug("waiting for controller to recover")
        recovery_timeout = TimeoutConfig(timeout_s=240, backoff_s=10)
        wait_until(
            lambda: controller_available(),
            timeout_sec=recovery_timeout.timeout_s,
            backoff_sec=recovery_timeout.backoff_s,
            err_msg="Controller never came back",
        )
        self.redpanda.logger.debug("controller recovered")

        for joiner_node in joiner_nodes:
            self.redpanda.logger.debug(f"joining node {joiner_node.name}")
            self._join_new_node(joiner_node)

        self._toggle_recovery_mode(
            node=designated_survivor,
            timeout=TimeoutConfig(timeout_s=60, backoff_s=2),
            recovery_mode_enabled=False,
        )

        self.logger.debug(f"recovering from: {killed_node_ids}")
        self._rpk = RpkTool(self.redpanda)

        # issue a node wise recovery
        self._rpk.force_partition_recovery(
            from_nodes=killed_node_ids, to_node=designated_survivor
        )

        self._wait_for_no_reconfigurations()

        for dead_node_id in killed_node_ids:
            self.redpanda._admin.decommission_broker(dead_node_id, designated_survivor)

        self._check_topic_recovered(topic, cluster_size, killed_node_ids)

        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=10000,
            msg_count=1000,
        )
