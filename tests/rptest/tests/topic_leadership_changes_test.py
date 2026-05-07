# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest


class TopicLeadershipChangesTest(RedpandaTest):
    """
    Verify that the kafka_leadership_changes internal metric accumulates
    correctly and is preserved when replicas move across nodes.
    """

    METRIC_NAME = "vectorized_kafka_leadership_changes"

    topics = (TopicSpec(partition_count=1, replication_factor=3),)

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(
            test_context=test_context,
            num_brokers=7,
            extra_rp_conf={"enable_leader_balancer": False},
        )
        self.admin = Admin(self.redpanda)

    def get_leadership_changes_total(self, topic: str) -> int:
        """Read kafka_leadership_changes from all nodes, return sum."""
        samples = self.redpanda.metrics_sample(
            name=self.METRIC_NAME,
            metrics_endpoint=MetricsEndpoint.METRICS,
        )
        assert samples is not None, f"{self.METRIC_NAME} metric not found"
        return sum(
            int(s.value)
            for s in samples.samples
            if s.labels.get("redpanda_topic") == topic
        )

    def has_leadership_changes_metric(self, topic: str) -> bool:
        """Check whether the metric exists for the given topic."""
        samples = self.redpanda.metrics_sample(
            name=self.METRIC_NAME,
            metrics_endpoint=MetricsEndpoint.METRICS,
        )
        if samples is None:
            return False
        return any(s.labels.get("redpanda_topic") == topic for s in samples.samples)

    def get_replica_node_ids(self, topic: str, partition: int = 0) -> set[int]:
        """Return the set of node IDs currently hosting the partition."""
        info = self.admin.get_partitions(topic, partition)
        return {r["node_id"] for r in info["replicas"]}

    def transfer_leadership(self, topic: str, partition: int, target_id: int) -> int:
        """Request leadership transfer and wait for a stable leader.

        The actual leader may differ from target_id due to races.
        Returns the node_id of the new stable leader.
        """
        self.admin.partition_transfer_leadership("kafka", topic, partition, target_id)
        return self.admin.await_stable_leader(topic, partition=partition, timeout_s=30)

    def assert_approx(self, actual: int, expected: int, tolerance: int = 2) -> None:
        self.logger.info(f"assert_approx: {expected=}, {actual=}")
        assert expected <= actual <= expected + tolerance, (
            f"Expected {expected}..{expected + tolerance}, got {actual}"
        )

    @cluster(num_nodes=7)
    def test_metric_preserved_across_replica_move(self):
        topic = self.topics[0].name

        # Wait for initial leader.
        initial_leader = self.admin.await_stable_leader(
            topic, partition=0, timeout_s=30
        )
        initial_replicas = self.get_replica_node_ids(topic)
        self.logger.info(
            f"Initial replicas: {initial_replicas}, leader: {initial_leader}"
        )

        # Transfer leadership a few times among the initial replicas.
        targets = sorted(initial_replicas - {initial_leader})
        for target in targets:
            self.transfer_leadership(topic, 0, target)
        transfers_done = len(targets)
        expected = 1 + transfers_done  # +1 for the initial leader election.
        metric = self.get_leadership_changes_total(topic)
        self.logger.info(f"After {transfers_done} transfers: {metric=}")
        self.assert_approx(metric, expected)
        metric_before_move = metric

        # Move replicas to a completely different set of nodes.
        all_node_ids = {self.redpanda.node_id(n) for n in self.redpanda.nodes}
        new_node_ids = list(all_node_ids - initial_replicas)[:3]
        self.logger.info(f"Moving replicas from {initial_replicas} to {new_node_ids}")
        new_assignments = [{"node_id": n, "core": 0} for n in new_node_ids]
        self.admin.set_partition_replicas(topic, 0, new_assignments)

        # Wait for the move to complete.
        wait_until(
            lambda: self.get_replica_node_ids(topic) == set(new_node_ids),
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Replica move did not complete",
        )
        self.admin.await_stable_leader(topic, partition=0, timeout_s=30)
        metric_after_move = self.get_leadership_changes_total(topic)

        # The move itself may cause leadership changes, but the counter
        # must not decrease.
        self.logger.info(f"{metric_after_move=}, {metric_before_move=}")
        assert metric_after_move >= metric_before_move, (
            f"Metric decreased after move: {metric_after_move} < {metric_before_move}"
        )

        # Transfer leadership among the new replicas.
        leader_after_move = self.admin.get_partition_leader(
            namespace="kafka", topic=topic, partition=0
        )
        other_new_replicas = set(new_node_ids) - {leader_after_move}
        for target in other_new_replicas:
            self.transfer_leadership(topic, 0, target)
        transfers_after_move = len(other_new_replicas)

        final_metric = self.get_leadership_changes_total(topic)
        self.logger.info(
            f"After {transfers_after_move} more transfers: {final_metric=}"
        )
        self.assert_approx(final_metric, metric_after_move + transfers_after_move)

        # Delete the topic and verify the metric is gone.
        rpk = RpkTool(self.redpanda)
        rpk.delete_topic(topic)
        wait_until(
            lambda: not self.has_leadership_changes_metric(topic),
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"Metric {self.METRIC_NAME} still present after topic deletion",
        )

        # Re-create the topic with the same name and verify the counter
        # starts fresh (only the initial leader election).
        recreated_partitions = 10
        rpk.create_topic(topic, partitions=recreated_partitions, replicas=3)
        for p in range(recreated_partitions):
            self.admin.await_stable_leader(topic, partition=p, timeout_s=30)
        recreated_metric = self.get_leadership_changes_total(topic)
        self.assert_approx(recreated_metric, recreated_partitions)
