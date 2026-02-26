# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any, Callable

from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.admin.v2 import Admin as AdminV2, l0_pb, ntp_pb
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin as AdminV1
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.cloud_topics.e2e_test import EndToEndCloudTopicsBase
import rptest.tests.cloud_topics.utils as ct_utils


class CloudTopicsSizeReportingTest(EndToEndCloudTopicsBase):
    """
    Test that the L0 size estimator contributes to DescribeLogDirs size
    reporting, and that the total reported size is consistent as data
    moves from L0 to L1.

    Uses three independent observation points:
      - L0 admin API (GetSizeEstimate) for the L0 size estimator
      - L1 admin API (metastore GetSize) for the L1 metastore size
      - DescribeLogDirs (via rpk) for the combined L0+L1 size
    """

    topic_name = "size_reporting_test"

    # Override base class topics - we create our own single-partition topic.
    topics = ()

    def __init__(self, test_context: TestContext):
        # Disable reconciliation loop so data stays in L0. We will re-enable
        # it later with fast intervals to trigger L0 -> L1 movement.
        extra_rp_conf: dict[str, Any] = {
            "cloud_topics_disable_reconciliation_loop": True,
        }
        super().__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
        )
        assert self.redpanda
        self.admin_v1 = AdminV1(self.redpanda)
        self.admin_v2 = AdminV2(self.redpanda)

    def setUp(self) -> None:
        super().setUp()
        self.rpk.create_topic(
            topic=self.topic_name,
            partitions=1,
            replicas=3,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
            },
        )

    # --- Helpers ---

    def _get_l0_size(self, topic: str, partition: int) -> int:
        """Query L0 active bytes via the admin API."""
        resp = self.admin_v2.l0().get_size_estimate(
            l0_pb.GetSizeEstimateRequest(
                partition=ntp_pb.TopicPartition(topic=topic, partition=partition)
            )
        )
        return resp.active_bytes

    def _get_l1_size(self, topic: str, partition: int) -> int | None:
        """Query L1 size via the metastore admin API."""
        return ct_utils.get_l1_partition_size(self.admin, topic, partition)

    def _get_describe_log_dirs_size(self, topic: str, partition: int) -> int:
        """Query partition size via DescribeLogDirs (rpk cluster logdirs describe).

        DescribeLogDirs returns one entry per broker that hosts a replica.
        We return the max across brokers as a representative single-replica size.
        """
        sizes: list[int] = []
        for item in self.rpk.describe_log_dirs():  # pyright: ignore[reportUnknownVariableType]
            if item.topic == topic and item.partition == partition:  # pyright: ignore[reportUnknownMemberType]
                sizes.append(int(item.size))  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        return max(sizes) if sizes else 0

    def _produce_data(self, bytes_to_produce: int) -> None:
        assert self.redpanda
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size=1024,
            msg_count=bytes_to_produce // 1024,
        )

    def _resume_reconciliation(self, interval_ms: int = 2000) -> None:
        """Re-enable reconciliation with a fast interval."""
        assert self.redpanda
        self.redpanda.set_cluster_config(
            {
                "cloud_topics_disable_reconciliation_loop": False,
                "cloud_topics_reconciliation_min_interval": interval_ms,
                "cloud_topics_reconciliation_max_interval": interval_ms,
            }
        )

    def _restart_node(self, node: ClusterNode) -> None:
        """Restart a node and wait for it to rejoin the cluster."""
        assert self.redpanda
        node_id = self.redpanda.node_id(node)
        self.logger.info(f"Restarting node {node.name} (id={node_id})")
        self.redpanda.stop_node(node, timeout=30)
        self.redpanda.start_node(node, timeout=30)

    def _transfer_leadership(self, topic: str, partition: int) -> None:
        """Transfer partition leadership to a different replica."""
        self.admin_v1.transfer_leadership_to(
            namespace="kafka",
            topic=topic,
            partition=partition,
            target_id=None,
        )
        self.admin_v1.await_stable_leader(
            topic=topic,
            partition=partition,
            namespace="kafka",
            timeout_s=30,
            backoff_s=2,
        )

    def _wait_for_l0_size(
        self,
        topic: str,
        partition: int,
        predicate: Callable[[int], bool],
        timeout_sec: int = 30,
        msg: str = "",
    ) -> int:
        last_size = [0]

        def check() -> bool:
            size = self._get_l0_size(topic, partition)
            last_size[0] = size
            return predicate(size)

        wait_until(
            condition=check,
            timeout_sec=timeout_sec,
            backoff_sec=3,
            err_msg=lambda: f"L0 size check failed ({msg}): last_size={last_size[0]}",
            retry_on_exc=True,
        )
        return last_size[0]

    # --- Test ---

    @cluster(num_nodes=4)
    def test_l0_size_reporting(self) -> None:
        """
        Verify that L0 data appears in DescribeLogDirs, survives restarts
        and leadership transfers, and that total size is roughly preserved
        as data moves from L0 to L1.
        """
        assert self.redpanda
        topic = self.topic_name
        partition = 0
        produce_bytes = 100 * 1024 * 1024  # 100 MiB

        # The L0 size estimator checkpoints cumulative bytes at sparse
        # offsets with a default interval of 50 MiB. The worst-case
        # estimation error is one checkpoint interval, so with 100 MiB
        # of data we should always see at least 50 MiB reported.
        min_expected_bytes = produce_bytes // 2

        # --- Phase 1: Data in L0 only (reconciliation disabled) ---

        self.logger.info("Phase 1: Producing data with reconciliation disabled")
        self._produce_data(produce_bytes)

        # L0 size should be reported via the admin API.
        l0_size = self._wait_for_l0_size(
            topic,
            partition,
            lambda s: s > 0,
            msg="L0 size should be positive after produce",
        )
        self.logger.info(f"L0 size after produce: {l0_size}")

        # L1 should have no data since reconciliation is disabled.
        l1_size = self._get_l1_size(topic, partition)
        self.logger.info(f"L1 size (should be 0 or None): {l1_size}")
        assert l1_size is None or l1_size == 0, (
            f"Expected no L1 data with reconciliation disabled, got {l1_size}"
        )

        # DescribeLogDirs should report the L0 data through the Kafka path.
        # With no L1 data, it should be close to the L0 size.
        dld_size = self._get_describe_log_dirs_size(topic, partition)
        self.logger.info(
            f"DescribeLogDirs size (L0-only): {dld_size}, L0 admin: {l0_size}"
        )
        assert dld_size > 0, (
            f"DescribeLogDirs should report positive size with L0 data, got {dld_size}"
        )

        # --- Phase 2: L0 estimator survives restart ---

        self.logger.info("Phase 2: Verifying L0 estimator survives restart")
        leader_id = self.admin_v1.await_stable_leader(
            topic=topic,
            partition=partition,
            namespace="kafka",
            timeout_s=30,
            backoff_s=2,
        )
        leader_node = self.redpanda.get_node(leader_id)
        self._restart_node(leader_node)

        size_after_restart = self._wait_for_l0_size(
            topic,
            partition,
            lambda s: s >= min_expected_bytes,
            msg=f"L0 size should be >= {min_expected_bytes} after restart",
        )
        self.logger.info(f"L0 size after restart: {size_after_restart}")

        # --- Phase 3: L0 estimator survives leadership transfer ---

        self.logger.info("Phase 3: Verifying L0 estimator survives leadership transfer")
        self._transfer_leadership(topic, partition)

        size_after_transfer = self._wait_for_l0_size(
            topic,
            partition,
            lambda s: s >= min_expected_bytes,
            msg=f"L0 size should be >= {min_expected_bytes} after leadership transfer",
        )
        self.logger.info(f"L0 size after leadership transfer: {size_after_transfer}")

        # --- Phase 4: Resume reconciliation, data moves L0 -> L1 ---

        self.logger.info("Phase 4: Resuming reconciliation")
        l0_before = self._get_l0_size(topic, partition)
        self.logger.info(f"L0 size before reconciliation: {l0_before}")

        self._resume_reconciliation(interval_ms=2000)

        # Wait for L1 to receive data.
        ct_utils.wait_until_l1_partition_size(
            self.admin,
            topic,
            partition,
            lambda size: size > 0,
            timeout_sec=120,
        )

        # Wait for full reconciliation.
        self.wait_until_reconciled(topic=topic, partition=partition)

        l0_after = self._get_l0_size(topic, partition)
        l1_after = self._get_l1_size(topic, partition)
        self.logger.info(f"After reconciliation: L0={l0_after}, L1={l1_after}")

        # After full reconciliation, L1 should hold the data.
        assert l1_after is not None and l1_after > 0

        # L0 active bytes should have decreased (data moved to L1).
        assert l0_after < l0_before, (
            f"L0 should shrink after reconciliation: before={l0_before}, after={l0_after}"
        )

        # Total (L0 + L1) should be roughly the same as L0 before.
        total_after = l0_after + (l1_after or 0)
        ratio = total_after / max(l0_before, 1)
        self.logger.info(
            f"Total after reconciliation: {total_after}, ratio vs before: {ratio:.2f}"
        )
        assert 0.5 < ratio < 2.0, (
            f"Total size changed too much: before={l0_before}, "
            f"after={total_after} (L0={l0_after} + L1={l1_after}), "
            f"ratio={ratio:.2f}"
        )

        # DescribeLogDirs should reflect the combined L0 + L1 size.
        dld_size_after = self._get_describe_log_dirs_size(topic, partition)
        dld_ratio = dld_size_after / max(total_after, 1)
        self.logger.info(
            f"DescribeLogDirs after reconciliation: {dld_size_after}, "
            f"L0+L1={total_after}, ratio={dld_ratio:.2f}"
        )
        assert 0.5 < dld_ratio < 2.0, (
            f"DescribeLogDirs size diverged from L0+L1: "
            f"dld={dld_size_after}, L0+L1={total_after}, ratio={dld_ratio:.2f}"
        )

        # --- Phase 5: L1 estimator survives restart after reconciliation ---

        self.logger.info(
            "Phase 5: Verifying estimator after restart post-reconciliation"
        )
        leader_id = self.admin_v1.await_stable_leader(
            topic=topic,
            partition=partition,
            namespace="kafka",
            timeout_s=30,
            backoff_s=2,
        )
        leader_node = self.redpanda.get_node(leader_id)
        self._restart_node(leader_node)

        # L1 size should persist across restarts. After full reconciliation
        # L1 holds the actual data, so use the same lower bound.
        ct_utils.wait_until_l1_partition_size(
            self.admin,
            topic,
            partition,
            lambda size: size >= min_expected_bytes,
            timeout_sec=60,
        )
        l1_after_restart = self._get_l1_size(topic, partition)
        self.logger.info(
            f"L1 size after restart post-reconciliation: {l1_after_restart}"
        )
        assert l1_after_restart is not None and l1_after_restart >= min_expected_bytes

        # --- Phase 6: Leadership transfer after reconciliation ---

        self.logger.info(
            "Phase 6: Verifying estimator after leadership transfer post-reconciliation"
        )
        self._transfer_leadership(topic, partition)

        l1_after_transfer = self._get_l1_size(topic, partition)
        self.logger.info(
            f"L1 size after transfer post-reconciliation: {l1_after_transfer}"
        )
        assert l1_after_transfer is not None and l1_after_transfer >= min_expected_bytes
