# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
"""
Scale tests to verify that the leader balancer doesn't cause excessive
leadership transfers during maintenance mode, decommission/recommission,
and node restart operations.

These tests validate that leadership transfers don't exceed expected bounds,
which helps detect issues like excessive leadership transfers.
"""

from abc import abstractmethod, ABC
from dataclasses import dataclass
import random
from collections import Counter
from typing import Any, TypedDict

from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, LoggingConfig
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import repeat_check, wait_until_result
from rptest.utils.node_operations import NodeDecommissionWaiter

from rptest.clients.types import TopicSpec


@dataclass
class TransferCounts:
    main: int
    followup: int

    def total(self) -> int:
        return self.main + self.followup


@dataclass
class TransferEntry:
    context: str
    actual: TransferCounts
    expected: int
    leeway: int


class IterationStats(TypedDict):
    node: str
    transfers: list[TransferEntry]


class ExcessiveLeadershipTransfersTestBase(RedpandaTest, ABC):
    """
    Base class for leadership transfer tests.

    Provides common functionality for tracking and verifying leadership
    transfers during various cluster operations.
    """

    # Leader balancer timing configuration
    LEADER_BALANCER_PERIOD_MS = 10000  # 10 seconds
    # Timeout for waiting for cluster to stabilize
    STABILIZE_TIMEOUT_SEC = 300
    # Timeout for leader balancer to rebalance
    LEADER_BALANCE_TIMEOUT_SEC = 600
    # Polling interval for leadership counts
    POLL_INTERVAL_SEC = 1

    # Topic configuration
    TOPIC_NAME_PREFIX = "leadership-transfer-test"
    TOPICS_COUNT = 30
    PARTITION_COUNT = 30
    REPLICATION_FACTOR = 3

    # Leader balancer mode identifiers
    MODE_CALIBRATED = "calibrated"
    MODE_RANDOM = "random"

    ITERATIONS_PER_MODE = 5

    # Number of nodes in the cluster
    NODES = 9

    TEST_NAME: str

    def __init__(
        self,
        test_context: TestContext,
        num_brokers: int = 3,
        *args: Any,
        **kwargs: Any,
    ):
        kwargs.setdefault("extra_rp_conf", {}).update(
            {
                # Enable leader balancer - this is what we're testing
                "enable_leader_balancer": True,
                # Fast leader balancer for quicker test iterations
                "leader_balancer_idle_timeout": self.LEADER_BALANCER_PERIOD_MS,
                "leader_balancer_mute_timeout": self.LEADER_BALANCER_PERIOD_MS,
            }
        )

        kwargs["log_config"] = LoggingConfig(
            "info",
            logger_levels={
                "cluster": "trace",
                "storage": "warn",
                "storage-gc": "warn",
                "raft": "debug",
            },
        )

        super().__init__(
            test_context,
            num_brokers=num_brokers,
            *args,
            **kwargs,
        )

        self.admin = Admin(self.redpanda)

    def setUp(self):
        super().setUp()
        self.redpanda.set_expected_controller_records(None)
        self._create_topics()
        self._wait_for_all_leaders_elected()
        self._wait_for_leadership_stable()

    @abstractmethod
    def _run_iteration(self) -> IterationStats:
        pass

    def _create_topics(self):
        for i in range(self.TOPICS_COUNT):
            self.client().create_topic(
                TopicSpec(
                    name=f"{self.TOPIC_NAME_PREFIX}-{i}",
                    partition_count=self.PARTITION_COUNT,
                    replication_factor=self.REPLICATION_FACTOR,
                )
            )

    def _get_test_partitions(self) -> list[dict[str, Any]]:
        """Get all partitions for test topics using admin API."""
        return [
            p
            for p in self.admin.get_cluster_partitions()
            if p["topic"].startswith(self.TOPIC_NAME_PREFIX)
        ]

    def _get_partitions_with_leaders(self) -> list[dict[str, Any]]:
        """Get partitions that have valid leaders."""
        return [p for p in self._get_test_partitions() if p.get("leader_id", 0) > 0]

    def _get_leadership_distribution(self) -> Counter[int]:
        """Get mapping of node_id -> leader_count."""
        return Counter(p["leader_id"] for p in self._get_partitions_with_leaders())

    def _get_total_leaders_on_node(self, node_id: int) -> int:
        """Get the total number of leaders on a specific node."""
        return self._get_leadership_distribution()[node_id]

    def _total_partition_count(self) -> int:
        """Get the total number of partitions across all test topics."""
        return self.TOPICS_COUNT * self.PARTITION_COUNT

    def _wait_for_all_leaders_elected(self):
        """Wait until all partitions have elected leaders."""
        expected = self._total_partition_count()

        def all_leaders_elected():
            total = len(self._get_partitions_with_leaders())
            self.logger.debug(f"Total leaders elected: {total}/{expected}")
            return total == expected

        wait_until(
            all_leaders_elected,
            timeout_sec=self.STABILIZE_TIMEOUT_SEC,
            backoff_sec=5,
            err_msg=f"Not all {expected} partitions have leaders",
        )

    def _get_replicas_on_node(self, node_id: int) -> int:
        """Get the number of replicas on a specific node."""
        return sum(
            1
            for p in self._get_test_partitions()
            for r in p["replicas"]
            if r["node_id"] == node_id
        )

    def _wait_for_node_to_have_leaders(self, node_id: int, min_leaders: int = 5):
        """
        Wait for a node to have at least min_leaders leaders.
        This ensures the leader balancer has started rebalancing to the node
        after leader_activation_delay expires.
        See: src/v/cluster/scheduling/leader_balancer.h::leader_activation_delay
        """
        self.logger.info(
            f"Waiting for node {node_id} to have at least {min_leaders} leaders"
        )
        wait_until(
            lambda: self._get_total_leaders_on_node(node_id) >= min_leaders,
            timeout_sec=120,
            backoff_sec=5,
            err_msg=f"Node {node_id} did not get {min_leaders} leaders",
        )
        leaders = self._get_total_leaders_on_node(node_id)
        self.logger.info(f"Node {node_id} now has {leaders} leaders")

    def _get_leadership_changes_metric(
        self, nodes: list[ClusterNode] | None = None
    ) -> int:
        """
        Get the total successful leader transfers from the leader balancer metric.
        This metric is node-level and persistent (not lost when partitions move).
        Only queries running nodes.
        """
        metric = self.redpanda.metrics_sample(
            "vectorized_leader_balancer_leader_transfer_succeeded",
            nodes=self.redpanda.started_nodes() if nodes is None else nodes,
        )
        assert metric, "leader_transfer_succeeded metric not found"
        ret = int(sum(s.value for s in metric.samples))
        self.logger.debug(f"leader_transfer_succeeded metric value: {ret}")
        return ret

    def _wait_for_leadership_stable(self, stability_checks: int = 7) -> int:
        """
        Wait for leadership to stabilize (metric unchanged for stability_checks consecutive checks).
        Uses the leader balancer's leader_transfer_succeeded metric to detect changes.
        Returns the final metric value.
        """

        @repeat_check(stability_checks, require_same=True)
        def is_stable():
            count = self._get_leadership_changes_metric()
            self.logger.debug(f"Leadership changes metric: {count}")
            return True, count

        final_count = wait_until_result(
            is_stable,
            timeout_sec=self.LEADER_BALANCE_TIMEOUT_SEC,
            backoff_sec=self.POLL_INTERVAL_SEC,
            err_msg="Leadership did not stabilize",
        )
        self.logger.info(f"Leadership stable for {stability_checks} consecutive checks")
        return final_count

    def _do_track_transfers_until_stable(self, initial_count: int) -> int:
        """
        Wait for leadership to stabilize and return the total number of transfers
        since initial_count.
        """
        final_count = self._wait_for_leadership_stable(
            self.LEADER_BALANCER_PERIOD_MS // 1000 + 5
        )
        total_transfers = final_count - initial_count
        self.logger.info(f"Total leadership transfers: {total_transfers}")
        return total_transfers

    def _track_transfers_until_stable(self, initial_count: int) -> TransferCounts:
        """
        Wrapper around _do_track_transfers_until_stable to also verify no moves after switching to random.
        """
        return TransferCounts(
            main=self._do_track_transfers_until_stable(initial_count),
            followup=self._followup_no_moves_after_switching_to_random(),
        )

    def _set_leader_balancer_mode(self, mode: str):
        """Set the leader balancer mode via cluster config."""
        self.logger.info(f"Setting leader_balancer_mode to '{mode}'")
        self.redpanda.set_cluster_config(
            {"leader_balancer_mode": mode}, tolerate_stopped_nodes=True
        )
        self._current_balancer_mode = mode

    def _followup_no_moves_after_switching_to_random(self) -> int:
        """
        If not already, switch to random mode and verify no leadership moves occur.
        This validates that the calibrated strategy reached an optimal state.
        """
        if self._current_balancer_mode == self.MODE_RANDOM:
            return 0

        initial_metric = self._get_leadership_changes_metric()
        self._set_leader_balancer_mode(self.MODE_RANDOM)

        # Wait for stability and check no moves were made
        moves_after_switch = self._do_track_transfers_until_stable(initial_metric)

        if moves_after_switch > 0:
            self.logger.warning(
                f"Unexpected moves after switching to random: {moves_after_switch}"
            )
        else:
            self.logger.info(
                "No moves after switching to random - calibrated did the full job"
            )

        self._set_leader_balancer_mode(self.MODE_CALIBRATED)
        return moves_after_switch

    def _run_iterations_in_mode(self, mode: str) -> list[IterationStats]:
        stats: list[IterationStats] = []
        for i in range(self.ITERATIONS_PER_MODE):
            self.logger.info(
                f"=== {self.TEST_NAME} {mode} iteration {i + 1}/{self.ITERATIONS_PER_MODE} ==="
            )
            self._set_leader_balancer_mode(mode)
            stats.append(self._run_iteration())
            self.logger.info(f"=== {mode} iteration {i + 1} completed ===")
        self._print_avg(mode, stats)
        return stats

    def _run_calibrated(self):
        """Run test iterations with calibrated strategy."""

        calibrated_stats = self._run_iterations_in_mode(self.MODE_CALIBRATED)
        self._assert_all_transfers_bounded(self.MODE_CALIBRATED, calibrated_stats)

    def _run_random(self):
        """Run test iterations with random strategy."""
        self._run_iterations_in_mode(self.MODE_RANDOM)

    def _print_avg(self, mode: str, stats_list: list[IterationStats]):
        """
        Print summary statistics for a set of iterations.
        """
        self.logger.info(
            f"{self.TEST_NAME} - {mode} iterations summary ({len(stats_list)} iterations):"
        )

        transfer_totals: Counter[str] = Counter()
        for i, stats in enumerate(stats_list):
            self.logger.info(f"  Iteration {i + 1}:")
            self.logger.info(f"    node: {stats['node']}")
            for t in stats["transfers"]:
                actual_total = t.actual.total()
                self.logger.info(
                    f"    {t.context}: {actual_total=}, expected={t.expected}, leeway={t.leeway}"
                )
                transfer_totals[t.context] += actual_total

        # Print averages with full context for easy grepping
        n = len(stats_list)
        self.logger.info(f"  --- Averages ({n} iterations) ---")
        for context, actual_sum in transfer_totals.items():
            self.logger.info(
                f"{self.TEST_NAME} - {mode} {context}: {n=}, avg_moves={actual_sum / n:.1f}"
            )

    def _assert_all_transfers_bounded(
        self, mode: str, stats_list: list[IterationStats]
    ):
        """
        Assert that all transfers in stats_list are within bounds.
        """
        failures: list[str] = []
        for i, stats in enumerate(stats_list):
            for t in stats["transfers"]:
                max_allowed = t.leeway * t.expected
                actual_total = t.actual.total()
                prefix = f"  {self.TEST_NAME} - {mode} iteration {i + 1} {t.context}: "
                if actual_total > max_allowed:
                    failures.append(
                        f"{prefix}{actual_total=} > {max_allowed=} (expected={t.expected}, leeway={t.leeway})"
                    )
                if t.actual.followup > 0:
                    failures.append(
                        f"{prefix}unexpected moves after switching to random: {t.actual.followup}"
                    )

        if failures:
            failure_msg = "\n".join(failures)
            assert False, f"Transfer bounds exceeded:\n{failure_msg}"


class ExcessiveLeadershipTransfersMaintenanceModeTest(
    ExcessiveLeadershipTransfersTestBase
):
    """
    Test suite to verify that the leader balancer doesn't cause excessive
    leadership transfers during maintenance mode operations.

    The leader balancer should efficiently rebalance leadership when nodes
    enter/exit maintenance mode without causing unnecessary churn.

    Each iteration:
    1. Select a random node
    2. Enable maintenance mode (drains leaders)
    3. Track leadership transfers during drain
    4. Disable maintenance mode
    5. Track transfers during rebalancing
    6. Verify transfers are bounded by leader count on the node
    """

    TEST_NAME = "MaintenanceMode"

    MAINTENANCE_TIMEOUT_SEC = 300

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(
            test_context,
            num_brokers=ExcessiveLeadershipTransfersTestBase.NODES,
            *args,
            **kwargs,
        )

    def _in_maintenance_mode(self, node: ClusterNode) -> bool:
        """Check if a node is in maintenance mode."""
        status = self.admin.maintenance_status(node)
        return status["draining"]

    def _maintenance_drain_finished(self, node: ClusterNode) -> bool:
        """Check if maintenance mode has finished draining."""
        status = self.admin.maintenance_status(node)
        return (
            all([key in status for key in ["finished", "errors", "partitions"]])
            and status["finished"]
            and not status["errors"]
        )

    def _enable_maintenance_mode(self, node: ClusterNode):
        """Enable maintenance mode on a node and wait for it to drain."""
        self.logger.info(f"Enabling maintenance mode on {node.name}")
        self.admin.maintenance_start(node)
        wait_until(
            lambda: self._in_maintenance_mode(node),
            timeout_sec=30,
            backoff_sec=5,
            err_msg=f"Node {node.name} did not enter maintenance mode",
        )

        self.logger.info(f"Waiting for {node.name} to drain leadership")
        wait_until(
            lambda: self._maintenance_drain_finished(node),
            timeout_sec=self.MAINTENANCE_TIMEOUT_SEC,
            backoff_sec=10,
            err_msg=f"Node {node.name} maintenance mode did not complete",
        )
        self.logger.info(f"Node {node.name} maintenance mode completed")

    def _disable_maintenance_mode(self, node: ClusterNode):
        """Disable maintenance mode on a node."""
        self.logger.info(f"Disabling maintenance mode on {node.name}")
        self.admin.maintenance_stop(node)

        wait_until(
            lambda: not self._in_maintenance_mode(node),
            timeout_sec=60,
            backoff_sec=5,
            err_msg=f"Node {node.name} did not exit maintenance mode",
        )
        self.logger.info(f"Node {node.name} exited maintenance mode")

    def _run_iteration(self) -> IterationStats:
        """
        Run a single maintenance mode iteration.
        Returns stats dict with iteration metrics.
        """
        # Choose a random node
        node = random.choice(self.redpanda.nodes)
        node_id = self.redpanda.node_id(node)
        self.logger.info(f"Selected node {node.name} (id={node_id})")

        # Count initial leaders on this node
        initial_leaders_on_node = self._get_total_leaders_on_node(node_id)
        self.logger.info(
            f"Node {node.name} has {initial_leaders_on_node} leaders before maintenance"
        )

        # Capture initial metric before enabling maintenance
        initial_metric = self._get_leadership_changes_metric()

        # Enable maintenance mode
        self._enable_maintenance_mode(node)

        # Track any transfers
        transfers_on_drain = self._track_transfers_until_stable(initial_metric)

        # Verify node has no leaders after maintenance
        leaders_after_drain = self._get_total_leaders_on_node(node_id)
        assert leaders_after_drain == 0, (
            f"Node {node.name} still has {leaders_after_drain} leaders after maintenance"
        )

        # Capture initial metric before disabling maintenance
        initial_metric = self._get_leadership_changes_metric()

        # Disable maintenance mode
        self._disable_maintenance_mode(node)

        # Track transfers during rebalancing
        transfers_after_restore = self._track_transfers_until_stable(initial_metric)

        final_leaders_on_node = self._get_total_leaders_on_node(node_id)
        self.logger.info(
            f"Node {node.name} has {final_leaders_on_node} leaders after maintenance"
        )

        return {
            "node": node.name,
            "transfers": [
                TransferEntry(
                    "during_drain", transfers_on_drain, initial_leaders_on_node, 3
                ),
                TransferEntry(
                    "after_restore", transfers_after_restore, final_leaders_on_node, 3
                ),
            ],
        }

    @cluster(
        num_nodes=ExcessiveLeadershipTransfersTestBase.NODES,
        log_allow_list=RESTART_LOG_ALLOW_LIST,
    )
    def test_maintenance_mode_calibrated(self):
        self._run_calibrated()

    @cluster(
        num_nodes=ExcessiveLeadershipTransfersTestBase.NODES,
        log_allow_list=RESTART_LOG_ALLOW_LIST,
    )
    def test_maintenance_mode_random(self):
        self._run_random()


class ExcessiveLeadershipTransfersDecommissionTest(
    ExcessiveLeadershipTransfersTestBase
):
    """
    Test that the leader balancer doesn't cause excessive leadership transfers
    during decommission/recommission operations beyond what's expected from
    replica movements.

    Each iteration:
    1. Select a random node to decommission
    2. Decommission the node (replicas move away)
    3. Track leadership transfers during decommission
    4. Recommission a new node (replicas rebalance to it)
    5. Track transfers during rebalancing
    6. Verify transfers are bounded by replica movements
    """

    TEST_NAME = "Decommission"

    DECOMMISSION_TIMEOUT_SEC = 600

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(
            test_context,
            num_brokers=ExcessiveLeadershipTransfersTestBase.NODES,
            *args,
            **kwargs,
        )

    def _wait_for_decommission_complete(self, node_id: int):
        waiter = NodeDecommissionWaiter(
            self.redpanda,
            node_id=node_id,
            logger=self.logger,
            progress_timeout=120,
        )
        waiter.wait_for_removal()

    def _node_is_active(self, node_id: int) -> bool:
        """Check if a node is active in the cluster."""
        return any(
            b["node_id"] == node_id and b["membership_status"] == "active"
            for b in self.admin.get_brokers()
        )

    def _run_iteration(self) -> IterationStats:
        """
        Run a single decommission/recommission iteration.
        Returns stats dict with iteration metrics.
        """
        # Choose a random node to decommission
        node = random.choice(self.redpanda.nodes)
        node_id = self.redpanda.node_id(node)
        self.logger.info(f"Selected node {node.name} (id={node_id}) for decommission")

        # Count initial state
        initial_leaders_on_node = self._get_total_leaders_on_node(node_id)
        initial_replicas_on_node = self._get_replicas_on_node(node_id)

        self.logger.info(
            f"Node {node.name} has {initial_leaders_on_node} leaders and "
            f"{initial_replicas_on_node} replicas before decommission"
        )

        # For decom we'll only compare metrics on surviving nodes
        surviving_nodes = [
            n
            for n in self.redpanda.started_nodes()
            if self.redpanda.node_id(n) != node_id
        ]
        initial_metric = self._get_leadership_changes_metric(nodes=surviving_nodes)

        # Decommission
        self.logger.info(f"Starting decommission of node {node_id}")
        self.admin.decommission_broker(node_id)
        self._wait_for_decommission_complete(node_id)
        self.logger.info(f"Stopping decommissioned node {node.name}")
        self.redpanda.stop_node(node)

        # Track leadership transfers until stable
        total_decom_transfers = self._track_transfers_until_stable(initial_metric)

        # Capture initial metric before recommissioning
        initial_metric = self._get_leadership_changes_metric()

        # Recommission: restart node with new identity
        self.logger.info(f"Restarting node {node.name} to recommission")
        self.redpanda.clean_node(node)
        self.redpanda.start_node(
            node, auto_assign_node_id=True, omit_seeds_on_idx_one=False
        )
        new_node_id = self.redpanda.node_id(node, force_refresh=True)
        self.logger.info(f"Node {node.name} rejoined with new id {new_node_id}")
        wait_until(
            lambda: self._node_is_active(new_node_id),
            timeout_sec=60,
            backoff_sec=5,
            err_msg=f"Node {node.name} did not become active after restart",
        )

        # Wait for leadership to actually rebalance to the new node.
        # This ensures leader_activation_delay has expired and balancer has acted.
        self._wait_for_node_to_have_leaders(new_node_id, min_leaders=5)

        # Wait for leadership to rebalance to the new node
        total_recom_transfers = self._track_transfers_until_stable(initial_metric)

        return {
            "node": node.name,
            "transfers": [
                TransferEntry(
                    "decommission", total_decom_transfers, initial_replicas_on_node, 1
                ),
                TransferEntry(
                    "recommission", total_recom_transfers, initial_replicas_on_node, 1
                ),
            ],
        }

    @cluster(
        num_nodes=ExcessiveLeadershipTransfersTestBase.NODES,
        log_allow_list=RESTART_LOG_ALLOW_LIST,
    )
    def test_decommission_recommission_calibrated(self):
        self._run_calibrated()

    @cluster(
        num_nodes=ExcessiveLeadershipTransfersTestBase.NODES,
        log_allow_list=RESTART_LOG_ALLOW_LIST,
    )
    def test_decommission_recommission_random(self):
        self._run_random()


class ExcessiveLeadershipTransfersRestartTest(ExcessiveLeadershipTransfersTestBase):
    """
    Test that the leader balancer doesn't cause excessive leadership transfers
    during node restart operations.

    The number of leadership transfers should be bounded by the number of
    replicas on the restarted node:
    - When node goes down: leaders move away (bounded by leaders on node)
    - When node comes back: leader balancer moves leaders back

    Each iteration:
    1. Select a random node to restart
    2. Stop the node
    3. Track leadership transfers while node is down
    4. Start the node
    5. Track transfers during rebalancing
    6. Verify transfers are bounded by replica count on the node
    """

    TEST_NAME = "Restart"

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(
            test_context,
            num_brokers=ExcessiveLeadershipTransfersTestBase.NODES,
            *args,
            **kwargs,
        )

    def _run_iteration(self) -> IterationStats:
        """
        Run a single node restart iteration.
        Returns stats dict with iteration metrics.
        """
        # Choose a random node to restart
        node = random.choice(self.redpanda.nodes)
        node_id = self.redpanda.node_id(node)
        self.logger.info(f"Selected node {node.name} (id={node_id}) for restart")

        # Count initial state
        initial_leaders_on_node = self._get_total_leaders_on_node(node_id)
        initial_replicas_on_node = self._get_replicas_on_node(node_id)

        self.logger.info(
            f"Node {node.name} has {initial_leaders_on_node} leaders and "
            f"{initial_replicas_on_node} replicas before restart"
        )

        # Capture initial metrics
        surviving_nodes = [
            n
            for n in self.redpanda.started_nodes()
            if self.redpanda.node_id(n) != node_id
        ]
        initial_metric = self._get_leadership_changes_metric(nodes=surviving_nodes)

        # Stop the node
        self.logger.info(f"Stopping node {node.name}")
        self.redpanda.stop_node(node)

        # Wait for leadership to move away from the stopped node
        self._wait_for_all_leaders_elected()

        # Track leadership changes while node is down until stable
        stop_transfers = self._track_transfers_until_stable(initial_metric)

        # Capture initial metric before starting node
        initial_metric = self._get_leadership_changes_metric()

        # Start the node again
        self.logger.info(f"Starting node {node.name}")
        self.redpanda.start_node(node)

        # Wait for leadership to actually rebalance to the restarted node.
        # This ensures leader_activation_delay has expired and balancer has acted.
        self._wait_for_node_to_have_leaders(node_id, min_leaders=5)

        # Wait for the cluster to stabilize and leader balancer to rebalance
        self._wait_for_all_leaders_elected()

        final_replicas_on_node = self._get_replicas_on_node(node_id)

        # Track transfers during rebalancing
        restart_transfers = self._track_transfers_until_stable(initial_metric)

        return {
            "node": node.name,
            "transfers": [
                TransferEntry("node_stop", stop_transfers, initial_replicas_on_node, 1),
                TransferEntry(
                    "node_restart", restart_transfers, final_replicas_on_node, 1
                ),
            ],
        }

    @cluster(
        num_nodes=ExcessiveLeadershipTransfersTestBase.NODES,
        log_allow_list=RESTART_LOG_ALLOW_LIST,
    )
    def test_restart_calibrated(self):
        self._run_calibrated()

    @cluster(
        num_nodes=ExcessiveLeadershipTransfersTestBase.NODES,
        log_allow_list=RESTART_LOG_ALLOW_LIST,
    )
    def test_restart_random(self):
        self._run_random()
