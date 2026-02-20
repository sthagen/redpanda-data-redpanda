# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
from typing import TypeAlias, cast


from rptest.clients.admin.v2 import Admin, l0_gc_pb, ntp_pb
from rptest.context.cloud_storage import CloudStorageType
from rptest.services.kgo_repeater_service import repeater_traffic
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from connectrpc.errors import ConnectError
from ducktape.cluster.cluster import ClusterNode
from ducktape.errors import TimeoutError
from ducktape.tests.test import TestContext
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    SISettings,
    get_cloud_storage_type,
    CLOUD_TOPICS_CONFIG_STR,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception


class CloudTopicsL0GCTestBase(RedpandaTest):
    def __init__(
        self, test_context: TestContext, housekeeping_interval_ms: int | None = None
    ):
        self.test_context = test_context
        si_settings = SISettings(
            test_context=test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            cloud_storage_housekeeping_interval_ms=housekeeping_interval_ms
            if housekeeping_interval_ms is not None
            else 1000,
            fast_uploads=True,
        )
        extra_rp_conf = {
            CLOUD_TOPICS_CONFIG_STR: True,
            "cloud_topics_reconciliation_min_interval": 2000,
            "cloud_topics_reconciliation_max_interval": 2000,
            "cloud_topics_epoch_service_epoch_increment_interval": 5000,
            "cloud_topics_epoch_service_local_epoch_cache_duration": 5000,
            "cloud_topics_short_term_gc_minimum_object_age": 10000,
            "cloud_topics_short_term_gc_interval": 2000,
            "cloud_topics_short_term_gc_backoff_interval": 10000,
        }
        super().__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
            si_settings=si_settings,
        )

    def create_topics(self, topics: list[TopicSpec]):
        rpk = RpkTool(self.redpanda)
        for spec in topics:
            rpk.create_topic(
                spec.name,
                spec.partition_count,
                spec.replication_factor,
                config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD},
            )

    def get_num_objects_deleted(self, nodes: list[ClusterNode] | None = None):
        samples = self.redpanda.metrics_sample(
            "vectorized_cloud_topics_l0_gc_objects_deleted_total",
            nodes=nodes,
        )
        self.logger.info(samples)
        if samples is not None and samples.samples:
            deleted_total = int(sum(s.value for s in samples.samples))
            self.logger.debug(f"{deleted_total=}")
            return deleted_total
        return 0

    def produce_some(self, topics: list[str], n: int = 300):
        with repeater_traffic(
            context=self.test_context,
            redpanda=self.redpanda,
            topics=topics,
            msg_size=1024,
            rate_limit_bps=2 * 1024 * 1024,
            workers=1,
        ) as repeater:
            repeater.await_group_ready()
            repeater.await_progress(n, timeout_sec=90)


class CloudTopicsL0GCTest(CloudTopicsL0GCTestBase):
    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_l0_gc(self, cloud_storage_type: CloudStorageType):
        self.topics = [TopicSpec(partition_count=2)]
        self.create_topics(self.topics)
        self.produce_some(topics=[spec.name for spec in self.topics])

        # TODO: we are only checking that deletes are happening here (and should
        # also be happening in parallel with the repeater's fetch/produce
        # workload), but we do want to add tests that check constraints
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=30,
            backoff_sec=5,
            retry_on_exc=True,
        )

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_idle_housekeeping(self, cloud_storage_type: CloudStorageType):
        self.topics = [
            TopicSpec(partition_count=2),
            TopicSpec(
                # more partitions to show per-partition epoch bump
                partition_count=4
            ),
        ]
        self.create_topics(self.topics)
        self.logger.debug(
            "Produce to only one topic, so only the housekeeping loop can progress the max collectible epoch"
        )
        self.produce_some(topics=[self.topics[0].name])

        self.logger.debug(
            f"GC should still make progress because the housekeeping loop kicks in and bumps the epoch on each {self.topics[1].name} partition"
        )
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=30,
            backoff_sec=5,
            retry_on_exc=True,
        )


GcStatus: TypeAlias = l0_gc_pb.Status
StatusReport: TypeAlias = dict[int, dict[int, GcStatus] | str]
EpochInfo: TypeAlias = l0_gc_pb.EpochInfo
EpochReport: TypeAlias = dict[str, dict[int, l0_gc_pb.EpochInfo | str]]


class CloudTopicsL0GCAdminTest(CloudTopicsL0GCTestBase):
    """
    Integration: Admin API rpcs for starting and stopping level zero garbage collection.
    """

    def __init__(self, test_context: TestContext):
        # Use a long housekeeping interval so that the housekeeper does not
        # auto-advance epochs during the test; we want to observe the effect
        # of manually bumping a specific partition's epoch via Admin rpc.
        super().__init__(
            test_context=test_context, housekeeping_interval_ms=10 * 60 * 60 * 1000
        )

    @property
    def l0_gc_client(self):
        return Admin(self.redpanda).l0_gc()

    def gc_get_status(self, node: int | None = None) -> StatusReport:
        response = self.l0_gc_client.get_status(l0_gc_pb.GetStatusRequest(node_id=node))
        assert response is not None, "GetStatusResponse should not be None"
        expected_nodes = len(self.redpanda.nodes) if node is None else 1
        assert len(response.nodes) == expected_nodes, (
            f"{len(response.nodes)=} != {expected_nodes=}"
        )
        return {
            n.node_id: (
                {s.shard_id: cast(GcStatus, s.status) for s in n.shards}
                if n.error == ""
                else n.error
            )
            for n in response.nodes
        }

    def gc_pause(self, node: int | None = None) -> dict[int, str]:
        self.logger.debug(
            f"Pause L0 Garbage Collection {'clusterwide' if node is None else f'Node {node}'}"
        )
        response = self.l0_gc_client.pause(l0_gc_pb.PauseRequest(node_id=node))
        assert response is not None, "PauseResponse should not be None"
        expected_nodes = len(self.redpanda.nodes) if node is None else 1
        assert len(response.results) == expected_nodes, (
            f"{len(response.results)=} != {expected_nodes=}"
        )
        return {r.node_id: r.error for r in response.results if r.error}

    def gc_start(self, node: int | None = None) -> dict[int, str]:
        self.logger.debug(
            f"Start L0 Garbage Collection {'clusterwide' if node is None else f'Node {node}'}"
        )
        response = self.l0_gc_client.start(l0_gc_pb.StartRequest(node_id=node))
        assert response is not None, "StartResponse should not be None"
        expected_nodes = len(self.redpanda.nodes) if node is None else 1
        assert len(response.results) == expected_nodes, (
            f"{len(response.results)=} != {expected_nodes=}"
        )
        return {r.node_id: r.error for r in response.results if r.error}

    def gc_advance_epoch(self, topic: str, partition: int, new_epoch: int) -> EpochInfo:
        self.logger.debug(f"Advance epoch for '{topic}/{partition}'")
        response = self.l0_gc_client.advance_epoch(
            l0_gc_pb.AdvanceEpochRequest(
                partition=ntp_pb.TopicPartition(topic=topic, partition=partition),
                new_epoch=new_epoch,
            )
        )
        assert response is not None, "AdvanceEpochResponse should not be None"
        return response.epoch

    def gc_get_epoch_info(
        self,
        topic_partitions: list[tuple[str, int]] | None = None,
    ) -> EpochReport:
        if topic_partitions is None:
            topic_partitions = [
                (t.name, i) for t in self.topics for i in range(0, t.partition_count)
            ]
        self.logger.debug(f"Get epoch info for {topic_partitions=}")
        result: dict[str, dict[int, l0_gc_pb.EpochInfo | str]] = {}
        for topic, partition in topic_partitions:
            if topic not in result:
                result[topic] = {}
            try:
                response = self.l0_gc_client.get_epoch_info(
                    l0_gc_pb.GetEpochInfoRequest(
                        partition=ntp_pb.TopicPartition(
                            topic=topic, partition=partition
                        )
                    )
                )
                result[topic][partition] = response.epoch
            except Exception as e:
                result[topic][partition] = str(e)
        return result

    def check_statuses(
        self,
        report: StatusReport,
        nodes: list[int] | None = None,
        status: GcStatus.ValueType | None = None,
        error: str | None = None,
        strict: bool = True,
    ):
        assert status is not None or error is not None, (
            "check_statuses usage: Set expected status or expected error"
        )
        if nodes is None or nodes == []:
            nodes = [self.redpanda.node_id(n) for n in self.redpanda.nodes]
        assert not strict or len(report) == len(nodes), (
            f"Expected report for exactly {nodes=}, got {report=}"
        )

        for node_id in nodes:
            assert node_id in report, f"Expected status for {node_id=}"
            shards = report[node_id]
            if error is not None:
                assert isinstance(shards, str), (
                    f"{node_id}: Expected error got {shards=}"
                )
                assert error in shards, f"{node_id=} Unexpected error body '{shards}'"
            else:
                assert isinstance(shards, dict), (
                    f"{node_id=}: Unexpected error '{shards}'"
                )
                assert len(shards) > 0, f"{node_id=}: Report unexpectedly empty"
                assert all(s == status for _, s in shards.items()), (
                    f"Expected {status=} on {node_id=}: {shards=}"
                )

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_get_status(self, cloud_storage_type: CloudStorageType):
        self.logger.debug("Clusterwide status")
        statuses = self.gc_get_status()
        self.check_statuses(statuses, status=GcStatus.L0_GC_STATUS_RUNNING)

        target_node = self.redpanda.nodes[2]
        target_node_id = self.redpanda.node_id(target_node)

        self.logger.debug("Single-node status")
        statuses = self.gc_get_status(target_node_id)
        self.check_statuses(
            statuses, nodes=[target_node_id], status=GcStatus.L0_GC_STATUS_RUNNING
        )

        self.logger.debug("Kill a node and check partial failure")
        self.redpanda.stop_node(target_node, timeout=30)
        statuses = self.gc_get_status()
        self.check_statuses(
            statuses,
            nodes=[target_node_id],
            error="(Service unavailable)",
            strict=False,
        )

        self.check_statuses(
            statuses,
            nodes=[
                self.redpanda.node_id(n)
                for n in self.redpanda.nodes
                if n.name != target_node.name
            ],
            status=GcStatus.L0_GC_STATUS_RUNNING,
            strict=False,
        )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_basic_pause_unpause(self, cloud_storage_type: CloudStorageType):
        self.topics = [
            TopicSpec(partition_count=2),
            TopicSpec(partition_count=2),
        ]
        self.create_topics(self.topics)
        self.logger.debug("Produce some")
        self.produce_some(topics=[spec.name for spec in self.topics])

        self.check_statuses(self.gc_get_status(), status=GcStatus.L0_GC_STATUS_RUNNING)

        self.logger.debug("Wait until we've deleted something...")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=30,
            backoff_sec=5,
            retry_on_exc=True,
        )

        errs = self.gc_pause()
        assert len(errs) == 0, f"Unexpected errors pausing GC: {errs=}"
        self.check_statuses(self.gc_get_status(), status=GcStatus.L0_GC_STATUS_PAUSED)

        n_deleted = self.get_num_objects_deleted()
        self.logger.debug(f"GC should be stopped now, so we won't exceed {n_deleted=}")
        with expect_exception(TimeoutError, lambda _: True):
            wait_until(
                lambda: self.get_num_objects_deleted() > n_deleted,
                timeout_sec=30,
                backoff_sec=5,
                retry_on_exc=True,
            )

        self.logger.debug(
            "Re-start garbage collection. We should see the deleted object count ticking up."
        )
        errs = self.gc_start()
        assert len(errs) == 0, f"Unexpected errors restarting GC: {errs=}"
        self.check_statuses(self.gc_get_status(), status=GcStatus.L0_GC_STATUS_RUNNING)

        wait_until(
            lambda: self.get_num_objects_deleted() > n_deleted,
            timeout_sec=30,
            backoff_sec=5,
            retry_on_exc=True,
        )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_single_node_pause_unpause(self, cloud_storage_type: CloudStorageType):
        self.topics = [TopicSpec(partition_count=2)]
        self.create_topics(self.topics)

        pause_node = self.redpanda.nodes[0]
        pause_node_id = self.redpanda.node_id(pause_node)

        self.logger.debug(f"Pause GC on {pause_node.name} and produce some records")
        errs = self.gc_pause(pause_node_id)
        assert len(errs) == 0, (
            f"Unexpected error pausing GC on {pause_node.name}: {errs=}"
        )
        self.check_statuses(
            self.gc_get_status(node=pause_node_id),
            nodes=[pause_node_id],
            status=GcStatus.L0_GC_STATUS_PAUSED,
        )
        self.check_statuses(
            self.gc_get_status(),
            nodes=[self.redpanda.node_id(n) for n in self.redpanda.nodes[1:]],
            status=GcStatus.L0_GC_STATUS_RUNNING,
            strict=False,
        )

        self.produce_some(topics=[spec.name for spec in self.topics])

        self.logger.debug("Wait for GC to kick in")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=30,
            backoff_sec=5,
            retry_on_exc=True,
        )

        self.logger.debug(
            f"Confirm that we're not deleting any objects on {pause_node.name}"
        )
        with expect_exception(TimeoutError, lambda e: True):
            wait_until(
                lambda: self.get_num_objects_deleted(nodes=[pause_node]) > 0,
                timeout_sec=15,
                backoff_sec=3,
                retry_on_exc=True,
            )

        self.logger.debug(f"Now unpause {pause_node.name} and wait for some deletes")
        errs = self.gc_start(pause_node_id)
        assert len(errs) == 0, (
            f"Unexpected error re-starting GC on {pause_node.name}: {errs=}"
        )
        self.check_statuses(self.gc_get_status(), status=GcStatus.L0_GC_STATUS_RUNNING)

        wait_until(
            lambda: self.get_num_objects_deleted(nodes=[pause_node]) > 0,
            timeout_sec=30,
            backoff_sec=3,
            retry_on_exc=True,
        )

    @cluster(num_nodes=1)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_not_found(self, cloud_storage_type: CloudStorageType):
        nonexistent_node_id: int = 23
        with expect_exception(ConnectError, lambda e: "23 not found" in str(e)):
            self.gc_start(nonexistent_node_id)
        with expect_exception(ConnectError, lambda e: "23 not found" in str(e)):
            self.gc_pause(nonexistent_node_id)

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_partial_failure(self, cloud_storage_type: CloudStorageType):
        node_to_kill = self.redpanda.nodes[1]
        node_to_kill_id = self.redpanda.node_id(node_to_kill)

        self.logger.debug(f"Check that GC admin API is up and stop {node_to_kill.name}")
        errs = self.gc_start()
        assert len(errs) == 0, f"{errs=}"
        self.redpanda.stop_node(node_to_kill, timeout=30)

        self.logger.debug(
            f"Try to pause GC clusterwide. Only {node_to_kill.name} ({node_to_kill_id})"
            "should report an error."
        )
        errs = self.gc_pause()
        assert len(errs) == 1, f"Expected 1 error, got {errs=}"
        assert node_to_kill_id in errs, f"Unexpected error {errs=}"
        assert "(Service unavailable)" in errs[node_to_kill_id], (
            f"Unexpected error {errs=}"
        )

        self.logger.debug(f"Restart {node_to_kill.name} and pause GC there")
        self.redpanda.start_node(
            node_to_kill, timeout=30, node_id_override=node_to_kill_id
        )
        errs = self.gc_pause(node_to_kill_id)
        assert len(errs) == 0, "Unexpected errors: {errs=}"

    def _epoch_report_to_str(self, epochs: EpochReport, indent: int = 1) -> str:
        def epoch_info_to_dict(info: l0_gc_pb.EpochInfo) -> dict[str, int]:
            return {
                "estimated_inactive_epoch": info.estimated_inactive_epoch,
                "max_applied_epoch": info.max_applied_epoch,
                "last_reconciled_log_offset": info.last_reconciled_log_offset,
                "current_epoch_window_offset": info.current_epoch_window_offset,
            }

        serializable = {
            t: {
                p: (epoch_info_to_dict(e) if isinstance(e, l0_gc_pb.EpochInfo) else e)
                for p, e in ps.items()
            }
            for t, ps in epochs.items()
        }
        return json.dumps(serializable, indent=indent)

    def check_epochs(
        self, epochs: EpochReport, active_topics: list[str], stalled_topics: list[str]
    ):
        self.logger.debug(self._epoch_report_to_str(epochs))
        for t in self.topics:
            assert t.name in epochs, f"Expected {t.name=} got {epochs=}"
            ps = epochs[t.name]
            assert all(p in ps for p in range(0, t.partition_count)), (
                f"Expected partitions [0..{t.partition_count}] got {ps=}"
            )
            if t.name in active_topics:
                # active topics should have EpochInfo with positive inactive epoch
                assert all(
                    isinstance(e, l0_gc_pb.EpochInfo) and e.estimated_inactive_epoch > 0
                    for _, e in ps.items()
                ), f"Expected EpochInfo with positive epochs for {t.name=}"
            elif t.name in stalled_topics:
                # Stalled topics should have EpochInfo with nonexistent estimated_inactive_epoch
                # since no data has been reconciled
                assert all(
                    isinstance(e, l0_gc_pb.EpochInfo) and e.estimated_inactive_epoch < 0
                    for _, e in ps.items()
                ), f"Expected EpochInfo with min epoch for stalled {t.name=}"
            else:
                assert False, f"{t.name} not in {(active_topics + stalled_topics)=}"

        return True

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_advance_epoch(self, cloud_storage_type: CloudStorageType):
        self.topics = [
            TopicSpec(partition_count=1),
            TopicSpec(partition_count=1),
        ]
        self.create_topics(self.topics)
        produce_topics = [t.name for t in self.topics[0:1]]
        stalled_topic = self.topics[1].name
        assert stalled_topic not in produce_topics, (
            f"{stalled_topic=} in {produce_topics=}"
        )

        self.produce_some(topics=produce_topics, n=200)

        # since we've produced nothing to stalled_topic, that partition will block GC
        # from progressing. this block checks that the epoch report has the right shape
        # and that it shows errors for all the partitions of stalled_topic
        # NOTE: wait_until here so we don't race against reconciliation of produce_topics data.
        # "monotonic epoch" invariant guarantees that epoch(stalled_topic) didn't advance then return to 0.
        wait_until(
            lambda: self.check_epochs(
                self.gc_get_epoch_info(), produce_topics, [stalled_topic]
            ),
            timeout_sec=15,
            backoff_sec=3,
            retry_on_exc=True,
        )
        epochs = self.gc_get_epoch_info()
        self.check_epochs(epochs, produce_topics, [stalled_topic])

        self.logger.debug(
            f"Check that GC doesn't progress despite reconciliation making progress on {produce_topics=}"
        )
        with expect_exception(TimeoutError, lambda _: True):
            wait_until(
                lambda: self.get_num_objects_deleted() > 0,
                timeout_sec=30,
                backoff_sec=5,
                retry_on_exc=True,
            )

        epochs = self.gc_get_epoch_info()
        self.check_epochs(epochs, produce_topics, [stalled_topic])

        self.logger.debug(
            "Force the stalled topic's epoch up to the inactive epoch of the active topic. This should unstick GC"
        )
        target_epoch = cast(
            EpochInfo, epochs[produce_topics[0]][0]
        ).estimated_inactive_epoch

        new_epoch = self.gc_advance_epoch(
            topic=stalled_topic,
            partition=0,
            new_epoch=target_epoch,
        )
        self.logger.debug(f"New EpochInfo for {stalled_topic=}: {new_epoch}")

        gc_epoch = new_epoch.estimated_inactive_epoch
        max_epoch = new_epoch.max_applied_epoch
        epoch_offset = new_epoch.current_epoch_window_offset
        lrlo = new_epoch.last_reconciled_log_offset

        assert gc_epoch == (target_epoch - 1) and gc_epoch < max_epoch, (
            f"Expected {gc_epoch=} == {target_epoch-1=}"
        )
        assert epoch_offset == lrlo and epoch_offset > 0, (
            f"Expected 0 < ({epoch_offset=}) == ({lrlo=}) on {stalled_topic=}"
        )

        self.logger.debug(
            f"Now that we've advanced {stalled_topic=} epoch window, GC can make progress"
        )
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=30,
            backoff_sec=5,
            retry_on_exc=True,
        )


class CloudTopicsL0GCMetricsTest(CloudTopicsL0GCTestBase):
    """
    Integration: Basic semantics for some GC metrics
    """

    def _get_int_metric(self, name: str) -> list[int]:
        samples = self.redpanda.metrics_sample(name)
        self.logger.debug(samples)
        assert samples is not None, "samples unexpectedly None"
        vals = [int(s.value) for s in samples.samples]
        return vals

    def get_epoch_lag(self) -> list[int]:
        return self._get_int_metric("vectorized_cloud_topics_l0_gc_epoch_lag")

    def get_max_deleted_epoch(self) -> list[int]:
        return self._get_int_metric("vectorized_cloud_topics_l0_gc_max_deleted_epoch")

    def get_collection_rounds(self) -> list[int]:
        return self._get_int_metric(
            "vectorized_cloud_topics_l0_gc_collection_rounds_total"
        )

    def get_objects_listed(self) -> list[int]:
        return self._get_int_metric(
            "vectorized_cloud_topics_l0_gc_objects_listed_total"
        )

    def get_delete_requests(self) -> list[int]:
        return self._get_int_metric(
            "vectorized_cloud_topics_l0_gc_delete_requests_total"
        )

    @cluster(num_nodes=5)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_gc_metrics(self, cloud_storage_type: CloudStorageType):
        self.topics = [TopicSpec(partition_count=1)]
        self.create_topics(self.topics)
        self.produce_some(topics=[spec.name for spec in self.topics], n=100)

        # GC should be completing collection rounds and scanning objects
        wait_until(
            lambda: sum(self.get_collection_rounds()) > 0,
            timeout_sec=30,
            backoff_sec=2,
            retry_on_exc=True,
        )
        self.logger.info(
            f"GC running, collection_rounds={self.get_collection_rounds()}"
        )

        wait_until(
            lambda: sum(self.get_objects_listed()) > 0,
            timeout_sec=30,
            backoff_sec=2,
            retry_on_exc=True,
        )
        self.logger.info(
            f"GC scanning objects, objects_listed={self.get_objects_listed()}"
        )

        # Wait for GC to start deleting - max_deleted_epoch should become >= 0 on some shard
        wait_until(
            lambda: any(e >= 0 for e in self.get_max_deleted_epoch()),
            timeout_sec=30,
            backoff_sec=2,
            retry_on_exc=True,
        )

        initial_max_deleted = max(self.get_max_deleted_epoch())
        self.logger.info(f"GC started deleting, {initial_max_deleted=}")

        # Produce more to create new epochs while GC is running
        self.produce_some(topics=[spec.name for spec in self.topics], n=100)

        # max_deleted_epoch should increase as GC makes progress
        wait_until(
            lambda: max(self.get_max_deleted_epoch()) > initial_max_deleted,
            timeout_sec=30,
            backoff_sec=2,
            retry_on_exc=True,
        )
        self.logger.info(
            f"GC progressing, max_deleted_epoch increased to "
            f"{max(self.get_max_deleted_epoch())}"
        )

        # Verify batching: delete requests should be fewer than objects deleted,
        # indicating multiple objects per batch request.
        total_deleted = self.get_num_objects_deleted()
        total_delete_requests = sum(self.get_delete_requests())
        self.logger.info(f"Batching check: {total_deleted=}, {total_delete_requests=}")
        assert total_delete_requests > 0, "Expected at least one delete request"
        assert total_deleted > total_delete_requests, (
            f"Expected batching: {total_deleted=} should be greater than "
            f"{total_delete_requests=}"
        )
