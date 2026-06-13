# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.tests.cloud_topics.e2e_test import EndToEndCloudTopicsBase
from rptest.utils.mode_checks import is_debug_mode


class LevelingStressBase(EndToEndCloudTopicsBase):
    """
    Base class for cloud topics leveling stress tests.

    Leveling rewrites runs of undersized L1 objects into fewer, larger
    objects. These tests induce heavy fragmentation (many undersized
    objects) and exercise the per-range leveling pipeline, then assert that
    leveling reclaims extents and that no records are lost.

    Fragmentation is forced via a small reconciliation object size plus a
    high undersized-ratio, so that nearly every freshly-reconciled object is
    leveling-eligible. The leveling interval is lowered so the test does not
    wait minutes between ticks.
    """

    # Override — each test creates its own topic with the partition count it
    # wants.
    topics = ()

    LEVELING_INTERVAL_MS = 2000
    MAX_CONCURRENT = 4
    MIN_EXTENT_RATIO = 0.9
    RECONCILIATION_MAX_OBJECT_SIZE = 1024 * 1024  # 1 MiB

    def __init__(
        self,
        test_context: TestContext,
        extra_rp_conf: dict[str, Any] | None = None,
    ):
        conf = {
            "cloud_topics_leveling_interval_ms": self.LEVELING_INTERVAL_MS,
            "cloud_topics_max_concurrent_leveling_jobs_per_shard": self.MAX_CONCURRENT,
            "cloud_topics_leveling_min_extent_size_ratio": self.MIN_EXTENT_RATIO,
            "cloud_topics_reconciliation_max_object_size": self.RECONCILIATION_MAX_OBJECT_SIZE,
        }
        if extra_rp_conf:
            conf.update(extra_rp_conf)

        environment = {
            "__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON",
        }
        super().__init__(
            test_context,
            extra_rp_conf=conf,
            environment=environment,
        )

    # ── Wait helpers ────────────────────────────────────────────────

    def wait_for_extents_reclaimed(
        self,
        min_reclaimed: int,
        timeout_sec: int = 360,
    ):
        """Wait until leveling has reclaimed at least `min_reclaimed` extents."""
        wait_until(
            lambda: self.get_extents_reclaimed() >= min_reclaimed,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg=lambda: (
                f"Expected >= {min_reclaimed} extents reclaimed, "
                f"got {self.get_extents_reclaimed()}"
            ),
        )

    # ── Workflow helpers ────────────────────────────────────────────

    def produce_and_wait(
        self,
        topic: str,
        msg_size: int,
        msg_count: int,
        rate_limit_bps: int | None = None,
        tolerate_failed_produce: bool = False,
    ) -> KgoVerifierProducer:
        """Produce `msg_count` messages and wait for all acks."""
        assert self.redpanda
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=msg_size,
            msg_count=msg_count,
            rate_limit_bps=rate_limit_bps,
            tolerate_failed_produce=tolerate_failed_produce,
        )
        producer.start()
        try:
            producer.wait()
        finally:
            producer.stop()
        return producer

    def consume_and_verify(
        self,
        topic: str,
        msg_size: int,
        producer: KgoVerifierProducer,
    ):
        """
        Read every record back and validate it. Leveling is a lossless
        rewrite (no deduplication), so all produced records must survive.
        """
        assert self.redpanda
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            topic,
            msg_size,
            loop=False,
            nodes=[producer.nodes[0]],
        )
        consumer.start(clean=False)
        try:
            consumer.wait(timeout_sec=120)
        finally:
            consumer.stop()


class LevelingStressHighFragmentationTest(LevelingStressBase):
    """
    Spread rate-limited writes across many partitions to generate lots of
    undersized L1 extents (the workload leveling exists to fold), then wait
    for leveling to fully converge, assert it reclaimed a meaningful number
    of extents, and verify every record survives.

    NOTE on what actually fragments: empirically, fanning writes across many
    partitions is what produces undersized extents at scale — a single
    partition reconciles into well-sized objects that are never leveled,
    regardless of produce rate. The partition count is the crux here.
    """

    TOPIC_NAME = "leveling_fragmentation_stress"
    MSG_SIZE = 4096
    PARTITIONS = 8
    RATE_LIMIT_BPS = 50 * 1024 * 1024  # 50 MB/s

    if is_debug_mode():
        msg_count = 40_000
        min_reclaimed = 5
    else:
        msg_count = 400_000
        min_reclaimed = 50

    def __init__(self, test_context: TestContext):
        super().__init__(test_context)

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        self.rpk.create_topic(
            topic=self.TOPIC_NAME,
            partitions=self.PARTITIONS,
            replicas=3,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
            },
        )

    @cluster(num_nodes=4)
    def test_high_fragmentation(self):
        self.wait_for_managed_logs()

        producer = self.produce_and_wait(
            topic=self.TOPIC_NAME,
            msg_size=self.MSG_SIZE,
            msg_count=self.msg_count,
            rate_limit_bps=self.RATE_LIMIT_BPS,
        )

        # The trickle produced many undersized objects; wait for leveling to
        # fold them and converge (reclaimed-extents counter stable + queue
        # drained).
        self.wait_for_leveling_quiesce()

        # Guard against regressing to a workload that doesn't actually
        # fragment (and therefore never exercises leveling).
        reclaimed = self.get_extents_reclaimed()
        assert reclaimed >= self.min_reclaimed, (
            f"Expected leveling to reclaim >= {self.min_reclaimed} extents, "
            f"got {reclaimed} — the workload may not be fragmenting"
        )

        self.consume_and_verify(
            topic=self.TOPIC_NAME,
            msg_size=self.MSG_SIZE,
            producer=producer,
        )


class LevelingStressWritePressureTest(LevelingStressBase):
    """
    Continuous high-throughput writes across many partitions while leveling
    runs. Verifies leveling makes forward progress (reclaims extents)
    concurrently with sustained production, rather than only converging once
    writes stop.
    """

    TOPIC_NAME = "leveling_write_pressure_stress"
    MSG_SIZE = 4096
    PARTITIONS = 8
    RATE_LIMIT_BPS = 50 * 1024 * 1024  # 50 MB/s

    if is_debug_mode():
        min_produced = 20_000
        min_reclaimed = 10
    else:
        min_produced = 400_000
        min_reclaimed = 100

    def __init__(self, test_context: TestContext):
        super().__init__(test_context)

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        self.rpk.create_topic(
            topic=self.TOPIC_NAME,
            partitions=self.PARTITIONS,
            replicas=3,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
            },
        )

    @cluster(num_nodes=4)
    def test_write_pressure_during_leveling(self):
        self.wait_for_managed_logs()

        assert self.redpanda
        # Large ceiling; we stop by ack count, not by exhausting the producer.
        msg_count = 10_000_000
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_NAME,
            msg_size=self.MSG_SIZE,
            msg_count=msg_count,
            rate_limit_bps=self.RATE_LIMIT_BPS,
            tolerate_failed_produce=True,
        )
        producer.start()
        try:
            # Wait for sustained production so leveling has steady fragmentation
            # to chew on while writes continue.
            wait_until(
                lambda: producer.produce_status.acked >= self.min_produced,
                timeout_sec=360,
                backoff_sec=5,
                err_msg=lambda: (
                    f"Producer only acked {producer.produce_status.acked}"
                    f"/{self.min_produced}"
                ),
            )

            self.logger.info(
                f"Write phase: {producer.produce_status.acked} acked, "
                f"{self.get_leveling_completed()} ranges completed, "
                f"{self.get_extents_reclaimed()} extents reclaimed"
            )

            # While the producer is still running, confirm leveling reclaims a
            # meaningful number of extents — i.e. it makes forward progress
            # under write pressure rather than stalling.
            self.wait_for_extents_reclaimed(
                self.min_reclaimed,
                timeout_sec=180,
            )
        finally:
            producer.stop()
