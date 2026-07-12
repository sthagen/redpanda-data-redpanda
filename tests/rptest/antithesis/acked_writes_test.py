# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Property under test: committed-entries-never-lost
#
# Any write acknowledged with acks=all (Raft quorum_ack) must be durable
# and readable after any combination of minority node failures. The commit
# index is computed as the median of flushed offsets across voters
# (consensus.cc:3221-3275), and only advances when the entry at the
# majority match was written in the current term (the Section 5.4.2 guard
# at consensus.cc:3238). Truncation on followers must never go below
# commit_index (consensus.cc:2213-2236).
#
# Each storage path is tested with two independent Kafka client
# implementations (kgo-verifier / Go and VerifiableProducer / Java)
# to exercise different protocol paths and retry behaviors.
#
# Antithesis injects faults externally (process kills, network partitions,
# clock skew, I/O delays). These tests drive the workload and emit
# Antithesis SDK assertions for the durability invariant.

from __future__ import annotations

from typing import cast

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import (
    CHAOS_LOG_ALLOW_LIST,
    SISettings,
)
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.services.verifiable_producer import VerifiableProducer, is_int_with_prefix
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_for_local_storage_truncate
from rptest.utils.si_utils import quiesce_uploads

from rptest.antithesis.antithesis_utils import (
    AntithesisTimeoutMixin,
    always,
    always_or_unreachable,
    reachable,
    unreachable,
)


def _verify_kgo(
    producer: KgoVerifierProducer, consumer: KgoVerifierSeqConsumer, path: str
) -> None:
    """Emit assertions using kgo-verifier's built-in offset validation."""
    acked = producer.produce_status.acked
    bad = producer.produce_status.bad_offsets
    invalid = consumer.consumer_status.validator.invalid_reads
    valid = consumer.consumer_status.validator.valid_reads
    # kgo-verifier returns null when the offset map wasn't loaded, so this is
    # genuinely optional at runtime even though the field is typed non-optional.
    lost = cast(
        "dict[str, int] | None", consumer.consumer_status.validator.lost_offsets
    )

    has_lost = lost is not None and any(v > 0 for v in lost.values())
    total_lost = sum(lost.values()) if lost else 0

    always(acked > 0, f"Producer acked writes ({path})", {"acked": acked})
    always(valid > 0, f"Consumer read records ({path})", {"valid_reads": valid})

    always_or_unreachable(
        bad == 0,
        f"No idempotency violations during produce ({path})",
        {"bad_offsets": bad, "acked": acked},
    )

    if invalid > 0:
        unreachable(
            f"Invalid reads — consumed data doesn't match produced ({path})",
            {"invalid_reads": invalid, "valid_reads": valid},
        )

    if has_lost:
        unreachable(
            f"Lost acked writes — data loss ({path})",
            {"lost_offsets": lost, "acked": acked, "valid_reads": valid},
        )

    always_or_unreachable(
        not has_lost,
        f"All acked writes consumable — no data loss ({path})",
        {"lost_offsets": lost, "acked": acked, "valid_reads": valid},
    )

    reachable(
        f"kgo produce-consume cycle completed ({path})",
        {
            "acked": acked,
            "valid_reads": valid,
            "bad_offsets": bad,
            "invalid_reads": invalid,
            "lost": total_lost,
        },
    )


def _verify_java(
    producer: VerifiableProducer, consumer: VerifiableConsumer, path: str
) -> None:
    """Emit assertions using VerifiableProducer/Consumer."""
    acked = producer.num_acked
    consumed = consumer.total_consumed()

    always(acked > 0, f"Java producer acked writes ({path})", {"acked": acked})
    always(consumed > 0, f"Java consumer read records ({path})", {"consumed": consumed})

    if consumed < acked:
        unreachable(
            f"Lost acked writes — data loss, java client ({path})",
            {"acked": acked, "consumed": consumed},
        )

    always_or_unreachable(
        consumed >= acked,
        f"All acked writes consumable — no data loss, java client ({path})",
        {"acked": acked, "consumed": consumed},
    )
    reachable(
        f"Java produce-consume cycle completed ({path})",
        {"acked": acked, "consumed": consumed},
    )


class AckedWritesBase(AntithesisTimeoutMixin, RedpandaTest):
    """Shared config and test flow for all storage paths."""

    MSG_SIZE = 512
    MSG_COUNT = 5000
    TOPIC_NAME = "acked-writes"
    RATE_LIMIT_BPS = 256 * 1024

    def _kgo_produce_and_verify(self, path: str) -> None:
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_NAME,
            self.MSG_SIZE,
            self.MSG_COUNT,
            rate_limit_bps=self.RATE_LIMIT_BPS,
        )
        producer.start(clean=True)
        try:
            producer.wait_for_acks(self.MSG_COUNT, timeout_sec=600, backoff_sec=5)
            producer.wait_for_offset_map()
        except Exception as e:
            self.logger.warning(f"kgo producer incomplete: {e}")
        try:
            producer.wait(timeout_sec=60)
        except Exception as e:
            self.logger.warning(f"kgo producer wait failed: {e}")
        try:
            producer.stop()
        except Exception:
            pass
        try:
            producer.free()
        except Exception as e:
            self.logger.debug(f"Producer free failed: {e}")

        try:
            self._before_consume()
        except Exception as e:
            self.logger.warning(f"before_consume failed: {e}")

        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.TOPIC_NAME,
            loop=False,
        )
        consumer.start(clean=False)
        consumer.wait(timeout_sec=600)

        _verify_kgo(producer, consumer, f"kgo {path}")

    def _java_produce_and_verify(self, path: str) -> None:
        producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.TOPIC_NAME,
            throughput=1000,
            message_validator=is_int_with_prefix,
            acks=-1,
        )
        producer.start()
        try:
            wait_until(
                lambda: producer.num_acked >= self.MSG_COUNT,
                timeout_sec=600,
                backoff_sec=5,
                err_msg="Java producer failed to ack messages",
            )
        except Exception as e:
            self.logger.warning(f"Java producer incomplete: {e}")
        producer.stop()

        try:
            self._before_consume()
        except Exception as e:
            self.logger.warning(f"before_consume failed: {e}")

        consumer = VerifiableConsumer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.TOPIC_NAME,
            group_id="acked-java-verify",
        )
        consumer.start()
        try:
            wait_until(
                lambda: consumer.total_consumed() >= producer.num_acked,
                timeout_sec=600,
                backoff_sec=5,
                err_msg="Java consumer failed to consume all records",
            )
        except Exception as e:
            self.logger.warning(f"Java consumer incomplete: {e}")
        consumer.stop()

        _verify_java(producer, consumer, f"java {path}")
        producer.free()
        consumer.free()

    def _before_consume(self) -> None:
        pass


class AckedWritesSurviveLocalDiskTest(AckedWritesBase):
    TOPIC_NAME = "acked-writes-local"

    topics = [
        TopicSpec(name=TOPIC_NAME, partition_count=3, replication_factor=3),
    ]

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf=dict(
                raft_heartbeat_interval_ms=100,
                raft_election_timeout_ms=500,
                log_segment_size=1048576,
            ),
        )

    @cluster(num_nodes=4, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_acked_writes_kgo(self) -> None:
        self._kgo_produce_and_verify("local disk")

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_acked_writes_java(self) -> None:
        self._java_produce_and_verify("local disk")


class AckedWritesSurviveTieredStorageTest(AckedWritesBase):
    TOPIC_NAME = "acked-writes-tiered"

    segment_size = 1048576

    def __init__(self, test_context: TestContext) -> None:
        si_settings = SISettings(
            test_context,
            log_segment_size=self.segment_size,
            cloud_storage_max_connections=10,
            fast_uploads=True,
        )
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf=dict(
                raft_heartbeat_interval_ms=100,
                raft_election_timeout_ms=500,
            ),
            si_settings=si_settings,
        )
        self.topics = [
            TopicSpec(name=self.TOPIC_NAME, partition_count=3, replication_factor=3),
        ]

    def _before_consume(self) -> None:
        quiesce_uploads(self.redpanda, [self.TOPIC_NAME], timeout_sec=600)

        rpk = RpkTool(self.redpanda)
        self.redpanda.wait_until(
            lambda: rpk.alter_topic_config(
                self.TOPIC_NAME,
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
                str(self.segment_size),
            )
            or True,
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Failed to set local retention",
            retry_on_exc=True,
        )

        wait_for_local_storage_truncate(
            self.redpanda,
            self.TOPIC_NAME,
            target_bytes=self.segment_size * 2,
            timeout_sec=120,
        )

    @cluster(num_nodes=4, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_acked_writes_tiered_kgo(self) -> None:
        self._kgo_produce_and_verify("tiered storage")

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_acked_writes_tiered_java(self) -> None:
        self._java_produce_and_verify("tiered storage")


class AckedWritesSurviveCloudTopicsTest(AckedWritesBase):
    TOPIC_NAME = "acked-writes-cloud"
    MSG_COUNT = 3000
    RATE_LIMIT_BPS = 128 * 1024

    def __init__(self, test_context: TestContext) -> None:
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf={
                "raft_heartbeat_interval_ms": 100,
                "raft_election_timeout_ms": 500,
                "cloud_topics_enabled": True,
                "enable_cluster_metadata_upload_loop": False,
            },
            si_settings=si_settings,
        )

    def _create_cloud_topic(self, name: str) -> None:
        self.create_topic(
            name,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
            },
        )

    @cluster(num_nodes=4, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_acked_writes_cloud_kgo(self) -> None:
        self._create_cloud_topic(self.TOPIC_NAME)
        self._kgo_produce_and_verify("cloud topics")

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_acked_writes_cloud_java(self) -> None:
        self._create_cloud_topic(self.TOPIC_NAME)
        self._java_produce_and_verify("cloud topics")
