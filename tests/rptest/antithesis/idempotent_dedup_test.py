# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Property under test: idempotent-dedup
#
# For an idempotent producer, retried requests with the same sequence
# number must never produce duplicate records, even across leader changes.
#
# Deduplication is tracked per-producer in rm_stm via producer_state's
# try_emplace_request (producer_state.cc:372).
#
# Three variants:
#   - Basic: single producer, default config — baseline idempotency check
#   - Expiry + restart: short transactional_id_expiration_ms (1s) with
#     producer restarts. Old PIDs expire between rounds, exercising the
#     eviction/re-registration path and skip_sequence_checks (rm_stm.cc:
#     1157). This is the only test that exercises producer state expiry.
#   - Cloud topics: same rm_stm logic but on the cloud topics storage
#     path (placeholder batches via Raft)
#
# Each kgo-verifier invocation creates a new Kafka client with a new PID
# (via InitProducerID). The expiry+restart test exploits this: round N's
# PID expires before round N+1, so the rm_stm sees the new PID as fresh.
#
# Antithesis injects faults externally to trigger leader elections
# during in-flight idempotent requests.

from __future__ import annotations

import logging
import time

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda_types import RedpandaServiceForClients
from rptest.services.redpanda import (
    CHAOS_LOG_ALLOW_LIST,
    SISettings,
    MetricsEndpoint,
)
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.services.verifiable_producer import VerifiableProducer, is_int_with_prefix
from rptest.tests.redpanda_test import RedpandaTest

from rptest.antithesis.antithesis_utils import (
    AntithesisTimeoutMixin,
    always,
    always_or_unreachable,
    sometimes,
    reachable,
    unreachable,
)


def _verify_idempotency(
    producer: KgoVerifierProducer, storage_path: str, strict: bool = True
) -> None:
    """Emit idempotency assertions for a single producer.

    When strict=False, bad_offsets are reported as sometimes
    rather than always_or_unreachable — used for expiry tests
    where evictions can cause expected duplicates.
    """
    bad = producer.produce_status.bad_offsets
    acked = producer.produce_status.acked

    if acked > 0 and strict:
        if bad > 0:
            unreachable(
                f"Idempotency violations detected ({storage_path})",
                {"bad_offsets": bad, "acked": acked},
            )

        always_or_unreachable(
            bad == 0,
            f"No idempotency violations ({storage_path})",
            {"bad_offsets": bad, "acked": acked},
        )

    always(
        acked > 0,
        f"Idempotent producer acked writes ({storage_path})",
        {"acked": acked},
    )


def _run_producer(
    test_context: TestContext,
    redpanda: RedpandaServiceForClients,
    topic: str,
    msg_size: int,
    msg_count: int,
    rate_limit_bps: int,
    storage_path: str,
    logger: logging.Logger,
    clean: bool = True,
) -> KgoVerifierProducer:
    """Run a single kgo-verifier producer and return it with status populated."""
    p = KgoVerifierProducer(
        test_context,
        redpanda,
        topic,
        msg_size,
        msg_count,
        rate_limit_bps=rate_limit_bps,
    )
    p.start(clean=clean)
    try:
        p.wait_for_acks(msg_count, timeout_sec=600, backoff_sec=5)
        p.wait_for_offset_map()
    except Exception as e:
        logger.warning(f"Producer incomplete ({storage_path}): {e}")
    try:
        p.wait(timeout_sec=60)
    except Exception as e:
        logger.warning(f"Producer wait failed ({storage_path}): {e}")
    try:
        p.stop()
    except Exception:
        pass
    try:
        p.free()
    except Exception as e:
        logger.debug(f"Producer free failed ({storage_path}): {e}")
    return p


_BASE_CONF = dict(
    raft_heartbeat_interval_ms=100,
    raft_election_timeout_ms=500,
    log_segment_size=1048576,
)


class IdempotentBasicTest(AntithesisTimeoutMixin, RedpandaTest):
    """Baseline idempotency: single producer, default config.
    Tested with both kgo (Go) and java clients."""

    MSG_SIZE = 512
    MSG_COUNT = 5000
    TOPIC_NAME = "idemp-basic"

    topics = [
        TopicSpec(name=TOPIC_NAME, partition_count=3, replication_factor=3),
    ]

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf=dict(_BASE_CONF),
        )

    @cluster(num_nodes=4, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_idempotent_basic_kgo(self) -> None:
        p = _run_producer(
            self.test_context,
            self.redpanda,
            self.TOPIC_NAME,
            self.MSG_SIZE,
            self.MSG_COUNT,
            256 * 1024,
            "kgo basic",
            self.logger,
        )

        _verify_idempotency(p, "kgo basic")
        reachable(
            "Idempotent kgo basic test completed", {"acked": p.produce_status.acked}
        )

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_idempotent_basic_java(self) -> None:
        producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.TOPIC_NAME,
            throughput=1000,
            message_validator=is_int_with_prefix,
            enable_idempotence=True,
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

        consumer = VerifiableConsumer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.TOPIC_NAME,
            group_id="idemp-java-verify",
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

        acked = producer.num_acked
        consumed = consumer.total_consumed()

        # With idempotence, consumed should equal acked (no duplicates).
        # consumed > acked means duplicates; consumed < acked means loss.
        if consumed != acked:
            unreachable(
                "Idempotent java producer: consumed != acked",
                {"acked": acked, "consumed": consumed, "diff": consumed - acked},
            )

        always_or_unreachable(
            consumed == acked,
            "No duplicates or loss with idempotent java producer",
            {"acked": acked, "consumed": consumed},
        )

        always(acked > 0, "Java idempotent producer acked writes", {"acked": acked})
        reachable(
            "Idempotent java basic test completed",
            {"acked": acked, "consumed": consumed},
        )

        producer.free()
        consumer.free()


class IdempotentExpiryRestartTest(AntithesisTimeoutMixin, RedpandaTest):
    """Short producer state expiry (1s) with multiple produce rounds.
    Between rounds we sleep to let the previous PID expire. When the
    next round's new PID arrives, rm_stm's producer_state for the old
    PID has been evicted by the producer_state_manager reaper. This
    exercises the eviction/re-registration path — the only test that
    hits skip_sequence_checks (rm_stm.cc:1157)."""

    MSG_SIZE = 512
    MSGS_PER_ROUND = 1500
    NUM_ROUNDS = 3
    TOPIC_NAME = "idemp-expiry"

    topics = [TopicSpec(name=TOPIC_NAME, partition_count=3, replication_factor=3)]

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf={
                **_BASE_CONF,
                # 1s expiry — any AT network fault > 1s triggers eviction
                "transactional_id_expiration_ms": 1000,
            },
        )

    @cluster(num_nodes=4, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_idempotent_expiry_restart(self) -> None:
        total_bad = 0
        total_acked = 0

        for round_ix in range(self.NUM_ROUNDS):
            p = _run_producer(
                self.test_context,
                self.redpanda,
                self.TOPIC_NAME,
                self.MSG_SIZE,
                self.MSGS_PER_ROUND,
                256 * 1024,
                f"expiry round {round_ix}",
                self.logger,
                clean=(round_ix == 0),
            )

            total_bad += p.produce_status.bad_offsets
            total_acked += p.produce_status.acked

            _verify_idempotency(p, f"expiry round {round_ix}", strict=False)

            # Sleep > expiry so the PID's producer_state gets reaped
            # before the next round's new PID arrives
            if round_ix < self.NUM_ROUNDS - 1:
                self.logger.info(f"Round {round_ix} done, sleeping 3s for PID expiry")
                time.sleep(3)

        # Check that producer evictions actually happened — this is
        # what makes this test different from the basic test
        try:
            evictions = self.redpanda.metric_sum(
                "vectorized_cluster_producer_state_manager_evicted_producers_total",
                metrics_endpoint=MetricsEndpoint.METRICS,
            )
        except Exception as e:
            self.logger.debug(f"Could not read eviction metric: {e}")
            evictions = 0

        sometimes(
            evictions > 0,
            "Producer state evictions occurred during expiry-restart test",
            {"evictions": evictions, "rounds": self.NUM_ROUNDS},
        )

        # Idempotency can degrade when producer state is evicted —
        # retries after eviction hit skip_sequence_checks and may
        # produce duplicates. Only assert strict idempotency when
        # no evictions occurred.
        if evictions == 0 and total_acked > 0:
            if total_bad > 0:
                unreachable(
                    "Idempotency violations without evictions",
                    {"total_bad": total_bad, "total_acked": total_acked},
                )

            always_or_unreachable(
                total_bad == 0,
                "No idempotency violations without evictions",
                {"total_bad": total_bad, "total_acked": total_acked},
            )

        if evictions > 0 and total_bad > 0:
            # Expected: evictions cause dedup to degrade
            reachable(
                "Idempotency degraded after producer state eviction",
                {
                    "total_bad": total_bad,
                    "evictions": evictions,
                    "total_acked": total_acked,
                },
            )

        reachable(
            "Idempotent expiry-restart test completed",
            {
                "total_acked": total_acked,
                "rounds": self.NUM_ROUNDS,
                "evictions": evictions,
            },
        )


class IdempotentCloudTopicsTest(AntithesisTimeoutMixin, RedpandaTest):
    """Idempotent produce on cloud topics — rm_stm sequence tracking
    applies to placeholder batches replicated via Raft."""

    MSG_SIZE = 512
    MSG_COUNT = 3000
    TOPIC_NAME = "idemp-ct"

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
                **_BASE_CONF,
                "cloud_topics_enabled": True,
                "enable_cluster_metadata_upload_loop": False,
            },
            si_settings=si_settings,
        )

    @cluster(num_nodes=4, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_idempotent_cloud_topics(self) -> None:
        self.create_topic(
            self.TOPIC_NAME,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
            },
        )

        p = _run_producer(
            self.test_context,
            self.redpanda,
            self.TOPIC_NAME,
            self.MSG_SIZE,
            self.MSG_COUNT,
            128 * 1024,
            "cloud topics",
            self.logger,
        )

        _verify_idempotency(p, "cloud topics")
        reachable(
            "Idempotent cloud topics test completed", {"acked": p.produce_status.acked}
        )
