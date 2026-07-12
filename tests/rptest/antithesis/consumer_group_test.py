# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Property under test: consumer-group-rebalance
#
# The group coordinator (group_manager / group_stm) must preserve
# committed offsets across rebalances and converge to a stable
# assignment after membership changes. This test asserts the
# no-loss half: total consumed across all members is at least
# total acked. Duplicate detection (no record consumed twice) is
# out of scope here and would require per-record dedup tracking
# in the consumer.
#
# Antithesis fault injection targets the coordinator node, member
# heartbeat paths, and the __consumer_offsets topic replication.

from __future__ import annotations

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.tests.redpanda_test import RedpandaTest

from rptest.antithesis.antithesis_utils import (
    AntithesisTimeoutMixin,
    always,
    always_or_unreachable,
    sometimes,
    reachable,
    unreachable,
)


class ConsumerGroupRebalanceTest(AntithesisTimeoutMixin, RedpandaTest):
    """Produce continuously while consumers join and leave a group.

    Two VerifiableConsumer instances (separate processes) share a
    consumer group. One runs for the entire test; the other is
    stopped and restarted to trigger rebalances. Under fault
    injection, additional rebalances occur organically from
    heartbeat failures and coordinator failovers.
    """

    MSG_SIZE = 512
    MSG_COUNT = 5000
    TOPIC_NAME = "cg-rebalance"
    PARTITION_COUNT = 8
    GROUP_ID = "cg-rebalance-group"

    topics = [
        TopicSpec(
            name=TOPIC_NAME,
            partition_count=PARTITION_COUNT,
            replication_factor=3,
        )
    ]

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf=dict(
                raft_heartbeat_interval_ms=100,
                raft_election_timeout_ms=500,
                group_initial_rebalance_delay=0,
            ),
        )

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_consumer_group_rebalance(self) -> None:
        # Start producer — runs for the entire test
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_NAME,
            self.MSG_SIZE,
            self.MSG_COUNT,
            rate_limit_bps=256 * 1024,
        )
        producer.start(clean=True)

        # Consumer A — stays up for the entire test
        consumer_a = VerifiableConsumer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.TOPIC_NAME,
            group_id=self.GROUP_ID,
            session_timeout_sec=30,
        )
        consumer_a.start()

        # Wait for consumer A to start consuming
        try:
            wait_until(
                lambda: consumer_a.total_consumed() > 0,
                timeout_sec=60,
                backoff_sec=2,
                err_msg="Consumer A didn't start consuming",
            )
        except Exception as e:
            self.logger.warning(f"Consumer A slow to start: {e}")

        # Consumer B — join, consume, leave, rejoin to trigger
        # rebalances. Each cycle forces partition reassignment.
        rebalance_cycles = 3
        rebalances_observed = 0
        total_consumed_by_b = 0

        for cycle in range(rebalance_cycles):
            consumer_b = VerifiableConsumer(
                self.test_context,
                num_nodes=1,
                redpanda=self.redpanda,
                topic=self.TOPIC_NAME,
                group_id=self.GROUP_ID,
                session_timeout_sec=30,
            )
            consumer_b.start()

            # Let both consumers run together
            try:
                wait_until(
                    lambda: consumer_b.total_consumed() > 0,
                    timeout_sec=60,
                    backoff_sec=2,
                    err_msg=f"Consumer B didn't consume in cycle {cycle}",
                )
            except Exception as e:
                self.logger.warning(f"Consumer B slow in cycle {cycle}: {e}")

            # Stop consumer B — triggers rebalance, A picks up
            # B's partitions
            b_consumed = consumer_b.total_consumed()
            total_consumed_by_b += b_consumed
            consumer_b.stop()
            consumer_b.free()
            rebalances_observed += 1

            self.logger.info(
                f"Rebalance cycle {cycle}: A={consumer_a.total_consumed()}"
                f" B={b_consumed} B_total={total_consumed_by_b}"
            )

        # Wait for producer to finish
        try:
            producer.wait_for_acks(self.MSG_COUNT, timeout_sec=600, backoff_sec=5)
            producer.wait_for_offset_map()
        except Exception as e:
            self.logger.warning(f"Producer incomplete: {e}")
        try:
            producer.wait(timeout_sec=60)
        except Exception as e:
            self.logger.warning(f"Producer wait failed: {e}")
        try:
            producer.stop()
        except Exception:
            pass
        try:
            producer.free()
        except Exception as e:
            self.logger.debug(f"Producer free failed: {e}")

        acked = producer.produce_status.acked

        # Wait for consumer A to drain remaining records by checking
        # the group's committed lag via rpk. This is authoritative —
        # it reflects what the group coordinator knows, not what
        # individual consumers counted locally.
        rpk = RpkTool(self.redpanda)

        def group_lag_zero() -> bool:
            try:
                group = rpk.group_describe(self.GROUP_ID)
                return group.total_lag == 0
            except Exception:
                return False

        try:
            wait_until(
                group_lag_zero,
                timeout_sec=300,
                backoff_sec=5,
                err_msg="Consumer group still has lag",
            )
        except Exception as e:
            self.logger.warning(f"Consumer group catch-up incomplete: {e}")

        consumer_a.stop()

        # Use rpk group describe for the authoritative consumed count
        try:
            group = rpk.group_describe(self.GROUP_ID)
            consumed = sum(
                p.current_offset
                for p in group.partitions
                if p.current_offset is not None
            )
        except Exception as e:
            self.logger.warning(f"Could not describe group: {e}")
            consumed = consumer_a.total_consumed() + total_consumed_by_b

        # Assertions
        if acked > 0:
            if consumed < acked:
                unreachable(
                    "Consumer group lost records after rebalances",
                    {"acked": acked, "consumed": consumed, "gap": acked - consumed},
                )

            always_or_unreachable(
                consumed >= acked,
                "All produced records consumed after rebalances",
                {"acked": acked, "consumed": consumed},
            )

        always(
            acked > 0,
            "Producer acked writes for group rebalance test",
            {"acked": acked},
        )

        sometimes(
            consumed > 0,
            "Consumer group consumed records",
            {"consumed": consumed},
        )

        sometimes(
            rebalances_observed > 0,
            "Consumer group rebalances occurred",
            {"rebalances": rebalances_observed},
        )

        reachable(
            "Consumer group rebalance test completed",
            {"acked": acked, "consumed": consumed, "rebalances": rebalances_observed},
        )
