# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Property under test: leader-election-progress
#
# After a leader failure with a surviving majority, a new leader must be
# elected within a bounded number of election rounds. The voter priority
# relaxation mechanism (voter_priority_tracker.h:25-27) guarantees
# eventual progress by lowering the target priority after each failed
# election. The vote timeout timer is always re-armed after each attempt
# (consensus.cc:1066, 1073), ensuring repeated attempts.
#
# This test creates a topic, produces data to establish leaders, then
# continuously monitors leadership state. Antithesis injects faults
# externally. The test asserts:
#   - sometimes: a leader exists (liveness — elections complete)
#   - sometimes: a leadership change was observed (elections actually happen)
#   - always: at most one leader per partition at any observation point
#
# Raft elections are independent of the storage path (local disk, tiered
# storage, cloud topics), so one test covers all variants.

from __future__ import annotations

from ducktape.tests.test import TestContext

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST
from rptest.tests.redpanda_test import RedpandaTest

from rptest.antithesis.antithesis_utils import (
    AntithesisTimeoutMixin,
    always,
    sometimes,
    reachable,
    monitor_leadership,
)


class LeaderElectionProgressTest(AntithesisTimeoutMixin, RedpandaTest):
    TOPIC_NAME = "election-progress"
    PARTITION_COUNT = 100

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
                # Fast elections for more state transitions per timeline
                raft_heartbeat_interval_ms=100,
                raft_election_timeout_ms=200,
                enable_leader_balancer=False,
            ),
        )

    @cluster(num_nodes=3, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_leader_election_progress(self) -> None:
        # Produce at least one message to every partition via rpk.
        # rpk runs on the ducktape-runner (excluded from faults),
        # so failures are from the broker side. Retry across rounds
        # until all partitions have acked — proving every partition
        # elected a leader for long enough to serve a write.
        rpk = RpkTool(self.redpanda)
        acked: set[int] = set()
        max_rounds = 10
        rounds_used = 0

        for round_ix in range(max_rounds):
            remaining = [p for p in range(self.PARTITION_COUNT) if p not in acked]
            if not remaining:
                break
            rounds_used = round_ix + 1

            self.logger.info(
                f"Produce round {round_ix}: {len(remaining)} partitions left"
            )
            for p in remaining:
                try:
                    rpk.produce(
                        self.TOPIC_NAME,
                        f"key-{p}",
                        f"round-{round_ix}-{p}",
                        partition=p,
                        timeout=30,
                    )
                    acked.add(p)
                except Exception as e:
                    self.logger.debug(
                        f"Partition {p} produce failed round {round_ix}: {e}"
                    )

        always(
            len(acked) == self.PARTITION_COUNT,
            "All partitions accepted at least one write",
            {
                "acked": len(acked),
                "total": self.PARTITION_COUNT,
                "rounds": rounds_used,
                "missing": [p for p in range(self.PARTITION_COUNT) if p not in acked],
            },
        )

        reachable(
            "Initial per-partition produce completed",
            {"acked": len(acked), "rounds": rounds_used},
        )

        admin = Admin(self.redpanda)

        # Explicitly transfer leadership on a few partitions to
        # guarantee the property is exercised. Antithesis fault
        # injection may cause additional elections on top of these.
        transfers_initiated = 0
        for p_id in range(0, min(10, self.PARTITION_COUNT)):
            try:
                admin.partition_transfer_leadership(
                    namespace="kafka", topic=self.TOPIC_NAME, partition=p_id
                )
                transfers_initiated += 1
            except Exception as e:
                self.logger.debug(f"Transfer for partition {p_id} failed: {e}")

        reachable(
            "Leadership transfers initiated",
            {"transfers_initiated": transfers_initiated},
        )

        changes, with_all, total, ever_had_leader = monitor_leadership(
            admin,
            self.TOPIC_NAME,
            self.PARTITION_COUNT,
            rounds=60,
            interval_sec=2,
            logger=self.logger,
        )

        sometimes(
            changes > 0,
            "Leadership change observed during test",
            {"changes": changes, "rounds": total},
        )

        sometimes(
            with_all > 0,
            "All partitions had leaders simultaneously",
            {"rounds_with_all_leaders": with_all, "total_rounds": total},
        )

        sometimes(
            len(ever_had_leader) > self.PARTITION_COUNT // 2,
            "Majority of partitions had a leader during observation",
            {
                "partitions_with_leader": len(ever_had_leader),
                "total": self.PARTITION_COUNT,
                "never_had_leader": [
                    p for p in range(self.PARTITION_COUNT) if p not in ever_had_leader
                ],
            },
        )

        self.logger.info(
            f"Election test complete: {changes} leadership "
            f"changes over {total} rounds, "
            f"{with_all} rounds with all leaders present"
        )
