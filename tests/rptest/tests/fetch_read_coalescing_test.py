# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest

RECORD_COUNT = 50
# One node per consumer; the fanout is the thundering herd the coalescer exists
# for.
FANOUT = 6


class FetchReadCoalescingTest(RedpandaTest):
    """End-to-end check for INC-2843 P2a fetch read coalescing: a fanout of
    consumers reading the same partition offset shares one serialized read
    instead of reading (and serializing) the partition once per consumer, while
    every consumer still receives the correct data."""

    topics = (TopicSpec(name="coalesce", partition_count=1, replication_factor=1),)

    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(test_ctx, num_brokers=1, *args, **kwargs)

    def _coalescer_metric(self, name: str) -> int:
        return int(
            self.redpanda.metric_sum(
                f"vectorized_kafka_fetch_read_coalescer_{name}_total",
                metrics_endpoint=MetricsEndpoint.METRICS,
                expect_metric=True,
            )
        )

    @cluster(num_nodes=1 + FANOUT)
    def test_fanout_shares_one_read(self):
        rpk = RpkTool(self.redpanda)

        # needs_restart::no, so coalescing turns on without a bounce.
        self.redpanda.set_cluster_config({"kafka_fetch_read_coalescing_enabled": True})

        # Independent consumers (no shared group), all reading partition 0 from
        # the start. Started against the still-empty topic so they park in a
        # long-poll on the same partition; the produce below then wakes the
        # whole fleet on each record, so every offset is fetched near
        # simultaneously by all of them and coalesces.
        consumers = [
            RpkConsumer(
                self.test_context,
                self.redpanda,
                self.topic,
                partitions=[0],
                offset="oldest",
                num_msgs=RECORD_COUNT,
            )
            for _ in range(FANOUT)
        ]
        for c in consumers:
            c.start()
        # Let the fleet connect and park before the first record arrives.
        time.sleep(5)

        expected = [(f"key-{i:04d}", f"val-{i:04d}") for i in range(RECORD_COUNT)]
        for key, value in expected:
            rpk.produce(self.topic, key, value, partition=0)

        for i, c in enumerate(consumers):
            wait_until(
                lambda c=c: c.message_count >= RECORD_COUNT,
                timeout_sec=60,
                backoff_sec=0.5,
                err_msg=f"consumer {i} got {c.message_count}/{RECORD_COUNT}",
            )
        for c in consumers:
            c.stop()

        # Correctness: serving a shared read must deliver each consumer the same
        # bytes a solo read would have.
        for i, c in enumerate(consumers):
            got = [
                (m["key"], m["value"])
                for m in sorted(c.messages, key=lambda m: int(m["offset"]))
            ][:RECORD_COUNT]
            assert got == expected, (
                f"consumer {i} data mismatch (first records: {got[:2]})"
            )

        insertions = self._coalescer_metric("insertions")
        reinsertions = self._coalescer_metric("reinsertions")
        ready_hits = self._coalescer_metric("ready_hits")
        inflight_hits = self._coalescer_metric("inflight_hits")
        self.logger.info(
            f"coalescer: insertions={insertions} reinsertions={reinsertions} "
            f"ready_hits={ready_hits} inflight_hits={inflight_hits}"
        )

        # Coalescing engaged: some of the fanout's reads were served from a
        # shared result rather than each reading the partition afresh.
        assert ready_hits + inflight_hits > 0, (
            "expected the fanout to coalesce reads, but saw no hits"
        )
