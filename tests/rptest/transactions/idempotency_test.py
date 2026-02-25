# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from collections import defaultdict

import confluent_kafka as ck

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.transactions.verifiers.idempotency_load_generator import (
    PausableIdempotentProducer,
)
from rptest.util import wait_until


class IdempotentProducerRecoveryTest(RedpandaTest):
    """This test ensures that various client implementations can recover
    from scenario in which the producers get evicted on the brokers.
    When a producer is evicted it loses all old state including the
    sequence numbers. So the client attempting to produce after that
    should still recover and be able to make progress. There are subtle
    variations among client implementations around how they deal
    with this situation, so this test acts a regression test ensuring that we
    do not break this behavior."""

    def __init__(self, test_context):
        super(IdempotentProducerRecoveryTest, self).__init__(
            test_context=test_context, num_brokers=1
        )
        self.test_topic = TopicSpec(
            name="test", partition_count=1, replication_factor=1
        )

    def get_producer_count(self):
        """Returns the total idempotent producer cache size across all nodes."""
        metrics = self.redpanda.metrics_sample(
            "idempotency_pid_cache_size", self.redpanda.started_nodes()
        )
        assert metrics, "No metrics samples found"
        total = sum(int(s.value) for s in metrics.samples)
        self.redpanda.logger.debug(f"producer cache size: {total}")
        return total

    def wait_for_eviction(self, active_producers_remaining, expected_to_be_evicted):
        def do_wait():
            samples = [
                "idempotency_pid_cache_size",
                "producer_state_manager_evicted_producers",
            ]
            brokers = self.redpanda.started_nodes()
            metrics = self.redpanda.metrics_samples(samples, brokers)
            producers_per_node = defaultdict(int)
            evicted_per_node = defaultdict(int)
            for pattern, metric in metrics.items():
                for m in metric.samples:
                    id = self.redpanda.node_id(m.node)
                    if pattern == "idempotency_pid_cache_size":
                        producers_per_node[id] += int(m.value)
                    elif pattern == "producer_state_manager_evicted_producers":
                        evicted_per_node[id] += int(m.value)

            self.redpanda.logger.debug(f"active producers: {producers_per_node}")
            self.redpanda.logger.debug(f"evicted producers: {evicted_per_node}")

            remaining_match = all(
                [
                    num == active_producers_remaining
                    for num in producers_per_node.values()
                ]
            )

            evicted_match = all(
                [val == expected_to_be_evicted for val in evicted_per_node.values()]
            )

            return (
                len(producers_per_node) == len(brokers)
                and remaining_match
                and evicted_match
            )

        wait_until(
            do_wait,
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Not all producers were evicted in 20secs.",
            retry_on_exc=False,
        )

    @cluster(num_nodes=2)
    def test_java_client_recovery_on_producer_eviction(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            self.test_topic.name,
            self.test_topic.partition_count,
            self.test_topic.replication_factor,
        )

        workload_svc = PausableIdempotentProducer(self.test_context, self.redpanda)
        workload_svc.start()

        workload_svc.start_producer(
            self.test_topic.name, self.test_topic.partition_count
        )
        # Generate some load
        workload_svc.ensure_progress(expected=1000, timeout_sec=20)

        # Pause the producer to evict the producer sessions
        workload_svc.pause_producer()

        progress_so_far = workload_svc.get_progress().json()["num_produced"]

        # Evict all producers
        rpk.cluster_config_set("transactional_id_expiration_ms", 0)
        self.wait_for_eviction(active_producers_remaining=0, expected_to_be_evicted=1)
        rpk.cluster_config_set("transactional_id_expiration_ms", 3600000)

        # Resume the idempotency session again.
        workload_svc.start_producer(
            self.test_topic.name, self.test_topic.partition_count
        )

        workload_svc.ensure_progress(expected=progress_so_far + 1000, timeout_sec=20)
        # Verify the producer can make progress without exceptions
        workload_svc.stop_producer()

    @cluster(num_nodes=1)
    def test_librdkafka_recovery_on_producer_eviction(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            self.test_topic.name,
            self.test_topic.partition_count,
            self.test_topic.replication_factor,
        )

        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "enable.idempotence": True,
            }
        )

        def produce_some():
            def on_delivery(err, _):
                assert err is None, err

            for i in range(1000):
                producer.produce(
                    self.test_topic.name, str(i), str(i), on_delivery=on_delivery
                )
            producer.flush()

        produce_some()

        # Evict all producers
        rpk.cluster_config_set("transactional_id_expiration_ms", 0)
        self.wait_for_eviction(active_producers_remaining=0, expected_to_be_evicted=1)
        rpk.cluster_config_set("transactional_id_expiration_ms", 3600000)

        # Ensure producer can recover.
        produce_some()

    @cluster(num_nodes=1)
    def test_producer_state_survives_prefix_truncation(self):
        """Verifies that idempotent producer state is preserved across
        log prefix truncation (delete-records). After truncation the broker
        should still know about the producers that were active before."""
        num_producers = 5
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            self.test_topic.name,
            self.test_topic.partition_count,
            self.test_topic.replication_factor,
        )

        producers = []
        for _ in range(num_producers):
            p = ck.Producer(
                {
                    "bootstrap.servers": self.redpanda.brokers(),
                    "enable.idempotence": True,
                }
            )
            producers.append(p)

        def on_delivery(err, _):
            assert err is None, err

        # Produce with each producer so the broker registers them.
        for p in producers:
            for i in range(100):
                p.produce(self.test_topic.name, str(i), str(i), on_delivery=on_delivery)
            p.flush()

        count_before = self.get_producer_count()
        assert count_before == num_producers, (
            f"Expected {num_producers} producers, got {count_before}"
        )

        # Get the high watermark and prefix truncate up to it.
        offsets = rpk.describe_topic(self.test_topic.name)
        hw = None
        for o in offsets:
            if o.id == 0:
                hw = o.high_watermark
                break
        assert hw is not None

        response = rpk.trim_prefix(self.test_topic.name, hw, partitions=[0])
        assert len(response) == 1
        assert response[0].error_msg == "", f"Err: {response[0].error_msg}"

        # Wait for the truncation to be applied.
        def truncation_applied():
            offsets = rpk.describe_topic(self.test_topic.name)
            for o in offsets:
                if o.id == 0:
                    return o.start_offset >= hw
            return False

        wait_until(
            truncation_applied,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Prefix truncation did not complete",
        )

        # Verify producer count is retained after truncation.
        count_after = self.get_producer_count()
        assert count_after == num_producers, (
            f"Expected {num_producers} producers after truncation, got {count_after}"
        )
