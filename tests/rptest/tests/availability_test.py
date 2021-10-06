# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from datetime import datetime
import random
import time

from ducktape.mark.resource import cluster
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.redpanda import RedpandaService
from rptest.tests.e2e_finjector import EndToEndFinjectorTest
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import Scale


class AvailabilityTests(EndToEndFinjectorTest):
    def validate_records(self):
        min_records = 40000
        producer_timeout_sec = 60
        consumer_timeout_sec = 60

        if self.scale.ci or self.scale.release:
            min_records = 100000
            producer_timeout_sec = 180
            consumer_timeout_sec = 180

        self.run_validation(min_records=min_records,
                            enable_idempotence=False,
                            producer_timeout_sec=producer_timeout_sec,
                            consumer_timeout_sec=consumer_timeout_sec)

    @cluster(num_nodes=5)
    def test_availability_when_one_node_failed(self):
        self.redpanda = RedpandaService(
            self.test_context,
            3,
            KafkaCliTools,
            extra_rp_conf={
                "enable_auto_rebalance_on_node_add": True,
                "group_topic_partitions": 1,
                "default_topic_replications": 3,
            })

        self.redpanda.start()
        spec = TopicSpec(name="test-topic",
                         partition_count=6,
                         replication_factor=3)

        self.redpanda.create_topic(spec)
        self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        # start failure injector with default parameters
        self.start_finjector()

        self.validate_records()

    @cluster(num_nodes=5)
    def test_recovery_after_catastrophic_failure(self):

        self.redpanda = RedpandaService(
            self.test_context,
            3,
            KafkaCliTools,
            extra_rp_conf={
                "enable_auto_rebalance_on_node_add": True,
                "group_topic_partitions": 1,
                "default_topic_replications": 3,
            })

        self.redpanda.start()
        spec = TopicSpec(name="test-topic",
                         partition_count=6,
                         replication_factor=3)

        self.redpanda.create_topic(spec)
        self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        # inject permanent random failure
        f_spec = FailureSpec(random.choice(FailureSpec.FAILURE_TYPES),
                             random.choice(self.redpanda.nodes[0:1]))

        self.inject_failure(f_spec)

        # inject transient failure on other node
        f_spec = FailureSpec(random.choice(FailureSpec.FAILURE_TYPES),
                             self.redpanda.nodes[2],
                             length=2.0 if self.scale.local else 15.0)

        self.inject_failure(f_spec)

        self.validate_records()
