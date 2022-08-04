# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import requests
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableConsumerGroupConsumer, FranzGoVerifiableProducer, await_minimum_produced_records
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.clients.types import TopicSpec
from ducktape.mark import parametrize


class PartitionBalancerScaleTest(PreallocNodesTest, PartitionMovementMixin):
    NODE_AVAILABILITY_TIMEOUT = 10
    MANY_PARTITIONS = "many_partitions"
    BIG_PARTITIONS = "big_partitions"

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context=test_context,
            node_prealloc_count=1,
            num_brokers=5,
            extra_rp_conf={
                "partition_autobalancing_mode": "continuous",
                "partition_autobalancing_node_availability_timeout_sec":
                self.NODE_AVAILABILITY_TIMEOUT,
                "partition_autobalancing_tick_interval_ms": 5000,
                "raft_learner_recovery_rate": 1073741824,
            },
            *args,
            **kwargs)

    def _start_producer(self, topic_name, msg_cnt, msg_size):
        self.producer = FranzGoVerifiableProducer(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size,
            msg_cnt,
            custom_node=self.preallocated_nodes)
        self.producer.start(clean=False)

        wait_until(lambda: self.producer.produce_status.acked > 10,
                   timeout_sec=120,
                   backoff_sec=1)

    def _start_consumer(self, topic_name, msg_size, consumers):

        self.consumer = FranzGoVerifiableConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size,
            readers=consumers,
            nodes=self.preallocated_nodes)
        self.consumer.start(clean=False)

    def verify(self):
        self.producer.wait()
        # wait for consumers to finish
        wait_until(
            lambda: self.consumer.consumer_status.valid_reads == self.producer.
            produce_status.acked, 300)
        self.consumer.shutdown()
        self.consumer.wait()

        assert self.consumer.consumer_status.valid_reads == self.producer.produce_status.acked

    def node_replicas(self, topics, node_id):
        topic_descriptions = self.client().describe_topics(topics)

        replicas = set()
        for tp_d in topic_descriptions:
            for p in tp_d.partitions:
                for r in p.replicas:
                    if r == node_id:
                        replicas.add(f'{tp_d.name}/{p}')

        return replicas

    @cluster(num_nodes=6)
    @parametrize(type=MANY_PARTITIONS)
    @parametrize(type=BIG_PARTITIONS)
    def test_partition_balancer_with_many_partitions(self, type):
        replication_factor = 3
        if type == self.MANY_PARTITIONS:
            # in total the test produces 250GB of data
            message_size = 128 * (2 ^ 10)
            message_cnt = 2000000
            consumers = 8
            partitions_count = 18000
        else:
            message_size = 512 * (2 ^ 10)
            message_cnt = 5000000
            consumers = 8
            partitions_count = 200

        topic = TopicSpec(partition_count=partitions_count,
                          replication_factor=replication_factor)
        self.client().create_topic(topic)

        self._start_producer(topic.name, message_cnt, message_size)
        self._start_consumer(topic.name, message_size, consumers=consumers)
        self.logger.info(
            f"waiting for {(message_size*message_cnt/2) / (2^20)} MB to be produced to "
            f"{partitions_count} partitions ({((message_size*message_cnt/2) / (2^20)) / partitions_count} MB per partition"
        )
        # wait for the partitions to be filled with data
        await_minimum_produced_records(self.redpanda,
                                       self.producer,
                                       min_acked=message_cnt / 2)

        # stop one of the nodes to trigger partition balancer
        stopped = random.choice(self.redpanda.nodes)
        self.redpanda.stop_node(stopped)

        stopped_id = self.redpanda.idx(stopped)

        def stopped_node_is_empty():
            replicas = self.node_replicas([topic.name], stopped_id)
            self.logger.debug(
                f"stopped node {stopped_id} hosts {len(replicas)} replicas")
            return len(replicas) == 0

        wait_until(stopped_node_is_empty, 120, 5)
        admin = Admin(self.redpanda)

        def all_reconfigurations_done():
            ongoing = admin.list_reconfigurations()
            self.logger.debug(
                f"Waiting for partition reconfigurations to finish. "
                f"Currently reconfiguring partitions: {len(ongoing)}")

            return len(ongoing) == 0

        wait_until(all_reconfigurations_done, 300, 5)

        self.verify()
