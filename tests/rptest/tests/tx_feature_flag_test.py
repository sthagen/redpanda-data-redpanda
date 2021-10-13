import random
import time

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
import requests
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kcl import KCL

from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest


class TxFeatureFlagTest(EndToEndTest):
    @cluster(num_nodes=6)
    def test_disabling_transactions_after_they_being_used(self):
        '''
        Validate that transactions can be safely disabled after 
        the feature have been used
        '''
        # start redpanda with tranasactions enabled, we use
        # replication factor 1 for group topic to make
        # it unavailable when one of the nodes is down,
        self.start_redpanda(num_nodes=3,
                            extra_rp_conf={
                                "enable_idempotence": True,
                                "enable_transactions": True,
                                "default_topic_replications": 1,
                                "default_topic_partitions": 1,
                                "health_manager_tick_interval": 3600000
                            })

        tx_topic = TopicSpec(name="tx-topic",
                             partition_count=1,
                             replication_factor=3)
        self.redpanda.create_topic(tx_topic)

        # produce some messages to tx_topic

        kcat = KafkaCat(self.redpanda)
        kcat.produce_one(tx_topic.name, msg='test-msg', tx_id='test-tx-id')

        # disable transactions,
        self.redpanda.stop()

        for n in self.redpanda.nodes:
            self.redpanda.start_node(n,
                                     override_cfg_params={
                                         "enable_idempotence": False,
                                         "enable_transactions": False,
                                         "transactional_id_expiration_ms":
                                         1000,
                                         "default_topic_replications": 3,
                                         "default_topic_partitions": 1
                                     })

        # create topic for test
        tester = TopicSpec(name="tester",
                           partition_count=1,
                           replication_factor=3)
        self.redpanda.create_topic(tester)
        self.topic = tester
        self.start_producer(2, throughput=10000)
        self.start_consumer(1)
        self.await_startup()

        self.run_validation(min_records=100000,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=300)

        # make sure that all redpanda nodes are up and running
        for n in self.redpanda.nodes:
            assert self.redpanda.redpanda_pid(n) != None
