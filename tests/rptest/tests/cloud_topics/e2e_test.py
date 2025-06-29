# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import random
import re
import time

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, get_cloud_storage_type, make_redpanda_service
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import Scale


class EndToEndCloudTopicsBase(EndToEndTest):
    s3_topic_name = "panda_topic"

    num_brokers = 3

    topics = (TopicSpec(
        name=s3_topic_name,
        partition_count=5,
        replication_factor=3,
    ), )

    def __init__(self, test_context, extra_rp_conf=None, environment=None):
        super(EndToEndCloudTopicsBase,
              self).__init__(test_context=test_context)

        self.test_context = test_context
        self.topic = self.s3_topic_name

        conf = dict(
            enable_developmental_unrecoverable_data_corrupting_features=int(
                time.time()),
            development_enable_cloud_topics=True,
            enable_cluster_metadata_upload_loop=False)

        if extra_rp_conf:
            for k, v in conf.items():
                extra_rp_conf[k] = v
        else:
            extra_rp_conf = conf

        self.si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )
        self.s3_bucket_name = self.si_settings.cloud_storage_bucket
        self.si_settings.load_context(self.logger, test_context)
        self.scale = Scale(test_context)

        self.redpanda = make_redpanda_service(context=self.test_context,
                                              num_brokers=self.num_brokers,
                                              si_settings=self.si_settings,
                                              extra_rp_conf=extra_rp_conf,
                                              environment=environment)
        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        for topic in self.topics:
            self.rpk.create_topic(topic=topic.name,
                                  partitions=topic.partition_count,
                                  replicas=topic.replication_factor,
                                  config={
                                      "redpanda.cloud_topic.enabled": "true",
                                  })


class EndToEndCloudTopicsTest(EndToEndCloudTopicsBase):
    def __init__(self, test_context, extra_rp_conf=None, env=None):
        super(EndToEndCloudTopicsTest, self).__init__(test_context,
                                                      extra_rp_conf, env)

    def await_num_produced(self, min_records, timeout_sec=120):
        wait_until(lambda: self.producer.num_acked > min_records,
                   timeout_sec=timeout_sec,
                   err_msg="Producer failed to produce messages for %ds." %\
                   timeout_sec)

    @cluster(num_nodes=5)
    def test_write(self):

        self.start_producer()

        self.await_num_produced(min_records=50000)

        self.start_consumer()
        self.run_validation()
