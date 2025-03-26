# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import datetime
import re
import time
from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool, TopicSpec
from rptest.services.cluster import cluster
from random import randint

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from rptest.services.redpanda import PandaproxyConfig, SchemaRegistryConfig, SISettings, MetricsEndpoint
from rptest.services.redpanda import CloudStorageType, SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.datalake.catalog_service_factory import supported_catalog_types, filesystem_catalog_type
from ducktape.mark import matrix, ignore
from ducktape.utils.util import wait_until
from rptest.services.metrics_check import MetricCheck
from rptest.services.catalog_service import CatalogType


class DatalakeThrottlingTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeThrottlingTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            environment={
                "__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"
            },
            *args,
            **kwargs)
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def _total_throttle(self):
        total = 0
        sample = self.redpanda.metrics_sample("total_throttle")
        assert sample is not None, "total_throttle metric not found"
        for s in sample.samples:
            self.logger.debug(f"metrics sample: {s}")
            total += s.value
        return total

    def producer_throttled(self, dl: DatalakeServices):
        dl.produce_to_topic(self.topic_name, 128, 10240)
        throttle = self._total_throttle()
        return throttle > 0

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types(),
            catalog_type=supported_catalog_types())
    def test_basic_throttling(self, cloud_storage_type, catalog_type):
        msg_cnt = 100
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              include_query_engines=[QueryEngineType.TRINO],
                              catalog_type=catalog_type) as dl:

            non_iceberg_topic = TopicSpec(partition_count=1,
                                          replication_factor=1)
            rpk = RpkTool(self.redpanda)
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1)
            DefaultClient(self.redpanda).create_topic(non_iceberg_topic)
            dl.produce_to_topic(self.topic_name, 1024, msg_cnt)
            dl.wait_for_translation(self.topic_name, msg_count=msg_cnt)
            assert self._total_throttle(
            ) == 0, "There should be no throttling in baseline conditions"

            # Block translation by setting the max number of translations to 0
            self.redpanda.set_cluster_config({
                "datalake_scheduler_max_concurrent_translations":
                0,
                "iceberg_target_backlog_size":
                1000
            })

            # Produce some more messages
            wait_until(lambda: self.producer_throttled(dl), timeout_sec=60)

            # Validate that non Iceberg related producers are not throttled
            current_throttle = self._total_throttle()
            dl.produce_to_topic(non_iceberg_topic.name, 1024, 10)
            assert self._total_throttle(
            ) == current_throttle, "Total throttle should not increase as the topic is not iceberg enabled"
            # Enable translation back
            self.redpanda.set_cluster_config(
                {"datalake_scheduler_max_concurrent_translations": 4})
            partitions = rpk.describe_topic(self.topic_name)
            total_messages = 0
            for p in partitions:
                total_messages += p.high_watermark
            self.logger.info(
                f"waiting for translation of {total_messages} messages")
            dl.wait_for_translation(self.topic_name, msg_count=total_messages)
            dl.produce_to_topic(self.topic_name, 1024, msg_cnt)
            assert self._total_throttle(
            ) == current_throttle, "Total throttle should not increase as the translation is progressing"
