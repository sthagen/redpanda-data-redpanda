# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import itertools
import time
import random
from uuid import uuid4

from requests.exceptions import HTTPError
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from ducktape.mark import matrix

from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest


class DatalakeCustomPartitioningConfigTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeCustomPartitioningConfigTest,
              self).__init__(test_ctx,
                             num_brokers=1,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={"iceberg_enabled": True},
                             *args,
                             **kwargs)

    @cluster(num_nodes=1)
    def test_configs(self):
        rpk = RpkTool(self.redpanda)
        try:
            rpk.create_topic("foo",
                             config={
                                 "redpanda.iceberg.mode":
                                 "value_schema_id_prefix",
                                 "redpanda.iceberg.partition.spec":
                                 "(hour(field"
                             })
        except RpkException:
            pass
        else:
            assert False, "creating topic with invalid spec should be forbidden"

        # should succeed
        rpk.create_topic("foo",
                         config={
                             "redpanda.iceberg.mode": "value_schema_id_prefix",
                             "redpanda.iceberg.partition.spec": "(hour(field))"
                         })

        try:
            rpk.alter_topic_config("foo", "redpanda.iceberg.partition.spec",
                                   "(unknown_transform(field))")
        except RpkException:
            pass
        else:
            assert False, "altering spec to invalid string should be forbidden"

        for spec in [
                "((unparseable", "(not_redpanda_field)", "(redpanda.offset)"
        ]:
            try:
                self.redpanda.set_cluster_config(
                    {"iceberg_default_partition_spec": spec})
            except HTTPError as e:
                if e.response.status_code != 400:
                    raise
            else:
                assert False, "setting default spec to invalid string should be forbidden"

        # should succeed
        self.redpanda.set_cluster_config(
            {"iceberg_default_partition_spec": "(day(redpanda.timestamp))"})

        # topic with default spec
        rpk.create_topic(
            "bar", config={"redpanda.iceberg.mode": "value_schema_id_prefix"})

        topic_configs = rpk.describe_topic_configs("bar")
        assert topic_configs["redpanda.iceberg.partition.spec"][0] == \
            "(day(redpanda.timestamp))"


AVRO_SCHEMA_STR = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "event_type", "type": "string"},
        {"name": "number", "type": "long"},
        {"name": "timestamp_us", "type": {"type": "long", "logicalType": "timestamp-micros"}}
    ]
}
"""


class DatalakeCustomPartitioningTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeCustomPartitioningTest,
              self).__init__(test_ctx,
                             num_brokers=4,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={
                                 "iceberg_enabled": True,
                                 "iceberg_catalog_commit_interval_ms": 5000,
                             },
                             schema_registry_config=SchemaRegistryConfig(),
                             *args,
                             **kwargs)

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types(),
            filesystem_catalog_mode=[False, True])
    def test_basic(self, cloud_storage_type, filesystem_catalog_mode):
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=filesystem_catalog_mode,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:
            # Create an iceberg topic with a custom partition spec and produce
            # some data.

            topic_name = "foo"
            msg_count = 1000
            partitions = 5
            dl.create_iceberg_enabled_topic(
                topic_name,
                partitions=partitions,
                iceberg_mode="value_schema_id_prefix",
                config={
                    "redpanda.iceberg.partition.spec":
                    "(hour(timestamp_us), event_type)"
                })

            value_serializer = AvroSerializer(
                SchemaRegistryClient(
                    {"url": self.redpanda.schema_reg().split(",")[0]}),
                AVRO_SCHEMA_STR)
            producer = SerializingProducer({
                'bootstrap.servers':
                self.redpanda.brokers(),
                'key.serializer':
                StringSerializer('utf_8'),
                'value.serializer':
                value_serializer,
            })

            # Have all records share the same timestamp, so that they are
            # guaranteed to end up in the same hour partition.
            timestamp = time.time()
            for i in range(msg_count):
                ev_type = random.choice(["type_A", "type_B"])
                record = {
                    "event_type": ev_type,
                    "number": i,
                    "timestamp_us": int(timestamp * 1000000),
                }
                producer.produce(
                    topic=topic_name,
                    # key to ensure that all partitions get some records
                    key=str(uuid4()),
                    value=record)
            producer.flush()
            dl.wait_for_translation(topic_name, msg_count=msg_count)

            # Check that created table has the correct partition spec.

            table_name = f"redpanda.{topic_name}"
            spark = dl.spark()
            spark_describe_out = spark.run_query_fetch_all(
                f"describe {table_name}")
            describe_partitioning = list(
                itertools.dropwhile(lambda r: r[0] != "# Partitioning",
                                    spark_describe_out))
            expected_partitioning = [
                ('# Partitioning', '', ''),
                ('Part 0', 'hours(timestamp_us)', ''),
                ('Part 1', 'event_type', ''),
            ]
            assert describe_partitioning == expected_partitioning

            # Check that files are correctly partitioned and that
            # spark can use this partitioning for a delete query.

            files_before = set(
                spark.run_query_fetch_all(
                    f"select file_path from {table_name}.files"))
            # The translator for each partition should produce a file for
            # each of 2 event types.
            assert len(files_before) == partitions * 2

            spark.make_client().cursor().execute(
                f"delete from {table_name} where event_type='type_A'")

            files_after = set(
                spark.run_query_fetch_all(
                    f"select file_path from {table_name}.files"))
            assert len(files_after) == partitions
            assert files_after.issubset(files_before)
