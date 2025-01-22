# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from time import time
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from random import randint

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from rptest.services.redpanda import PandaproxyConfig, SchemaRegistryConfig, SISettings
from rptest.services.redpanda import CloudStorageType, SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.metrics_check import MetricCheck

avro_schema_str = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "number", "type": "long"},
        {"name": "timestamp_us", "type": {"type": "long", "logicalType": "timestamp-micros"}}
    ]
}
"""


class DatalakeE2ETests(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeE2ETests,
              self).__init__(test_ctx,
                             num_brokers=1,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={
                                 "iceberg_enabled": "true",
                                 "iceberg_catalog_commit_interval_ms": 5000
                             },
                             schema_registry_config=SchemaRegistryConfig(),
                             pandaproxy_config=PandaproxyConfig(),
                             *args,
                             **kwargs)
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types(),
            query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO],
            filesystem_catalog_mode=[False, True])
    def test_e2e_basic(self, cloud_storage_type, query_engine,
                       filesystem_catalog_mode):
        # Create a topic
        # Produce some events
        # Ensure they end up in datalake
        count = 100
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=filesystem_catalog_mode,
                              include_query_engines=[query_engine]) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=count)

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=supported_storage_types(),
            query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO])
    def test_avro_schema(self, cloud_storage_type, query_engine):
        count = 100
        table_name = f"redpanda.{self.topic_name}"

        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=True,
                              include_query_engines=[query_engine]) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name, iceberg_mode="value_schema_id_prefix")

            schema = avro.loads(avro_schema_str)
            producer = AvroProducer(
                {
                    'bootstrap.servers': self.redpanda.brokers(),
                    'schema.registry.url':
                    self.redpanda.schema_reg().split(",")[0]
                },
                default_value_schema=schema)
            for _ in range(count):
                t = time()
                record = {"number": int(t), "timestamp_us": int(t * 1000000)}
                producer.produce(topic=self.topic_name, value=record)
            producer.flush()
            dl.wait_for_translation(self.topic_name, msg_count=count)

            if query_engine == QueryEngineType.TRINO:
                trino = dl.trino()
                trino_expected_out = [(
                    'redpanda',
                    'row(partition integer, offset bigint, timestamp timestamp(6), headers array(row(key varbinary, value varbinary)), key varbinary)',
                    '', ''), ('number', 'bigint', '', ''),
                                      ('timestamp_us', 'timestamp(6)', '', '')]
                trino_describe_out = trino.run_query_fetch_all(
                    f"describe {table_name}")
                assert trino_describe_out == trino_expected_out, str(
                    trino_describe_out)
            else:
                spark = dl.spark()
                spark_expected_out = [(
                    'redpanda',
                    'struct<partition:int,offset:bigint,timestamp:timestamp_ntz,headers:array<struct<key:binary,value:binary>>,key:binary>',
                    None), ('number', 'bigint', None),
                                      ('timestamp_us', 'timestamp_ntz', None),
                                      ('', '', ''), ('# Partitioning', '', ''),
                                      ('Part 0', 'hours(redpanda.timestamp)',
                                       '')]
                spark_describe_out = spark.run_query_fetch_all(
                    f"describe {table_name}")
                assert spark_describe_out == spark_expected_out, str(
                    spark_describe_out)

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_upload_after_external_update(self, cloud_storage_type):
        table_name = f"redpanda.{self.topic_name}"
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=True,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:
            count = 100
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, count)
            spark = dl.spark()
            spark.make_client().cursor().execute(f"delete from {table_name}")
            count_after_del = spark.count_table("redpanda", self.topic_name)
            assert count_after_del == 0, f"{count_after_del} rows, expected 0"

            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation_until_offset(self.topic_name,
                                                 2 * count - 1)
            count_after_produce = spark.count_table("redpanda",
                                                    self.topic_name)
            assert count_after_produce == count, f"{count_after_produce} rows, expected {count}"

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_remove_expired_snapshots(self, cloud_storage_type):
        table_name = f"redpanda.{self.topic_name}"
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=True,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1)
            count = 0
            records_per_round = 100
            num_rounds = 5
            for _ in range(num_rounds):
                count += records_per_round
                dl.produce_to_topic(self.topic_name, 1024, records_per_round)

                # Waiting for rows to be visible here ensures that at least one
                # new snapshot is written in each round.
                dl.wait_for_translation(self.topic_name, count)

            spark = dl.spark()
            snapshots_out = spark.run_query_fetch_all(
                f"select * from {table_name}.snapshots")
            assert len(
                snapshots_out
            ) >= num_rounds, f"Expected >={num_rounds} snapshots, got {len(snapshots_out)}: {snapshots_out}"

            spark.make_client().cursor().execute(
                f"alter table {table_name} "
                "set tblproperties ('history.expire.max-snapshot-age-ms'='1000')"
            )

            def has_one_snapshot():
                snapshots_out = spark.run_query_fetch_all(
                    f"select * from {table_name}.snapshots")
                return len(snapshots_out) == 1

            wait_until(has_one_snapshot, timeout_sec=30, backoff_sec=1)
            dl.wait_for_translation(self.topic_name, count)

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types(),
            filesystem_catalog_mode=[True, False])
    def test_topic_lifecycle(self, cloud_storage_type,
                             filesystem_catalog_mode):
        count = 100
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=filesystem_catalog_mode,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:
            rpk = RpkTool(self.redpanda)

            # produce some data then delete the topic
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=count)

            rpk.alter_topic_config(self.topic_name, "redpanda.iceberg.delete",
                                   "false")
            rpk.delete_topic(self.topic_name)

            # table is not deleted, it will contain messages from both topic instances
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=15)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=2 * count)

            # now table should be deleted
            rpk.delete_topic(self.topic_name)

            catalog_client = dl.catalog_client()

            def table_deleted():
                return not dl.table_exists(self.topic_name,
                                           client=catalog_client)

            wait_until(table_deleted,
                       timeout_sec=30,
                       backoff_sec=5,
                       err_msg="table was not deleted")

            # recreate an empty topic a few times
            for _ in range(3):
                dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
                rpk.delete_topic(self.topic_name)

            # check that the table is recreated after we start producing again
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=5)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=count)


class DatalakeMetricsTest(RedpandaTest):

    commit_lag = 'vectorized_cluster_partition_iceberg_offsets_pending_commit'
    translation_lag = 'vectorized_cluster_partition_iceberg_offsets_pending_translation'

    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeMetricsTest,
              self).__init__(test_ctx,
                             num_brokers=3,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={
                                 "iceberg_enabled": "true",
                                 "iceberg_catalog_commit_interval_ms": "5000",
                                 "enable_leader_balancer": False
                             },
                             schema_registry_config=SchemaRegistryConfig(),
                             pandaproxy_config=PandaproxyConfig(),
                             *args,
                             **kwargs)
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        pass

    def wait_for_lag(self, metric_check: MetricCheck, metric_name: str,
                     count: int):
        wait_until(
            lambda: metric_check.evaluate([(metric_name, lambda _, val: val ==
                                            count)]),
            timeout_sec=30,
            backoff_sec=5,
            err_msg=f"Timed out waiting for {metric_name} to reach: {count}")

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_lag_metrics(self, cloud_storage_type):

        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=False,
                              include_query_engines=[]) as dl:

            # Stop the catalog to halt the translation flow
            dl.catalog_service.stop()

            dl.create_iceberg_enabled_topic(self.topic_name,
                                            partitions=1,
                                            replicas=3)
            topic_leader = self.redpanda.partitions(self.topic_name)[0].leader
            count = randint(12, 21)
            dl.produce_to_topic(self.topic_name, 1, msg_count=count)

            m = MetricCheck(self.redpanda.logger,
                            self.redpanda,
                            topic_leader, [
                                DatalakeMetricsTest.commit_lag,
                                DatalakeMetricsTest.translation_lag
                            ],
                            labels={
                                'namespace': 'kafka',
                                'topic': self.topic_name,
                                'partition': '0'
                            },
                            reduce=sum)

            # Wait for lag build up
            self.wait_for_lag(m, DatalakeMetricsTest.translation_lag, count)
            self.wait_for_lag(m, DatalakeMetricsTest.commit_lag, count)

            # Resume iceberg translation
            dl.catalog_service.start()

            self.wait_for_lag(m, DatalakeMetricsTest.translation_lag, 0)
            self.wait_for_lag(m, DatalakeMetricsTest.commit_lag, 0)
