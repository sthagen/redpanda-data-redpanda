# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import dataclasses
from typing import Callable

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from databricks.sql.types import Row
from ducktape.mark import matrix
from ducktape.mark._mark import Mark

from rptest.clients.rpk import RpkTool
from rptest.context.databricks import DatabricksContext as DatabricksContext
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    PandaproxyConfig,
    SchemaRegistryConfig,
    SISettings,
    get_cloud_provider,
)
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest


def fetch_dbx_schema(dl: DatalakeServices, table_name: str) -> list[Row]:
    return dl.query_engine(
        QueryEngineType.DATABRICKS_SQL).make_client().cursor().execute(
            f"describe {table_name}").fetchall()


class DatabricksOnlyTestMark(Mark):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def apply(self, seed_context, context_list):
        """
        Apply the mark to the test context list.
        This will skip the test if the Databricks context is not available.
        """
        assert len(
            context_list
        ) > 0, "ignore annotation is not being applied to any test cases"

        should_ignore_test = False
        if not DatabricksContext.available(seed_context):
            seed_context.logger.debug(
                f"Skipping {seed_context} test because Databricks context is not available"
            )
            should_ignore_test = True
        elif get_cloud_provider() != "aws":
            seed_context.logger.debug(
                f"Skipping {seed_context} test because it is only supported on AWS, but the current cloud provider is {get_cloud_provider()}"
            )
            should_ignore_test = True

        for ctx in context_list:
            ctx.ignore = should_ignore_test

        return context_list


def databricks_only_test(func, /):
    """
    Decorator to mark a test as a Databricks test.
    Such tests will only run if the Databricks context is available.
    """

    Mark.mark(func, DatabricksOnlyTestMark())
    return func


class DatabricksTest(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context,
            num_brokers=1,
            si_settings=SISettings(
                test_context,
                # Temporary workaround:
                # Skip because we don't always run redpanda/setup tests at all and it fails
                # to cleanup. Will be fixed once we will avoid entering the tests at all if
                # they shouldn't run.
                skip_end_of_test_scrubbing=True),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            *args,
            **kwargs)
        self.test_context = test_context
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    @databricks_only_test
    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_e2e_basic(self, cloud_storage_type):
        count = 100
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              include_query_engines=[
                                  QueryEngineType.DATABRICKS_SQL,
                              ],
                              catalog_type=CatalogType.DATABRICKS_UNITY) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
            dl.produce_to_topic(self.topic_name, 1024, count)

            dl.wait_for_translation(self.topic_name,
                                    msg_count=count,
                                    timeout=60)

            actual_schema = fetch_dbx_schema(dl, f"redpanda.{self.topic_name}")
            expected_schema = [
                Row(col_name='redpanda',
                    data_type=
                    'struct<partition:int,offset:bigint,timestamp:timestamp_ntz,headers:array<struct<key:binary,value:binary>>,key:binary>',
                    comment=None),
                Row(col_name='value', data_type='binary', comment=None),
                Row(col_name='# Clustering Information',
                    data_type='',
                    comment=''),
                Row(col_name='# col_name',
                    data_type='data_type',
                    comment='comment'),
                Row(col_name='redpanda.timestamp',
                    data_type='timestamp_ntz',
                    comment=None)
            ]
            assert actual_schema == expected_schema, \
                f"Expected DBX schema {expected_schema} but got {actual_schema}"

            DatalakeVerifier.oneshot(
                self.redpanda, self.topic_name,
                dl.query_engine(QueryEngineType.DATABRICKS_SQL))

    @databricks_only_test
    @cluster(num_nodes=1)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_e2e_with_schema(self, cloud_storage_type):
        count = 100

        @dataclasses.dataclass
        class TestCase:
            schema_str: str
            record_generator: Callable[[], object]
            dbx_schema: list[Row]

        test_cases = {
            "root_primitive":
            TestCase(
                schema_str="""{"type": "long", "name": "a_number"}""",
                record_generator=lambda: 42,
                dbx_schema=[
                    Row(col_name='redpanda',
                        data_type=
                        'struct<partition:int,offset:bigint,timestamp:timestamp_ntz,headers:array<struct<key:binary,value:binary>>,key:binary>',
                        comment=None),
                    Row(col_name='root', data_type='bigint', comment=None),
                    Row(col_name='# Clustering Information',
                        data_type='',
                        comment=''),
                    Row(col_name='# col_name',
                        data_type='data_type',
                        comment='comment'),
                    Row(col_name='redpanda.timestamp',
                        data_type='timestamp_ntz',
                        comment=None)
                ]),
            "object_with_primitives":
            TestCase(
                schema_str="""{
                         "type": "record",
                         "name": "primitives",
                         "fields": [
                             {"name": "id", "type": "long" },
                             {"name": "name", "type": "string" }
                         ]}""",
                record_generator=lambda: {
                    "id": 42,
                    "name": "test_name"
                },
                dbx_schema=[
                    Row(col_name='redpanda',
                        data_type=
                        'struct<partition:int,offset:bigint,timestamp:timestamp_ntz,headers:array<struct<key:binary,value:binary>>,key:binary>',
                        comment=None),
                    Row(col_name='id', data_type='bigint', comment=None),
                    Row(col_name='name', data_type='string', comment=None),
                    Row(col_name='# Clustering Information',
                        data_type='',
                        comment=''),
                    Row(col_name='# col_name',
                        data_type='data_type',
                        comment='comment'),
                    Row(col_name='redpanda.timestamp',
                        data_type='timestamp_ntz',
                        comment=None)
                ]),
        }

        with DatalakeServices(
                self.test_context,
                redpanda=self.redpanda,
                include_query_engines=[QueryEngineType.DATABRICKS_SQL],
                catalog_type=CatalogType.DATABRICKS_UNITY) as dl:
            for tc_name, tc in test_cases.items():
                self.redpanda.logger.debug(
                    f"Running avro schema test case {tc_name}")
                test_case_topic_name = f"{tc_name}_test_case"
                table_name = f"redpanda.{test_case_topic_name}"
                dl.create_iceberg_enabled_topic(
                    test_case_topic_name,
                    iceberg_mode="value_schema_id_prefix")
                raw_schema = avro.loads(tc.schema_str)
                producer = AvroProducer(
                    {
                        'bootstrap.servers':
                        self.redpanda.brokers(),
                        'schema.registry.url':
                        self.redpanda.schema_reg().split(",")[0]
                    },
                    default_value_schema=raw_schema)
                for _ in range(count):
                    producer.produce(topic=test_case_topic_name,
                                     value=tc.record_generator())
                producer.flush()
                dl.wait_for_translation(test_case_topic_name, msg_count=count)

                actual_schema = fetch_dbx_schema(dl, table_name)
                assert actual_schema == tc.dbx_schema, \
                    f"Expected DBX schema {tc.dbx_schema} but got {actual_schema}"

    @databricks_only_test
    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_e2e_with_partition_evolution(self, cloud_storage_type):
        with DatalakeServices(
                self.test_context,
                redpanda=self.redpanda,
                include_query_engines=[QueryEngineType.DATABRICKS_SQL],
                catalog_type=CatalogType.DATABRICKS_UNITY) as dl:

            dl.create_iceberg_enabled_topic(
                self.topic_name,
                config={"redpanda.iceberg.partition.spec": "()"})

            produced_total = 0

            def produce_some_and_wait_for_translation():
                num_msgs = 100
                nonlocal produced_total

                dl.produce_to_topic(self.topic_name, 1024, num_msgs)
                produced_total += num_msgs

                dl.wait_for_translation(self.topic_name,
                                        msg_count=produced_total,
                                        timeout=60)

            self.logger.info(
                "Producing data to the topic with no partitioning")
            produce_some_and_wait_for_translation()

            rpk = RpkTool(self.redpanda)

            self.logger.info(
                "Producing data to the topic with partitioning by hour and bucket"
            )
            rpk.alter_topic_config(
                self.topic_name, "redpanda.iceberg.partition.spec",
                "(hour(redpanda.timestamp), bucket(4, redpanda.offset))")
            produce_some_and_wait_for_translation()

            self.logger.info(
                "Producing data to the topic with partitioning by bucket only")
            rpk.alter_topic_config(self.topic_name,
                                   "redpanda.iceberg.partition.spec",
                                   "(bucket(4, redpanda.offset))")

            produce_some_and_wait_for_translation()

            actual_schema = fetch_dbx_schema(dl, f"redpanda.{self.topic_name}")
            expected_schema = [
                Row(col_name='redpanda',
                    data_type=
                    'struct<partition:int,offset:bigint,timestamp:timestamp_ntz,headers:array<struct<key:binary,value:binary>>,key:binary>',
                    comment=None),
                Row(col_name='value', data_type='binary', comment=None),
                Row(col_name='# Clustering Information',
                    data_type='',
                    comment=''),
                Row(col_name='# col_name',
                    data_type='data_type',
                    comment='comment'),
                Row(col_name='redpanda.offset',
                    data_type='bigint',
                    comment=None)
            ]
            assert actual_schema == expected_schema, \
                f"Expected DBX schema {expected_schema} but got {actual_schema}"

            DatalakeVerifier.oneshot(
                self.redpanda, self.topic_name,
                dl.query_engine(QueryEngineType.DATABRICKS_SQL))

    # This test does not work because Iceberg tables in the managed catalog
    # w/ their databricks sql engine are read only. I.e. there is no support
    # for DELETE statements. They fail with Read Iceberg with Delta Uniform
    # has failed. Operation is not supported. Only CREATE and REFRESH are
    # supported on Uniform Iceberg Ingress Table.
    #
    # @cluster(num_nodes=4)
    # @matrix(cloud_storage_type=supported_storage_types())
    # def test_upload_after_external_update(self, cloud_storage_type):
    #     # TODO: Move this in the matrix decorator. Somehow.
    #     if not DatabricksContext.available(self.test_context):
    #         self.logger.warning(
    #             "Skipping test because Databricks context is not available")
    #         cleanup_on_early_exit(self)
    #         return

    #     table_name = f"redpanda.{self.topic_name}"
    #     with DatalakeServices(self.test_context,
    #                           redpanda=self.redpanda,
    #                           include_query_engines=[
    #                               QueryEngineType.DATABRICKS_SQL,
    #                           ],
    #                           catalog_type=CatalogType.DATABRICKS_UNITY) as dl:
    #         count = 100
    #         dl.create_iceberg_enabled_topic(self.topic_name, partitions=1)
    #         dl.produce_to_topic(self.topic_name, 1024, count)
    #         dl.wait_for_translation(self.topic_name, count)

    #         query_engine = dl.query_engine(QueryEngineType.DATABRICKS_SQL)
    #         query_engine.make_client().cursor().execute(
    #             f"delete from {table_name}")

    #         count_after_del = query_engine.count_table("redpanda",
    #                                                    self.topic_name)
    #         assert count_after_del == 0, f"{count_after_del} rows, expected 0"

    #         dl.produce_to_topic(self.topic_name, 1024, count)
    #         dl.wait_for_translation_until_offset(self.topic_name,
    #                                              2 * count - 1)
    #         count_after_produce = query_engine.count_table(
    #             "redpanda", self.topic_name)
    #         assert count_after_produce == count, f"{count_after_produce} rows, expected {count}"
