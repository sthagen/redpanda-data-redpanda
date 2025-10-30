# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

from confluent_kafka import Producer
from ducktape.mark import matrix
from ducktape.mark._mark import Mark

from rptest.clients.rpk import RpkTool
from rptest.context.gcp import GCPContext
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    PandaproxyConfig,
    SISettings,
    SchemaRegistryConfig,
)
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until


class GCPOnlyTestMark(Mark):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def apply(self, seed_context, context_list):
        """
        Apply the mark to the test context list.
        This will skip the test if the GCP context is not available.
        """
        assert len(context_list) > 0, (
            "ignore annotation is not being applied to any test cases"
        )

        should_ignore_test = False
        if not GCPContext.available(seed_context):
            seed_context.logger.debug(
                f"Skipping {seed_context} test because GCP context is not available"
            )
            should_ignore_test = True

        for ctx in context_list:
            ctx.ignore = should_ignore_test

        return context_list


def gcp_only_test(func, /):
    """
    Decorator to mark a test as a Google Cloud Platform test.
    Such tests will only run if the GCP context is available. I.e. we run
    in a GCP environment.
    """

    Mark.mark(func, GCPOnlyTestMark())
    return func


class BiglakeTest(RedpandaTest):
    dlq_table_suffix = "__panda_dlq"

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context,
            num_brokers=1,
            si_settings=SISettings(test_context),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
                "iceberg_dlq_table_suffix": self.dlq_table_suffix,
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            *args,
            **kwargs,
        )
        self.test_context = test_context
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def count_rows(self, dl: DatalakeServices, table_name: str) -> int:
        t = dl.catalog_client().load_table(f"redpanda.{table_name}")
        df = t.scan().to_duckdb("data")
        r = df.sql("SELECT count(*) FROM data").fetchone()
        self.logger.info(f"Row count for {table_name}: {r[0]}")
        return r[0]

    def wait_rows_count(
        self,
        dl: DatalakeServices,
        table_name: str,
        expected_count: int,
        timeout_sec: int = 60,
    ):
        wait_until(
            lambda: dl.catalog_client().table_exists(f"redpanda.{table_name}"),
            timeout_sec=timeout_sec,
            backoff_sec=1,
        )

        wait_until(
            lambda: self.count_rows(dl, table_name) == expected_count,
            timeout_sec=timeout_sec,
            backoff_sec=1,
        )

    @gcp_only_test
    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_e2e_basic(self, cloud_storage_type):
        count = 100
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[],
            catalog_type=CatalogType.BIGLAKE,
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
            dl.produce_to_topic(self.topic_name, 1024, count)

            self.wait_rows_count(dl, self.topic_name, count, timeout_sec=60)

    @gcp_only_test
    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_dlq(self, cloud_storage_type):
        count = 10
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[],
            catalog_type=CatalogType.BIGLAKE,
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name, partitions=1, iceberg_mode="value_schema_latest"
            )

            self.logger.info(f"Creating schema for topic {self.topic_name}")
            rpk = RpkTool(self.redpanda)
            rpk.create_schema_from_str(
                subject=f"{self.topic_name}-value",
                schema="""
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "type": "object",
                  "properties": {
                    "id": { "type": "integer" }
                  }
                }
                """,
                schema_suffix="json",
            )

            self.logger.info(f"Producing {count} invalid messages to {self.topic_name}")
            dl.produce_to_topic(self.topic_name, 1024, count)

            self.logger.info(f"Producing {count} valid messages to {self.topic_name}")
            producer = Producer({"bootstrap.servers": self.redpanda.brokers()})
            for i in range(count):
                producer.produce(
                    self.topic_name,
                    value=json.dumps({"id": i}),
                )
            producer.flush()

            self.logger.info("Waiting for DLQ table to have expected rows")
            self.wait_rows_count(
                dl, f"{self.topic_name}{self.dlq_table_suffix}", count, timeout_sec=60
            )

            self.logger.info("Waiting for main table to have expected rows")
            self.wait_rows_count(dl, self.topic_name, count, timeout_sec=60)
