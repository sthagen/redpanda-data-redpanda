# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from ducktape.mark import matrix

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.tests.datalake.catalog_service_factory import (
    filesystem_catalog_type,
)
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest

SCHEMA_A = {
    "type": "record",
    "name": "RecordA",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
    ],
}

SCHEMA_B = {
    "type": "record",
    "name": "RecordB",
    "fields": [
        {"name": "color", "type": "string"},
        {"name": "size", "type": "double"},
    ],
}

SCHEMA_C = {
    "type": "record",
    "name": "RecordC",
    "fields": [
        {"name": "x", "type": "int"},
    ],
}


class DatalakeSchemaRegistryContextTest(RedpandaTest):
    def __init__(self, test_context):
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 5000,
                "schema_registry_enable_qualified_subjects": True,
            },
            schema_registry_config=SchemaRegistryConfig(),
            si_settings=SISettings(test_context=test_context),
        )

    def setUp(self):
        # DatalakeServices starts Redpanda.
        pass

    def _sr_url(self):
        return self.redpanda.schema_reg().split(",")[0]

    def _produce_confluent_records(self, topic, context, schema_dict, records):
        """Produce Avro records using AvroSerializer with a context-qualified
        subject name strategy.  This mirrors how a real producer would use SR
        contexts: the serializer registers/looks up the schema under the
        context-qualified subject (e.g. ':.ctx1:topic-value') and encodes
        records in standard Confluent wire format."""
        sr_client = SchemaRegistryClient({"url": self._sr_url()})

        def subject_name_strategy(ctx, schema):
            return f":{context}:{ctx.topic}-value"

        avro_serializer = AvroSerializer(
            sr_client,
            json.dumps(schema_dict),
            conf={"subject.name.strategy": subject_name_strategy},
        )

        producer = SerializingProducer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "value.serializer": avro_serializer,
            }
        )
        for record in records:
            producer.produce(topic, value=record)
        producer.flush()

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_per_topic_context_routing(self, cloud_storage_type):
        """Each topic resolves schemas from its configured SR context."""

        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            catalog_type=filesystem_catalog_type(),
            include_query_engines=[QueryEngineType.SPARK],
        ) as dl:
            # Create topics with per-topic SR context.
            dl.create_iceberg_enabled_topic(
                "topic_a",
                iceberg_mode="value_schema_id_prefix",
                config={
                    TopicSpec.PROPERTY_SCHEMA_REGISTRY_CONTEXT: ".ctx1",
                },
            )
            dl.create_iceberg_enabled_topic(
                "topic_b",
                iceberg_mode="value_schema_id_prefix",
                config={
                    TopicSpec.PROPERTY_SCHEMA_REGISTRY_CONTEXT: ".ctx2",
                },
            )

            # Produce records — AvroSerializer registers SCHEMA_A under
            # .ctx1 and SCHEMA_B under .ctx2.
            records_a = [{"name": f"user_{i}", "age": 20 + i} for i in range(10)]
            records_b = [{"color": f"color_{i}", "size": float(i)} for i in range(10)]

            self._produce_confluent_records("topic_a", ".ctx1", SCHEMA_A, records_a)
            self._produce_confluent_records("topic_b", ".ctx2", SCHEMA_B, records_b)

            # Wait for translation.
            dl.wait_for_translation("topic_a", msg_count=10)
            dl.wait_for_translation("topic_b", msg_count=10)

            # Verify Iceberg table columns.
            spark = dl.spark()

            desc_a = spark.run_query_fetch_all("describe redpanda.topic_a")
            # Spark describe returns header row at [0] and partition info
            # in the last 3 rows; strip those.
            cols_a = {(r[0], r[1]) for r in desc_a[1:-3]}
            assert ("name", "string") in cols_a, (
                f"Expected 'name' string column in topic_a, got {cols_a}"
            )
            assert ("age", "int") in cols_a, (
                f"Expected 'age' int column in topic_a, got {cols_a}"
            )

            desc_b = spark.run_query_fetch_all("describe redpanda.topic_b")
            cols_b = {(r[0], r[1]) for r in desc_b[1:-3]}
            assert ("color", "string") in cols_b, (
                f"Expected 'color' string column in topic_b, got {cols_b}"
            )
            assert ("size", "double") in cols_b, (
                f"Expected 'size' double column in topic_b, got {cols_b}"
            )

            # Negative: topic_a should NOT have topic_b's columns.
            assert ("color", "string") not in cols_a, (
                "topic_a should not have topic_b's 'color' column"
            )
            assert ("name", "string") not in cols_b, (
                "topic_b should not have topic_a's 'name' column"
            )

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_wrong_context_dlq(self, cloud_storage_type):
        """Schema ID not present in the configured context sends records
        to the dead-letter-queue table."""

        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            catalog_type=filesystem_catalog_type(),
            include_query_engines=[QueryEngineType.SPARK],
        ) as dl:
            # Create topic pointing to a context where the schema does
            # not exist.
            dl.create_iceberg_enabled_topic(
                "topic_c",
                iceberg_mode="value_schema_id_prefix",
                config={
                    TopicSpec.PROPERTY_SCHEMA_REGISTRY_CONTEXT: ".wrong",
                    TopicSpec.PROPERTY_ICEBERG_INVALID_RECORD_ACTION: "dlq_table",
                },
            )

            # Produce records encoded with the schema ID from .ctx1.
            # The translator resolves IDs against .wrong, where SCHEMA_C
            # does not exist, so all records are routed to the DLQ.
            records = [{"x": i} for i in range(10)]
            self._produce_confluent_records("topic_c", ".ctx1", SCHEMA_C, records)

            # Records should land in the DLQ table, not the main table.
            dl.wait_for_translation(
                "topic_c",
                msg_count=10,
                table_override="topic_c~dlq",
            )
            assert dl.num_tables() == 1, (
                f"Expected only the DLQ table, got {dl.num_tables()} tables"
            )
