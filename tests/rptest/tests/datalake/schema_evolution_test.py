# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import tempfile
from time import time
from typing import NamedTuple
from collections.abc import Callable
import pyhive
from contextlib import contextmanager
import json

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import PandaproxyConfig, SISettings, SchemaRegistryConfig
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.utils import supported_storage_types
from rptest.util import expect_exception
from ducktape.mark import matrix

from ducktape.errors import TimeoutError

QUERY_ENGINES = [
    QueryEngineType.SPARK,
    QueryEngineType.TRINO,
]


class AvroSchema:
    TableDescription = list[tuple[str, str]]

    def __init__(self,
                 fields: list[dict],
                 generate_record: Callable[[float], dict],
                 spark_table: TableDescription = [],
                 trino_table: TableDescription = [],
                 name="AvroRecord"):
        self._rep: dict = {
            "type": "record",
            "name": name,
            "fields": fields,
        }
        self._generate_record = generate_record
        self._spark_table = spark_table
        self._trino_table = trino_table

    def load(self):
        return avro.loads(json.dumps(self._rep))

    def table(self, engine: QueryEngineType):
        return self._spark_table if engine == QueryEngineType.SPARK else self._trino_table

    @property
    def field_names(self) -> list[str]:
        return [field['name'] for field in self._rep['fields']]

    @property
    def fields(self) -> list[dict]:
        return self._rep['fields']

    def gen(self, x: float) -> dict:
        return self._generate_record(x)


class EvolutionTestCase(NamedTuple):
    initial_schema: AvroSchema
    next_schema: AvroSchema


LEGAL_TEST_CASES = {
    "add_column":
    EvolutionTestCase(
        initial_schema=AvroSchema(
            fields=[
                {
                    "name": "verifier_string",
                    "type": "string",
                },
            ],
            generate_record=lambda x: {
                "verifier_string": f"verify-{x}",
            },
            spark_table=[
                ('verifier_string', 'string'),
            ],
            trino_table=[
                ('verifier_string', 'varchar'),
            ],
        ),
        next_schema=AvroSchema(
            fields=[
                {
                    "name": "verifier_string",
                    "type": "string",
                },
                {
                    "name": "ordinal",
                    "type": "int",
                },
            ],
            generate_record=lambda x: {
                "verifier_string": f"verify-{x}",
                "ordinal": int(x),
            },
            spark_table=[
                ('verifier_string', 'string'),
                ('ordinal', 'int'),
            ],
            trino_table=[
                ('verifier_string', 'varchar'),
                ('ordinal', 'integer'),
            ],
        ),
    ),
    "drop_column":
    EvolutionTestCase(
        initial_schema=AvroSchema(
            fields=[
                {
                    "name": "verifier_string",
                    "type": "string",
                },
                {
                    "name": "ordinal",
                    "type": "int",
                },
            ],
            generate_record=lambda x: {
                "verifier_string": f"verify-{x}",
                "ordinal": int(x),
            },
            spark_table=[
                ('verifier_string', 'string'),
                ('ordinal', 'int'),
            ],
            trino_table=[
                ('verifier_string', 'varchar'),
                ('ordinal', 'integer'),
            ],
        ),
        next_schema=AvroSchema(
            fields=[
                {
                    "name": "verifier_string",
                    "type": "string",
                },
            ],
            generate_record=lambda x: {
                "verifier_string": f"verify-{x}",
            },
            spark_table=[
                ('verifier_string', 'string'),
            ],
            trino_table=[
                ('verifier_string', 'varchar'),
            ],
        ),
    ),
    "promote_column":
    EvolutionTestCase(
        initial_schema=AvroSchema(
            fields=[
                {
                    "name": "ordinal",
                    "type": "int",
                },
            ],
            generate_record=lambda x: {
                "ordinal": int(x),
            },
            spark_table=[
                ('ordinal', 'int'),
            ],
            trino_table=[
                ('ordinal', 'integer'),
            ],
        ),
        next_schema=AvroSchema(
            fields=[
                {
                    "name": "ordinal",
                    "type": "long",
                },
            ],
            generate_record=lambda x: {
                "ordinal": int(x),
            },
            spark_table=[
                ('ordinal', 'bigint'),
            ],
            trino_table=[
                ('ordinal', 'bigint'),
            ],
        ),
    ),
    "reorder_columns":
    EvolutionTestCase(
        initial_schema=AvroSchema(
            fields=[
                {
                    "name": "first",
                    "type": "string",
                },
                {
                    "name": "second",
                    "type": "string",
                },
            ],
            generate_record=lambda x: {
                "first": "first",
                "second": "second",
            },
            spark_table=[
                ('first', 'string'),
                ('second', 'string'),
            ],
            trino_table=[
                ('first', 'varchar'),
                ('second', 'varchar'),
            ],
        ),
        next_schema=AvroSchema(
            fields=[{
                "name": "second",
                "type": "string",
            }, {
                "name": "first",
                "type": "string",
            }, {
                "name": "third",
                "type": "string",
            }],
            generate_record=lambda x: {
                "second": "second",
                "first": "first",
                "third": "third",
            },
            spark_table=[
                ('second', 'string'),
                ('first', 'string'),
                ('third', 'string'),
            ],
            trino_table=[
                ('second', 'varchar'),
                ('first', 'varchar'),
                ('third', 'varchar'),
            ],
        ),
    ),
}

ILLEGAL_TEST_CASES = {
    "illegal promotion int->string":
    EvolutionTestCase(
        initial_schema=AvroSchema(
            fields=[
                {
                    "name": "ordinal",
                    "type": "int",
                },
            ],
            generate_record=lambda x: {
                "ordinal": int(x),
            },
            spark_table=[
                ('ordinal', 'int'),
            ],
            trino_table=[
                ('ordinal', 'integer'),
            ],
        ),
        next_schema=AvroSchema(
            fields=[
                {
                    "name": "ordinal",
                    "type": "string",
                },
            ],
            generate_record=lambda x: {
                "ordinal": str(x),
            },
        ),
    ),
}


# for keeping track of the expected total number of rows across rounds
# of translation (i.e. calls to _produce)
class TranslationContext:
    total: int = 0


class SchemaEvolutionE2ETests(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(SchemaEvolutionE2ETests,
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
        self.table_name = f"redpanda.{self.topic_name}"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def _select(self,
                dl: DatalakeServices,
                query_engine: QueryEngineType,
                cols=list[str],
                sort_by_offset: bool = True):
        qe = dl.spark() if query_engine == QueryEngineType.SPARK else dl.trino(
        )
        query = f"select redpanda.offset, {', '.join(cols)} from {self.table_name}"
        self.redpanda.logger.debug(f"QUERY: '{query}'")
        out = qe.run_query_fetch_all(query)
        if sort_by_offset:
            out.sort(key=lambda r: r[0])
        return out

    def produce(
        self,
        dl: DatalakeServices,
        schema: AvroSchema,
        count: int,
        context: TranslationContext,
        should_translate: bool = True,
    ):
        producer = AvroProducer(
            {
                'bootstrap.servers': self.redpanda.brokers(),
                'schema.registry.url': self.redpanda.schema_reg().split(",")[0]
            },
            default_value_schema=schema.load())

        for i in range(count):
            #TODO(oren): use something other than the index here?
            record = schema.gen(context.total + i)
            producer.produce(topic=self.topic_name, value=record)

        producer.flush()
        if should_translate:
            dl.wait_for_translation(self.topic_name,
                                    msg_count=context.total + count)
            context.total = context.total + count
            return

        with expect_exception(TimeoutError, lambda _: True):
            dl.wait_for_translation(self.topic_name,
                                    msg_count=context.total + count,
                                    timeout=10)

    def check_table_schema(
        self,
        dl: DatalakeServices,
        query_engine: QueryEngineType,
        expected: AvroSchema,
    ):
        qe = dl.spark() if query_engine == QueryEngineType.SPARK else dl.trino(
        )
        table = qe.run_query_fetch_all(f"describe {self.table_name}")

        if query_engine == QueryEngineType.SPARK:
            table = [(t[0], t[1]) for t in table[1:-3]]
        elif query_engine == QueryEngineType.TRINO:
            table = [(t[0], t[1]) for t in table[1:]]

        assert table == expected.table(query_engine), \
            str(table)

    @contextmanager
    def setup_services(self,
                       query_engine: QueryEngineType,
                       compat_level: str = "NONE"):
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=True,
                              include_query_engines=[query_engine]) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name,
                iceberg_mode="value_schema_id_prefix",
            )
            SchemaRegistryClient({
                'url':
                self.redpanda.schema_reg().split(",")[0]
            }).set_compatibility(subject_name=f"{self.topic_name}-value",
                                 level=compat_level)
            yield dl

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=QUERY_ENGINES,
        test_case=list(LEGAL_TEST_CASES.keys()),
    )
    def test_legal_schema_evolution(self, cloud_storage_type, query_engine,
                                    test_case):
        """
        Test that rows written with schema A are still readable after evolving
        the table to schema B.
        """
        with self.setup_services(query_engine) as dl:
            count = 10
            ctx = TranslationContext()
            tc = LEGAL_TEST_CASES[test_case]

            self.produce(dl, tc.initial_schema, count, ctx)
            self.check_table_schema(dl, query_engine, tc.initial_schema)
            self.produce(dl, tc.next_schema, count, ctx)
            self.check_table_schema(dl, query_engine, tc.next_schema)

            select_out = self._select(dl, query_engine,
                                      tc.next_schema.field_names)
            assert len(select_out) == count * 2, \
                f"Expected {count*2} rows, got {select_out}"

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=QUERY_ENGINES,
        test_case=list(ILLEGAL_TEST_CASES.keys()),
    )
    def test_illegal_schema_evolution(self, cloud_storage_type, query_engine,
                                      test_case):
        """
        check that records produced with an incompatible schema don't wind up
        in the table.
        """
        with self.setup_services(query_engine) as dl:
            count = 10
            ctx = TranslationContext()
            tc = ILLEGAL_TEST_CASES[test_case]

            self.produce(dl, tc.initial_schema, count, ctx)
            self.check_table_schema(dl, query_engine, tc.initial_schema)
            self.produce(dl,
                         tc.next_schema,
                         count,
                         ctx,
                         should_translate=False)
            self.check_table_schema(dl, query_engine, tc.initial_schema)

            select_out = self._select(dl, query_engine,
                                      tc.next_schema.field_names)
            assert len(select_out) == count, \
                f"Expected {count} rows, got {select_out}"

    @cluster(num_nodes=3)
    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=QUERY_ENGINES,
    )
    def test_dropped_column_no_collision(self, cloud_storage_type,
                                         query_engine):
        """
        Translate some records, drop field A, translate some more, reintroduce field A  *by name*
        (this should create a *new* column). Confirm that 'select A' reads only the new column,
        producing nulls for all rows written prior to the final update.
        """

        with self.setup_services(query_engine) as dl:
            count = 10
            ctx = TranslationContext()
            initial_schema, next_schema = LEGAL_TEST_CASES["drop_column"]

            dropped_field_names = list(
                set(initial_schema.field_names) - set(next_schema.field_names))

            for schema in LEGAL_TEST_CASES["drop_column"]:
                self.produce(dl, schema, count, ctx)
                self.check_table_schema(dl, query_engine, schema)
                select_out = self._select(dl, query_engine, schema.field_names)
                assert len(select_out) == ctx.total, \
                    f"Expected {ctx.total} rows, got {select_out}"

            restored_schema = AvroSchema(
                fields=[
                    {
                        "name": "verifier_string",
                        "type": "string",
                    },
                    {
                        "name": "ordinal",
                        "type": "long",
                    },
                ],
                generate_record=lambda x: {
                    "verifier_string": f"verify-{x}",
                    "ordinal": int(x),
                },
                spark_table=[
                    ('verifier_string', 'string'),
                    ('ordinal', 'bigint'),
                ],
                trino_table=[
                    ('verifier_string', 'varchar'),
                    ('ordinal', 'bigint'),
                ],
            )

            self.produce(dl, restored_schema, count, ctx)
            self.check_table_schema(dl, query_engine, restored_schema)

            select_out = self._select(dl, query_engine, dropped_field_names)
            assert len(select_out) == count*3, \
                f"Expected {count*3} rows, got {select_out}"

            assert all(r[1] is None for r in select_out[:count * 2])
            assert all(r[1] is not None for r in select_out[count * 2:])

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=QUERY_ENGINES,
    )
    def test_dropped_column_select_fails(self, cloud_storage_type,
                                         query_engine):
        """
        Test that selecting a dropped column fails "gracefully" - or at least
        predictably and consistently.
        """
        with self.setup_services(query_engine) as dl:
            count = 10
            ctx = TranslationContext()
            initial_schema, next_schema = LEGAL_TEST_CASES['drop_column']
            dropped_field_names = list(
                set(initial_schema.field_names) - set(next_schema.field_names))

            for schema in [initial_schema, next_schema]:
                self.produce(dl, schema, count, ctx)
                self.check_table_schema(dl, query_engine, schema)

            if query_engine == QueryEngineType.SPARK:
                with expect_exception(
                        pyhive.exc.OperationalError, lambda e:
                        'UNRESOLVED_COLUMN' in e.args[0].status.errorMessage):
                    self._select(dl, query_engine, dropped_field_names)
            else:
                with expect_exception(
                        pyhive.exc.DatabaseError, lambda e: e.args[0].get(
                            'errorName') == 'COLUMN_NOT_FOUND'):
                    select_out = self._select(dl, query_engine,
                                              dropped_field_names)

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=QUERY_ENGINES,
    )
    def test_reorder_columns(self, cloud_storage_type, query_engine):
        """
        Test that changing the order of columns doesn't change the values
        associated with a column or field name.
        """
        with self.setup_services(query_engine) as dl:
            count = 10
            ctx = TranslationContext()
            initial_schema, next_schema = LEGAL_TEST_CASES['reorder_columns']
            for schema in [initial_schema, next_schema]:
                self.produce(dl, schema, count, ctx)
                self.check_table_schema(dl, query_engine, schema)

            for field in initial_schema.field_names:
                select_out = self._select(dl, query_engine, [field])
                assert len(select_out) == count * 2, \
                    f"Expected {count*2} rows, got {len(select_out)}"
                assert all(r[1] == field for r in select_out), \
                    f"{field} column mangled: {select_out}"
