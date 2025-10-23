# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from enum import Enum
import json

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.admin import v2 as admin_v2
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.catalog_service import CatalogType
from rptest.services.spark_service import SparkService
from rptest.services.redpanda import SISettings
from rptest.services.redpanda_installer import RedpandaVersionTriple
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode


class MigrationType25_3(str, Enum):
    RENAME_COLUMNS = "rename_columns"
    RECREATE_TABLE = "recreate_table"
    # Alternatives:
    # - drop columns
    # - drop table
    # - rename table


class DatalakeUpgradeTest(RedpandaTest):
    def __init__(self, test_context):
        super(DatalakeUpgradeTest, self).__init__(
            test_context,
            num_brokers=3,
            si_settings=SISettings(test_context=test_context),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
            },
        )
        self.test_ctx = test_context
        self.topic_name = "upgrade_topic"
        self.topic_partition_count = 10

        # Initial version that supported Iceberg.
        self.initial_version: RedpandaVersionTriple = (24, 3, 1)
        self.min_version_with_lag_support: RedpandaVersionTriple = (25, 1, 1)
        self.min_version_25_3_with_new_schemas: RedpandaVersionTriple = (25, 3, 0)

    def setUp(self):
        self.redpanda._installer.install(self.redpanda.nodes, self.initial_version)

    def pre_25_3_migration(self) -> None:
        """
        Redpanda v25.3 includes changes that break table compatibility. There
        are manual steps that need to be run.
        1. Before upgrading to v25.3, disable Iceberg on all Iceberg topics.
        2. Upgrade to v25.3.
        3. Query the GetCoordinatorState endpoint to see if there are any
           remaining pending entries in the coordinator for the given topic.
        4. Run queries to make the existing table conformant with the breaking
           changes:
           - redpanda.timestamp column (was timestamp, is now timestamptz)
           - redpanda.headers.key column (was binary, is now string)
           - Avro optionals (two-field union of [null, <FIELD>]) (was struct,
             is now an optional FIELD)
           - Avro union (union field names are now the type names)
           - Avro enums (was integer, is now string)
           - Protobuf enums (was integer, is now string)
        5. Reenable Iceberg on all Iceberg topics.
        """
        self.logger.info("Running pre-25.3 migration steps")
        rpk = RpkTool(self.redpanda)

        # Disabling Iceberg before upgrading (and restarting) ensures that post
        # upgrade, no additional state will be sent to the coordinator while we
        # update our tables.
        rpk.alter_topic_config(self.topic_name, "redpanda.iceberg.mode", "disabled")

    def post_25_3_migration(
        self, migration_type: MigrationType25_3, spark: SparkService
    ) -> None:
        self.logger.info("Running post-25.3 migration steps")

        def no_pending_coordinator_state():
            topic = self.topic_name
            admin: admin_v2.Admin = admin_v2.Admin(self.redpanda)
            request = admin_v2.datalake_pb.GetCoordinatorStateRequest()
            try:
                response: admin_v2.datalake_pb.GetCoordinatorStateResponse = (
                    admin.datalake().get_coordinator_state(request)
                )
            except Exception as e:
                self.logger.debug(f"Exception while hitting endpoint: {e}")
                return False

            if topic not in response.state.topic_states:
                self.logger.debug(f"Topic {topic} not found")
                return False

            t_state = response.state.topic_states[topic]
            self.logger.debug(f"{topic}: {t_state}")
            if len(t_state.partition_states) != self.topic_partition_count:
                return False

            for _, p_state in t_state.partition_states.items():
                if len(p_state.pending_entries) > 0:
                    return False
            return True

        wait_until(no_pending_coordinator_state, timeout_sec=60, backoff_sec=1)

        match migration_type:
            case MigrationType25_3.RENAME_COLUMNS:
                # Rename the affected columns. This means that the query engine
                # will need to know about the renamed fields.
                with spark.run_query(f"""
                    ALTER TABLE redpanda.{self.topic_name}
                    RENAME COLUMN redpanda.timestamp TO timestamp_v1
                    """) as cursor:
                    cursor.close()

                with spark.run_query(f"""
                    ALTER TABLE redpanda.{self.topic_name}
                    RENAME COLUMN redpanda.headers TO headers_v1
                    """) as cursor:
                    cursor.close()

            case MigrationType25_3.RECREATE_TABLE:
                # Replace the table. This means that the query engines will be
                # able to use the existing table/columns with no modifications.
                with spark.run_query(f"""
                    REPLACE TABLE redpanda.{self.topic_name}
                    USING iceberg
                    AS SELECT
                    STRUCT(
                        redpanda.partition,
                        redpanda.offset,
                        CAST(redpanda.timestamp AS TIMESTAMP) AS timestamp,
                        TRANSFORM(redpanda.headers,
                          header -> STRUCT(
                            CAST(header.key AS STRING) AS key,
                            header.value)
                          ) AS headers,
                        redpanda.key
                        ) AS redpanda,
                    value
                    FROM redpanda.{self.topic_name}
                    """) as cursor:
                    cursor.close()

        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic_name, "redpanda.iceberg.mode", "key_value")

    @cluster(num_nodes=6)
    @skip_debug_mode
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
        migration_type=[
            MigrationType25_3.RECREATE_TABLE,
            MigrationType25_3.RENAME_COLUMNS,
        ],
    )
    def test_upload_through_upgrade(
        self, cloud_storage_type, query_engine, migration_type: MigrationType25_3
    ):
        """
        Test that Iceberg translation can progress through different versions
        of Redpanda (e.g. ensuring that data format changes or additional
        Iceberg fields don't block progress).
        """
        versions = self.load_version_range(self.initial_version)
        lag_set = self.initial_version >= self.min_version_with_lag_support
        migrated_schemas_25_3 = (
            self.initial_version >= self.min_version_25_3_with_new_schemas
        )

        last_version_with_legacy_schemas = (
            self.redpanda._installer.highest_from_prior_feature_version(
                self.min_version_25_3_with_new_schemas
            )
        )

        total_count = 0
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=CatalogType.REST_JDBC,
            include_query_engines=[query_engine],
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name,
                partitions=self.topic_partition_count,
                target_lag_ms=10000,
            )

            def run_workload():
                nonlocal total_count
                count = 100
                dl.produce_to_topic(self.topic_name, 1024, msg_count=count)
                total_count += count
                dl.wait_for_translation(self.topic_name, msg_count=total_count)

            for v in self.upgrade_through_versions(
                versions_in=versions, already_running=True
            ):
                self.logger.info(f"Updated to {v}")
                if not lag_set and v >= self.min_version_with_lag_support:
                    # When upgrading from older versions, unsupported topic properties
                    # are just ignored. Force a cluster config change right after upgrading
                    # to first version with the support
                    self.redpanda.set_cluster_config({"iceberg_target_lag_ms": 10000})
                    lag_set = True
                if (
                    not migrated_schemas_25_3
                    and v >= self.min_version_25_3_with_new_schemas
                ):
                    self.post_25_3_migration(migration_type, dl.spark())
                    migrated_schemas_25_3 = True

                run_workload()

                if v == last_version_with_legacy_schemas:
                    self.pre_25_3_migration()

            # Run some spot checks to ensure that the data is readable.
            result = dl.spark().run_query_fetch_one(f"""
                                                    SELECT count(*)
                                                    FROM redpanda.{self.topic_name}
                                                    WHERE redpanda.offset < 10
                                                      AND redpanda.partition = 0
                                                    """)
            assert result[0] == 10, f"Expected 10 rows, got {result[0]}"

            additional_where = ""
            if migration_type == MigrationType25_3.RENAME_COLUMNS:
                additional_where = "OR redpanda.timestamp_v1 >= '2025-01-01 00:00:00'"

            result = dl.spark().run_query_fetch_one(f"""
                                                    SELECT count(*)
                                                    FROM redpanda.{self.topic_name}
                                                    WHERE redpanda.timestamp >= '2025-01-01 00:00:00'
                                                    {additional_where}
                                                    """)
            assert result[0] == total_count, (
                f"Expected {total_count} rows, got {result[0]}"
            )

            # Check that all fields are queryable and the structure of the row
            # matches the expected structure.
            with dl.spark().run_query(f"""
                                      SELECT *
                                      FROM redpanda.{self.topic_name}
                                      """) as cursor:
                assert cursor.description == [
                    ("redpanda", "STRUCT_TYPE", None, None, None, None, True),
                    ("value", "BINARY_TYPE", None, None, None, None, True),
                ], f"Unexpected cursor description: {cursor.description}"

                rows = cursor.fetchall()
                assert rows

                # We're not checking internal redpanda fields as it is close to
                # impossible with our current client PyHive which returns a string
                # representation of the struct. It also loses some type information
                # and binary data which looks like numbers is represented as numbers.
                # If assert below begin to fail maybe we have changed the client and
                # now it is possible to check the types.
                assert isinstance(rows[0][0], str), (
                    f"Unexpected type {type(rows[0][0])}"
                )
                assert isinstance(rows[0][1], bytes), (
                    f"Unexpected type {type(rows[0][1])}"
                )

            # Check nested fields of redpanda struct. Fetch all rows
            with dl.spark().run_query(f"""
                                    SELECT to_json(redpanda)
                                    FROM redpanda.{self.topic_name}
                                    """) as cursor:
                rows = cursor.fetchall()
                assert rows

                # Fetch all rows to make sure the underlying query engine does
                # not fail internally.
                expected_keys_pre_25_3 = {
                    "partition",
                    "offset",
                    "timestamp",
                    "headers",
                    "key",
                }
                if migration_type == MigrationType25_3.RENAME_COLUMNS:
                    # Row written pre-25.3 will use the old, renamed column.
                    expected_keys_pre_25_3.remove("timestamp")
                    expected_keys_pre_25_3.remove("headers")
                    expected_keys_pre_25_3.add("timestamp_v1")
                    expected_keys_pre_25_3.add("headers_v1")

                # Note, individual rows only include either the pre-25.3
                # fields or post-25.3 fields, unlike the table schema which
                # contains both.
                expected_keys = {
                    "partition",
                    "offset",
                    "timestamp",
                    "headers",
                    "key",
                    "timestamp_type",
                }

                for row in rows:
                    row_keys = json.loads(row[0]).keys()
                    assert (
                        row_keys == expected_keys_pre_25_3 or row_keys == expected_keys
                    ), f"Unexpected JSON keys: {row_keys}"

                # The table schema will have both the pre-25.3 _and_ post-25.3
                # fields.
                expected_struct_str = ""
                match migration_type:
                    case MigrationType25_3.RENAME_COLUMNS:
                        expected_struct_str = (
                            "struct<"
                            "partition:int,"
                            "offset:bigint,"
                            "timestamp_v1:timestamp_ntz,"
                            "headers_v1:array<struct<key:binary,value:binary>>,"
                            "key:binary,"
                            "timestamp:timestamp,"
                            "headers:array<struct<key:string,value:binary>>,"
                            "timestamp_type:int>"
                        )
                    case MigrationType25_3.RECREATE_TABLE:
                        expected_struct_str = (
                            "struct<"
                            "partition:int,"
                            "offset:bigint,"
                            "timestamp:timestamp,"
                            "headers:array<struct<key:string,value:binary>>,"
                            "key:binary,"
                            "timestamp_type:int>"
                        )

                spark_expected_out = [
                    (
                        "redpanda",
                        expected_struct_str,
                        None,
                    ),
                    ("value", "binary", None),
                    ("", "", ""),
                    ("# Partitioning", "", ""),
                    ("Part 0", "hours(redpanda.timestamp)", ""),
                ]
                spark_describe_out = dl.spark().run_query_fetch_all(
                    f"describe redpanda.{self.topic_name}"
                )
                assert spark_describe_out == spark_expected_out, (
                    f"\n{spark_describe_out}\nvs\n{spark_expected_out}"
                )
