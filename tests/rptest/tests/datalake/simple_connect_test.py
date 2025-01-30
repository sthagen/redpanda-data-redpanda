# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import os
import tempfile
import time
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix

from rptest.services.admin import Admin, NamespacedTopic, InboundTopic, OutboundDataMigration, MigrationAction
from rptest.clients.types import TopicSpec
from rptest.utils.data_migrations import DataMigrationTestMixin


class RedpandaConnectIcebergTest(RedpandaTest, DataMigrationTestMixin):
    TOPIC_NAME = "ducky_topic"
    PARTITION_COUNT = 5
    FAST_COMMIT_INTVL_S = 5
    SLOW_COMMIT_INTVL_S = 60

    verifier_schema_avro = """
{
    "type": "record",
    "name": "VerifierRecord",
    "fields": [
        {
            "name": "verifier_string",
            "type": "string"
        },
        {
            "name": "ordinal",
            "type": "long"
        },
        {
            "name": "timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        }
    ]
}
    """

    def __init__(self, test_context):
        self._topic = None
        super(RedpandaConnectIcebergTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=SISettings(test_context,
                                   cloud_storage_enable_remote_read=False,
                                   cloud_storage_enable_remote_write=False),
            extra_rp_conf={
                "iceberg_enabled":
                True,
                "iceberg_catalog_commit_interval_ms":
                self.FAST_COMMIT_INTVL_S * 1000
            },
            schema_registry_config=SchemaRegistryConfig())

    def avro_stream_config(self, topic, subject, cnt=3000):
        return {
            "input": {
                "generate": {
                    "mapping": "root = counter()",
                    "interval": "",
                    "count": cnt,
                    "batch_size": 1
                }
            },
            "pipeline": {
                "processors": [{
                    "mapping":
                    """
                            root.ordinal = this
                            root.timestamp = timestamp_unix_milli()
                            root.verifier_string = uuid_v4()
                        """
                }, {
                    "schema_registry_encode": {
                        "url": self.redpanda.schema_reg().split(",")[0],
                        "subject": subject,
                        "refresh_period": "10s"
                    }
                }]
            },
            "output": {
                "redpanda": {
                    "seed_brokers": self.redpanda.brokers_list(),
                    "topic": topic,
                }
            }
        }

    def setUp(self):
        pass

    def _create_schema(self, subject: str, schema: str, schema_type="avro"):
        rpk = RpkTool(self.redpanda)
        with tempfile.NamedTemporaryFile(suffix=f".{schema_type}") as tf:
            tf.write(bytes(schema, 'UTF-8'))
            tf.flush()
            rpk.create_schema(subject, tf.name)

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types(),
            scenario=["simple", "unmount", "remount"])
    def test_translating_avro_serialized_records(self, cloud_storage_type,
                                                 scenario):
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=False,
                              include_query_engines=[
                                  QueryEngineType.SPARK,
                              ]) as datalake:
            extra_config = {} if scenario == "simple" else {
                "redpanda.remote.read": "true",
                "redpanda.remote.write": "true"
            }
            if scenario == "unmount":
                extra_config["redpanda.iceberg.delete"] = "false"
            datalake.create_iceberg_enabled_topic(
                self.TOPIC_NAME,
                partitions=self.PARTITION_COUNT,
                replicas=3,
                iceberg_mode="value_schema_id_prefix",
                config=extra_config)

            self._create_schema("verifier_schema", self.verifier_schema_avro)
            connect = RedpandaConnectService(self.test_context, self.redpanda)
            connect.start()
            verifier = DatalakeVerifier(self.redpanda, self.TOPIC_NAME,
                                        datalake.spark())
            scenario_verification_fn = {
                "simple": self.verify_simple_scenario,
                "unmount": self.verify_unmount_scenario,
                "remount": self.verify_remount_scenario,
            }[scenario]
            scenario_verification_fn(connect, verifier)

    def verify_simple_scenario(self, connect, verifier):
        connect.start_stream(name="ducky_stream",
                             config=self.avro_stream_config(
                                 self.TOPIC_NAME, "verifier_schema", 3000))
        verifier.start()
        connect.stop_stream("ducky_stream")
        verifier.wait()

    def verify_unmount_scenario(self, connect, verifier):
        connect.start_stream(name="ducky_stream",
                             config=self.avro_stream_config(
                                 self.TOPIC_NAME, "verifier_schema", 1000000))
        # todo make reasonable or just make sure something went through
        self.redpanda.set_cluster_config({
            "iceberg_catalog_commit_interval_ms":
            self.SLOW_COMMIT_INTVL_S * 1000
        })
        verifier.start(wait_first_iceberg_msg=True)
        self.admin = Admin(self.redpanda)
        ns_topic = NamespacedTopic(self.TOPIC_NAME)

        out_migration = OutboundDataMigration([ns_topic], consumer_groups=[])
        out_migration_id = self.create_and_wait(out_migration)
        self.admin.execute_data_migration_action(out_migration_id,
                                                 MigrationAction.prepare)
        self.wait_for_migration_states(out_migration_id, ['prepared'])
        self.admin.execute_data_migration_action(out_migration_id,
                                                 MigrationAction.execute)
        # the topic goes read-only during this wait
        self.wait_for_migration_states(out_migration_id, ['executed'])
        connect.stop_stream("ducky_stream", should_finish=False)
        time.sleep(1)  # just it case: let verifier consume remaining messages
        verifier.go_offline()

        self.admin.execute_data_migration_action(out_migration_id,
                                                 MigrationAction.finish)
        self.wait_for_migration_states(out_migration_id, ['finished'])
        self.wait_partitions_disappear([self.TOPIC_NAME])

        verifier.wait(progress_timeout_sec=10 * self.SLOW_COMMIT_INTVL_S)

    def verify_remount_scenario(self, connect, verifier):
        self.redpanda.set_cluster_config({
            "iceberg_catalog_commit_interval_ms":
            self.SLOW_COMMIT_INTVL_S * 1000
        })
        connect.start_stream(name="ducky_stream",
                             config=self.avro_stream_config(
                                 self.TOPIC_NAME, "verifier_schema", 100000))
        self.admin = Admin(self.redpanda)
        ns_topic = NamespacedTopic(self.TOPIC_NAME)
        self.logger.info(f"unmounting {self.TOPIC_NAME}")
        self.admin.unmount_topics([ns_topic])
        self.wait_partitions_disappear([self.TOPIC_NAME])
        self.logger.info(f"unmounted {self.TOPIC_NAME}")

        connect.stop_stream("ducky_stream", should_finish=False)

        self.logger.info(f"remounting {self.TOPIC_NAME}")
        self.admin.mount_topics([InboundTopic(ns_topic)])
        self.wait_partitions_appear([
            TopicSpec(name=self.TOPIC_NAME,
                      partition_count=self.PARTITION_COUNT)
        ])
        self.logger.info(f"remounted {self.TOPIC_NAME}")

        self.redpanda.set_cluster_config({
            "iceberg_catalog_commit_interval_ms":
            self.FAST_COMMIT_INTVL_S * 1000
        })

        verifier.start()
        connect.start_stream(name="ducky_stream2",
                             config=self.avro_stream_config(
                                 self.TOPIC_NAME, "verifier_schema", 3000))
        connect.stop_stream("ducky_stream2")
        verifier.wait(progress_timeout_sec=2 * self.SLOW_COMMIT_INTVL_S)
