# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from typing import Any

from ducktape.tests.test import TestContext

from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.admin.v2 import broker_pb
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until


class AdminV2ListKafkaConnectionsTest(RedpandaTest):
    """
    Tests the AdminV2 ListKafkaConnections endpoint by verifying active Kafka connections are correctly reported.
    """

    test_topic: str = "test-list-connections"

    def __init__(self, test_ctx: TestContext, *args: Any, **kwargs: Any):
        super().__init__(test_ctx, *args, **kwargs)
        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.superuser_admin = Admin(
            self.redpanda, auth=(self.superuser.username, self.superuser.password)
        )
        self.consumer = RpkConsumer(test_ctx, self.redpanda, self.test_topic)

    def setUp(self):
        super().setUp()
        self.redpanda.set_cluster_config({"admin_api_require_auth": True})

        rpk = RpkTool(self.redpanda)
        rpk.create_topic(self.test_topic)

    @cluster(num_nodes=2)
    def test_list_kafka_connections(self):
        """
        Tests the AdminV2 list_connections endpoint by verifying active Kafka connections are correctly reported
        """

        self.logger.debug("Start a consumer to open some kafka connections")
        self.consumer.start()

        admin_v2 = AdminV2(
            self.redpanda,
            auth=(self.superuser.username, self.superuser.password),
        )
        node_id = self.redpanda.node_id(self.redpanda.nodes[0])
        req = broker_pb.ListKafkaConnectionsRequest(
            node_id=node_id,
            page_size=10,
        )

        def valid_response() -> bool:
            resp = admin_v2.broker().list_kafka_connections(req)

            self.logger.info(
                f"ListKafkaConnectionsResponse: total_size={resp.total_size}, connections={len(resp.connections)}"
            )
            self.logger.debug(f"ListKafkaConnectionsResponse: {resp}")

            # Sanity check the response
            assert len(resp.connections) > 0
            conn = resp.connections[0]

            assert conn.node_id == node_id
            assert len(conn.source.ip_address) > 0
            assert conn.source.port != 0
            assert not conn.tls_info.enabled
            assert conn.total_request_statistics.request_count > 0

            return True

        wait_until(
            valid_response,
            timeout_sec=15,
            retry_on_exc=True,
            err_msg="Did not observe a valid ListKafkaConnectionsResponse",
        )

        self.logger.info(
            "Test the filtering integration by filtering for an unknown UUID, expect an empty response"
        )
        filtered_resp = admin_v2.broker().list_kafka_connections(
            broker_pb.ListKafkaConnectionsRequest(
                node_id=-1,
                filter='uid = "ba26cadd-90f6-4999-b2c9-a89b5f033507"',
            )
        )
        self.logger.debug(f"Filtered response: {filtered_resp}")
        assert len(filtered_resp.connections) == 0
        assert filtered_resp.total_size == 0

        self.consumer.stop()


class AdminV2ListKafkaConnectionsLicenseTest(RedpandaTest):
    """
    Tests that list_kafka_connections requires a valid license.
    """

    def __init__(self, test_ctx: TestContext, *args: Any, **kwargs: Any):
        super().__init__(test_ctx, *args, **kwargs)

    def setUp(self):
        self.redpanda.set_environment(
            {"__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE": "true"}
        )
        super().setUp()

    @cluster(num_nodes=1)
    def test_without_license(self):
        admin = AdminV2(self.redpanda)
        resp = admin.broker().call_list_kafka_connections(
            broker_pb.ListKafkaConnectionsRequest(node_id=-1)
        )
        err = resp.error()
        assert err is not None, f"expected error response without license, got {err}"
        assert "license" in err.message, (
            f"expected license in error message, got {err.message}"
        )
