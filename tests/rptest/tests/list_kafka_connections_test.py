# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from typing import Any, Literal, Tuple

from ducktape.tests.test import TestContext

from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.admin.v2 import broker_pb
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result


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

        def non_empty_result() -> Tuple[
            bool, broker_pb.ListKafkaConnectionsResponse | None
        ]:
            resp = admin_v2.broker().list_kafka_connections(req)
            if len(resp.connections) == 0:
                return False, None
            return True, resp

        resp = wait_until_result(
            non_empty_result,
            timeout_sec=15,
            err_msg="Unable to observe a non-empty ListKafkaConnectionsResponse",
        )

        self.logger.info(
            f"ListKafkaConnectionsResponse: "
            f"total_size={resp.total_size}, connections={len(resp.connections)}"
        )
        self.logger.debug(f"ListKafkaConnectionsResponse: {resp}")

        # Sanity check response
        assert len(resp.connections) > 0
        conn = resp.connections[0]
        assert conn.node_id == node_id
        assert len(conn.source.ip_address) > 0
        assert conn.source.port != 0
        assert not conn.tls_info.enabled

        self.consumer.stop()
