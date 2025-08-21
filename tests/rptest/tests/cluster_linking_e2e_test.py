# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from rptest.services.multi_cluster_services import Cluster, MultiClusterServices, ServiceType
from rptest.tests.redpanda_test import RedpandaTest

from rptest.services.cluster import cluster
from rptest.util import expect_exception, wait_until_result


class MultiClusterTestBase(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context, *args, **kwargs)

    def basic_ops(self, services: MultiClusterServices):
        def at_least_one_topic_exists(services: MultiClusterServices,
                                      node: Cluster):
            topics = services.list_topics(node, detailed=True)
            return len(topics) > 0, topics

        topic = 'test-topic'
        services.create_topic(services.primary,
                              topic,
                              partitions=3,
                              replicas=3)
        p_topics = wait_until_result(
            lambda: at_least_one_topic_exists(services, services.primary),
            timeout_sec=30,
            err_msg="Failed to create a single topic on the primary cluster")

        services.create_topic(services.secondary,
                              topic,
                              partitions=3,
                              replicas=3)
        s_topics = wait_until_result(
            lambda: at_least_one_topic_exists(services, services.secondary),
            timeout_sec=30,
            err_msg="Failed to create a single topic on the secondary cluster")

        assert p_topics == s_topics, \
            f"Expected same topics on both clusters, got {p_topics=} vs {s_topics=}"

        assert len(p_topics) == 1 and p_topics[0][0] == topic,\
            f"Expected {topic=}, got {p_topics=}"

        status_json = services.primary.admin.get_status_ready()
        assert status_json['status'] == 'ready', \
                f"Expected ready, got {status_json=}"

        if services.secondary.is_redpanda:
            status_json = services.secondary.admin.get_status_ready()
            assert status_json['status'] == 'ready', \
                f"Expected ready, got {status_json=}"
        else:
            with expect_exception(NotImplementedError, lambda e: True):
                services.secondary.admin.get_status_ready()


class MultiClusterRedpandaTest(MultiClusterTestBase):
    """
    Just verifies MultiClusterServices for now. rp + rp & rp + kafka
    """
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context,
                         num_brokers=3,
                         *args,
                         **kwargs)

        self.test_context = test_context

    def setUp(self):
        # MultiClusterServices will set itself up
        pass

    @cluster(num_nodes=6)
    def test_basic_ops(self):
        with MultiClusterServices(self.test_context,
                                  self.logger,
                                  self.redpanda,
                                  secondary_type=ServiceType.REDPANDA,
                                  num_brokers=3) as services:
            assert services.secondary.is_redpanda, \
                f"Expected Redpanda service, got {services.secondary}"
            self.basic_ops(services)


class MultiClusterKafkaTest(MultiClusterTestBase):
    """
    Just verifies MultiClusterServices for now. rp + rp & rp + kafka
    """
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context,
                         num_brokers=3,
                         *args,
                         **kwargs)

        self.test_context = test_context

    def setUp(self):
        # MultiClusterServices will set itself up
        pass

    @cluster(num_nodes=7)
    def test_basic_ops(self):
        with MultiClusterServices(self.test_context,
                                  self.logger,
                                  self.redpanda,
                                  secondary_type=ServiceType.KAFKA,
                                  num_brokers=3) as services:
            assert services.secondary.is_kafka, \
                f"Expected Kafka service, got {services.secondary}"
            self.basic_ops(services)
