# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService
from rptest.tests.redpanda_test import RedpandaMixedTest


class DefaultClientTest(RedpandaMixedTest):
    def __init__(self, ctx):
        super().__init__(test_context=ctx, min_brokers=1)

    @cluster(num_nodes=1)
    def test_create_delete_topic(self):
        name = "test-create-delete-topic"
        spec = TopicSpec(name=name, replication_factor=1)

        client = self.client()

        client.create_topic(spec)

        desc = client.describe_topic(name)
        assert desc.name == name
        assert len(desc.partitions) > 0

        client.delete_topic(name)


class KafkaCliToolsTest(RedpandaMixedTest):
    def __init__(self, ctx):
        super().__init__(test_context=ctx, min_brokers=1)

    @cluster(num_nodes=1)
    def test_produce_consume(self):
        name = "test-produce-consume"
        spec = TopicSpec(name=name, replication_factor=1)
        client = KafkaCliTools(self.redpanda)
        rpk = RpkTool(self.redpanda)

        client.create_topic(spec)

        # produce and consume a message to the topic
        client.produce(topic=name, num_records=1, record_size=100)
        rpk.consume(topic=name, n=1, timeout=10)

        client.delete_topic(name)


class RedpandaMixedTestSelfTest(RedpandaMixedTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, min_brokers=1, **kwargs)

    @cluster(num_nodes=1)
    def test_rpk(self):
        """A very basic rpk test."""
        rpk = RpkTool(self.redpanda)

        rpk.list_topics()

        name = "test-rpk-create-topic"
        rpk.create_topic(name)
        rpk.delete_topic(name)

    @cluster(num_nodes=1)
    def test_metrics(self):
        """Test metrics_sample() can retrieve internal metrics."""

        uptime = "vectorized_application_uptime"
        utilization = "vectorized_reactor_utilization"

        # single-pattern fuzzy
        vectorized_application_uptime = self.redpanda.metrics_sample(
            sample_pattern=uptime
        )
        assert vectorized_application_uptime is not None, "expected some metrics"
        if isinstance(self.redpanda, RedpandaService):
            assert len(vectorized_application_uptime.samples) == 1, "should be 1 node"
        else:
            assert len(vectorized_application_uptime.samples) >= 1, (
                "should be >=3s nodes"
            )
        assert vectorized_application_uptime.samples[0].value > 0, (
            "expected uptime greater than 0"
        )

        # single-pattern fuzzy, substring
        vectorized_application_uptime = self.redpanda.metrics_sample(
            sample_pattern="application_up"
        )
        assert vectorized_application_uptime is not None, "expected some metrics"
        if isinstance(self.redpanda, RedpandaService):
            assert len(vectorized_application_uptime.samples) == 1, "should be 1 node"
        else:
            assert len(vectorized_application_uptime.samples) >= 1, (
                "should be >=3s nodes"
            )
        assert vectorized_application_uptime.samples[0].value > 0, (
            "expected uptime greater than 0"
        )
        assert vectorized_application_uptime.samples[0].sample == uptime

        # multi-pattern fuzzy
        sample_patterns = [
            uptime,
            utilization,
        ]
        samples = self.redpanda.metrics_samples(sample_patterns)
        assert samples is not None, "expected sample patterns to match"

        count = self.redpanda.metric_sum("vectorized_application_uptime")
        assert count > 0, "expected count greater than 0"

        # test cases for bad combinations of name and sample_pattern
        # Expect ValueError when neither provided
        try:
            _ = self.redpanda.metrics_sample(sample_pattern="", name="")
            assert False, (
                "Expected ValueError when neither sample_pattern nor name provided"
            )
        except ValueError as e:
            assert "Either 'name' or 'sample_pattern'" in str(e)

        # Expect ValueError when both provided
        try:
            _ = self.redpanda.metrics_sample(sample_pattern=uptime, name=uptime)
            assert False, (
                "Expected ValueError when both sample_pattern and name provided"
            )
        except ValueError as e:
            assert "both were" in str(e) or "both provided" in str(e)

        # exact match single
        vectorized_application_uptime = self.redpanda.metrics_sample(name=uptime)
        assert vectorized_application_uptime is not None, "expected some metrics"
        if isinstance(self.redpanda, RedpandaService):
            assert len(vectorized_application_uptime.samples) == 1, "should be 1 node"
        else:
            assert len(vectorized_application_uptime.samples) >= 1, (
                "should be >=3s nodes"
            )
        assert vectorized_application_uptime.samples[0].value > 0, (
            "expected uptime greater than 0"
        )

        # exact match two
        exact_two = self.redpanda.metrics_samples(names=[uptime, utilization])
        assert uptime in exact_two
        assert utilization in exact_two
        assert len(exact_two) == 2

        uptime_value = exact_two[uptime]
        if isinstance(self.redpanda, RedpandaService):
            assert len(uptime_value.samples) == 1, "should be 1 node"
        else:
            assert len(uptime_value.samples) >= 1, "should be >=3s nodes"
        assert uptime_value.samples[0].value > 0, "expected uptime greater than 0"
