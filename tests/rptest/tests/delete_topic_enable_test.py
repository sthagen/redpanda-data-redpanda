# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkException, RpkTool
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception

from typing import Any


class DeleteTopicEnableTest(RedpandaTest):
    """
    Verify that the `delete_topic_enable` configuration property
    functions as intended to globally disable topic deletion.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3)
    def test_delete_topic_enable_disabled(self):
        """Test that topics cannot be deleted when delete_topic_enable=false"""
        test_topic = "test-topic"
        self.rpk.create_topic(test_topic)

        wait_until(
            lambda: test_topic in self.rpk.list_topics(),
            timeout_sec=30,
            backoff_sec=3,
        )

        # Disable topic deletion globally
        self.redpanda.set_cluster_config({"delete_topic_enable": False})

        # Attempt to delete - should fail with TOPIC_DELETION_DISABLED
        with expect_exception(
            RpkException, lambda e: "TOPIC_DELETION_DISABLED" in str(e)
        ):
            self.rpk.delete_topic(test_topic)

        # Allow time for any erroneous deletion to be propagated
        time.sleep(5)

        # Verify topic still exists
        topics = list(self.rpk.list_topics())
        assert test_topic in topics, f"Topic {test_topic} should still exist"

        # Re-enable and verify deletion works
        self.redpanda.set_cluster_config({"delete_topic_enable": True})
        self.rpk.delete_topic(test_topic)

        wait_until(
            lambda: test_topic not in self.rpk.list_topics(),
            timeout_sec=30,
            backoff_sec=3,
        )

    @cluster(num_nodes=3)
    def test_delete_topic_enable_default(self):
        """Test that the default value allows deletion (backward compat)"""
        test_topic = "test-topic-default"
        self.rpk.create_topic(test_topic)

        wait_until(
            lambda: test_topic in self.rpk.list_topics(),
            timeout_sec=30,
            backoff_sec=3,
        )

        # Without changing config, deletion should work
        self.rpk.delete_topic(test_topic)

        wait_until(
            lambda: test_topic not in self.rpk.list_topics(),
            timeout_sec=30,
            backoff_sec=3,
        )

    @cluster(num_nodes=3)
    def test_nodelete_topics_still_protected(self):
        """Test that kafka_nodelete_topics is still honored when delete_topic_enable=true"""
        test_topic = "protected-topic"
        self.rpk.create_topic(test_topic)

        wait_until(
            lambda: test_topic in self.rpk.list_topics(),
            timeout_sec=30,
            backoff_sec=3,
        )

        # Protect topic via kafka_nodelete_topics
        self.redpanda.set_cluster_config({"kafka_nodelete_topics": [test_topic]})

        # Even with delete_topic_enable=true (default), protected topics can't be deleted
        with expect_exception(
            RpkException, lambda e: "TOPIC_AUTHORIZATION_FAILED" in str(e)
        ):
            self.rpk.delete_topic(test_topic)

        # Allow time for any erroneous deletion to be propagated
        time.sleep(5)

        # Verify topic still exists
        topics = list(self.rpk.list_topics())
        assert test_topic in topics, f"Topic {test_topic} should still exist"

        # Remove from protection list and delete
        self.redpanda.set_cluster_config({"kafka_nodelete_topics": []})
        self.rpk.delete_topic(test_topic)

        wait_until(
            lambda: test_topic not in self.rpk.list_topics(),
            timeout_sec=30,
            backoff_sec=3,
        )

    @cluster(num_nodes=3)
    def test_delete_topic_enable_multiple_topics(self):
        """Test that all topics are rejected when delete_topic_enable=false"""
        topics = ["topic-a", "topic-b", "topic-c"]

        for topic in topics:
            self.rpk.create_topic(topic)

        for topic in topics:
            wait_until(
                lambda t=topic: t in self.rpk.list_topics(),
                timeout_sec=30,
                backoff_sec=3,
            )

        # Disable topic deletion globally
        self.redpanda.set_cluster_config({"delete_topic_enable": False})

        # Attempt to delete each topic - all should fail
        for topic in topics:
            with expect_exception(
                RpkException, lambda e: "TOPIC_DELETION_DISABLED" in str(e)
            ):
                self.rpk.delete_topic(topic)

        # Allow time for any erroneous deletion to be propagated
        time.sleep(5)

        # Verify all topics still exist
        existing_topics = list(self.rpk.list_topics())
        for topic in topics:
            assert topic in existing_topics, f"Topic {topic} should still exist"

        # Re-enable and clean up
        self.redpanda.set_cluster_config({"delete_topic_enable": True})
        for topic in topics:
            self.rpk.delete_topic(topic)
