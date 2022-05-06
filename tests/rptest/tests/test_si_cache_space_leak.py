# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster_spec import ClusterSpec

import os
import time

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer, FranzGoVerifiableRandomConsumer


class ShadowIndexingCacheSpaceLeakTest(RedpandaTest):
    """
    The test checks that SI cache doesn't exhibit a resource leak.
    In order to do this the test puts pressure to SI cache by settings its
    size to the minimum value. Then it uses FranzGoVerifiableProducer(Consumer)
    to produce and consume data via SI. The retention on the topic has to be
    small enough in order for SI to be involved. The test checks that no segment
    files are opened in the cache directory.
    """

    topics = (TopicSpec(partition_count=100, replication_factor=3), )
    test_defaults = {
        'default':
        dict(log_segment_size=1024 * 1024,
             cloud_storage_cache_size=10 * 1024 * 1024),
    }

    def __init__(self, test_context, *args, **kwargs):
        test_name = test_context.test_name
        si_params = self.test_defaults.get(
            test_name) or self.test_defaults.get('default')
        si_settings = SISettings(**si_params)
        self._segment_size = si_params['log_segment_size']
        extra_rp_conf = {
            'disable_metrics': True,
            'election_timeout_ms': 5000,
            'raft_heartbeat_interval_ms': 500,
            'segment_fallocation_step': 0x1000,
            'retention_bytes': self._segment_size,
        }
        super().__init__(test_context,
                         num_brokers=3,
                         extra_rp_conf=extra_rp_conf,
                         si_settings=si_settings)
        self._ctx = test_context
        self._verifier_node = test_context.cluster.alloc(
            ClusterSpec.simple_linux(1))[0]
        self.logger.info(
            f"Verifier node name: {self._verifier_node.name}, segment_size: {self._segment_size}"
        )

    def init_producer(self, msg_size, num_messages):
        self._producer = FranzGoVerifiableProducer(self._ctx, self.redpanda,
                                                   self.topic, msg_size,
                                                   num_messages,
                                                   [self._verifier_node])

    def init_consumer(self, msg_size, num_messages, concurrency):
        self._consumer = FranzGoVerifiableRandomConsumer(
            self._ctx, self.redpanda, self.topic, msg_size, num_messages,
            concurrency, [self._verifier_node])

    def free_nodes(self):
        super().free_nodes()
        wait_until(lambda: self.redpanda.sockets_clear(self._verifier_node),
                   timeout_sec=120,
                   backoff_sec=10)
        self.test_context.cluster.free_single(self._verifier_node)

    @cluster(num_nodes=4)
    @matrix(message_size=[10000],
            num_messages=[100000],
            num_read=[1000],
            concurrency=[2])
    def test_si_cache(self, message_size, num_messages, num_read, concurrency):
        if self.debug_mode:
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        self.init_producer(message_size, num_messages)
        self._producer.start(clean=False)

        def s3_has_some_data():
            objects = list(self.redpanda.get_objects_from_si())
            total_size = 0
            for o in objects:
                total_size += o.ContentLength
            return total_size > self._segment_size

        wait_until(s3_has_some_data, timeout_sec=300, backoff_sec=5)

        self.init_consumer(message_size, num_read, concurrency)
        self._consumer.start(clean=False)

        self._producer.wait()
        self._consumer.shutdown()
        self._consumer.wait()

        assert self._producer.produce_status.acked >= num_messages
        assert self._consumer.consumer_status.total_reads == num_read * concurrency

        # Verify that all files in cache are being closed
        def cache_files_closed():
            def is_cache_file(fname):
                # We target files in the cloud storage cache directory
                # and deleted files. The deleted files are likely cache
                # files that were deleted by retention previously but
                # still kept open because they're used by the consumer.
                return fname.startswith(
                    "/var/lib/redpanda/data/cloud_storage_cache"
                ) or fname == "(deleted)"

            files_count = 0
            for node in self.redpanda.nodes:
                files = self.redpanda.lsof_node(node)
                files_count += sum(1 for f in files if is_cache_file(f))
            return files_count == 0

        assert cache_files_closed() == False
        # Wait until all files are closed. The SI evicts all unused segments
        # after one minute of inactivity.
        wait_until(cache_files_closed, timeout_sec=120, backoff_sec=10)
