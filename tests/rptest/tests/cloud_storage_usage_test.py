# Copyright 2023 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import MetricsEndpoint, SISettings
from rptest.util import firewall_blocked
from rptest.utils.si_utils import BucketView
from rptest.clients.types import TopicSpec
from rptest.tests.partition_movement import PartitionMovementMixin
from ducktape.utils.util import wait_until

import random
import time


class CloudStorageUsageTest(RedpandaTest, PartitionMovementMixin):
    message_size = 32 * 1024  # 32KiB
    log_segment_size = 256 * 1024  # 256KiB
    produce_byte_rate_per_ntp = 512 * 1024  # 512 KiB
    target_runtime = 60  # seconds
    check_interval = 5  # seconds

    topics = [
        TopicSpec(name="test-topic-1",
                  partition_count=3,
                  replication_factor=3,
                  retention_bytes=3 * log_segment_size),
        TopicSpec(name="test-topic-2",
                  partition_count=1,
                  replication_factor=1,
                  retention_bytes=3 * log_segment_size,
                  cleanup_policy=TopicSpec.CLEANUP_COMPACT)
    ]

    def __init__(self, test_context):
        self.si_settings = SISettings(
            test_context,
            log_segment_size=self.log_segment_size,
            cloud_storage_segment_max_upload_interval_sec=5,
            cloud_storage_housekeeping_interval_ms=2000)

        extra_rp_conf = dict(log_compaction_interval_ms=2000,
                             compacted_log_segment_size=self.log_segment_size)

        super(CloudStorageUsageTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf,
                             si_settings=self.si_settings)

        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)
        self.s3_port = self.si_settings.cloud_storage_api_endpoint_port

    def _create_producers(self) -> list[KgoVerifierProducer]:
        producers = []

        for topic in self.topics:
            bps = self.produce_byte_rate_per_ntp * topic.partition_count
            bytes_count = bps * self.target_runtime
            msg_count = bytes_count // self.message_size

            self.logger.info(f"Will produce {bytes_count / 1024}KiB at"
                             f"{bps / 1024}KiB/s on topic={topic.name}")
            producers.append(
                KgoVerifierProducer(self.test_context,
                                    self.redpanda,
                                    topic,
                                    msg_size=self.message_size,
                                    msg_count=msg_count,
                                    rate_limit_bps=bps))

        return producers

    def _check_usage(self):
        bucket_view = BucketView(self.redpanda)

        def check():
            actual_usage = bucket_view.total_cloud_log_size()
            reported_usage = self.admin.cloud_storage_usage()

            self.logger.info(
                f"Expected {actual_usage} bytes of cloud storage usage")
            self.logger.info(
                f"Reported {reported_usage} bytes of cloud storage usage")
            return actual_usage == reported_usage

        # Manifests are not immediately uploaded after they are mutated locally.
        # For example, during cloud storage housekeeping, the manifest is not uploaded
        # after the 'start_offset' advances, but after the segments are deleted as well.
        # If a request lands mid-housekeeping, the results will not be consistent with
        # what's in the uploaded manifest. For this reason, we wait until the two match.
        wait_until(
            check,
            timeout_sec=5,
            backoff_sec=0.2,
            err_msg=
            "Reported cloud storage usage did not match the actual usage")

    def _test_epilogue(self):
        bucket_view = BucketView(self.redpanda, topics=self.topics)

        # Assert that housekeeping operated during the test
        topic_1_manifests = [
            bucket_view.manifest_for_ntp(self.topics[0].name, p)
            for p in range(self.topics[0].partition_count)
            if bucket_view.is_ntp_in_manifest(self.topics[0].name, p)
        ]
        self.logger.info(f"MANIFESTS {topic_1_manifests}")
        assert any(
            p_man.get("start_offset", 0) > 0 for p_man in topic_1_manifests)

        # Assert that compacted segment re-upload operated during the test
        bucket_view.assert_at_least_n_uploaded_segments_compacted(
            self.topics[1].name, partition=0, n=1)

    @cluster(num_nodes=5)
    def test_cloud_storage_usage_reporting(self):
        """
        This test uses a diverse cloud storage write-only workload
        (includes retention and compacted re-uploads). It periodically,
        checks that the cloud storage usage reported by `/v1/debug/cloud_storage_usage`
        is in line with the contents of the uploaded manifest.
        """
        assert self.admin.cloud_storage_usage() == 0

        producers = self._create_producers()
        for p in producers:
            p.start()

        producers_done = lambda: all([p.is_complete() for p in producers])
        while not producers_done():
            self._check_usage()

            time.sleep(self.check_interval)

        for p in producers:
            p.wait()

        bucket_view = BucketView(self.redpanda, topics=self.topics)

        self._test_epilogue()

    @cluster(num_nodes=5)
    def test_cloud_storage_usage_reporting_with_partition_moves(self):
        """
        This test has the same workload as test_cloud_storage_usage_reporting,
        but also includes random partition movements.
        """
        assert self.admin.cloud_storage_usage() == 0

        producers = self._create_producers()
        for p in producers:
            p.start()

        partitions = []
        for topic in self.topics:
            partitions.extend([(topic.name, pid)
                               for pid in range(topic.partition_count)])

        producers_done = lambda: all([p.is_complete() for p in producers])

        while not producers_done():
            ntp_to_move = random.choice(partitions)
            self._dispatch_random_partition_move(ntp_to_move[0],
                                                 ntp_to_move[1])

            self._check_usage()

            time.sleep(self.check_interval)

        for p in producers:
            p.wait()
