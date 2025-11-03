# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0


from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase
from rptest.services.cluster import cluster
from rptest.clients.rpk import RpkTool
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import OMBSampleConfigurations


class ClusterLinkingOMBTest(ShadowLinkTestBase):
    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(self.source_cluster.service)

    def get_target_cluster_rpk(self) -> RpkTool:
        return RpkTool(self.target_cluster.service)

    @cluster(num_nodes=9)
    def test_omb(self):
        """
        A basic PoC test for OMB producing to the source cluster and consuming
        from the target cluster.
        """
        topic_name = "testtopic"
        link_name = "test-link"
        topic_partitions = 50
        producer_rate_bytes_s = 50 * 1024  # 50 KiB/s

        self.create_link(link_name)
        self.get_source_cluster_rpk().create_topic(
            topic=topic_name,
            partitions=topic_partitions,
            replicas=3,
        )
        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic_name, topic_partitions),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic_name} not found in target cluster",
        )

        workload = {
            "name": "DRWorkload",
            "existing_topic_list": [topic_name],
            "subscriptions_per_topic": 1,
            "consumer_per_subscription": 5,
            "producers_per_topic": 5,
            "producer_rate": producer_rate_bytes_s // 1024,
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 1,
            "warmup_duration_minutes": 1,
            "message_size": 1024,
            "payload_file": "payload/payload-1Kb.data",
        }
        driver = {
            "name": "DRWorkloadDriver",
            "replication_factor": 3,
            "request_timeout": 300000,
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                "linger.ms": 10,
                "max.in.flight.requests.per.connection": 5,
                "batch.size": 16384,
            },
            "consumer_config": {
                # have the consumers consume from the backup cluster.
                "bootstrap.servers": f"{self.target_cluster.service.nodes[0].account.hostname}:9092",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "max.partition.fetch.bytes": 131072,
            },
        }
        validator = {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    (0.9 * producer_rate_bytes_s) // (1024 * 1024)
                )
            ]
        }

        benchmark = OpenMessagingBenchmark(
            ctx=self.test_context,
            redpanda=self.source_cluster_service,
            driver=driver,
            workload=(workload, validator),
            topology="ensemble",
        )
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time_mins() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
