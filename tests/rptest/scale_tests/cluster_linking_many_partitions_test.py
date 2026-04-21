from ducktape.mark import matrix

from rptest.tests.cluster_linking_test_base import (
    ALL_STORAGE_MODES,
    ClusterLinkingProgressVerifier,
    ShadowLinkPreAllocTestBase,
)
from rptest.clients.default import TopicSpec
from rptest.services.cluster import TestContext, cluster
from rptest.utils.scale_parameters import ScaleParameters


class ClusterLinkingScaleTest(ShadowLinkPreAllocTestBase):
    def __init__(self, test_context: TestContext):
        super().__init__(test_context=test_context)
        self.scale = ScaleParameters(
            self.redpanda,
            replication_factor=3,
            mib_per_partition=ScaleParameters.DEFAULT_MIB_PER_PARTITION,
            topic_replicas_per_shard=2000,
            tiered_storage_enabled=False,
            partition_memory_reserve_percentage=ScaleParameters.DEFAULT_PARTITIONS_MEMORY_ALLOCATION_PERCENT,
        )

    @cluster(num_nodes=7)
    @matrix(topic_count=[1, 5, 10], storage_mode=ALL_STORAGE_MODES)
    def test_many_partitions(self, topic_count: int, storage_mode: str):
        topics = [
            TopicSpec(
                name=f"source-topic-{i}",
                partition_count=int(self.scale.partition_limit / topic_count),
                replication_factor=3,
            )
            for i in range(topic_count)
        ]

        self.create_link("many_partitions_link")

        for topic in topics:
            self.create_source_topic(topic, storage_mode)

        total_bytes = 5 * 1024 * 1024 * 1024  # 5GB
        msg_size = 4 * 1024
        msg_count = int(total_bytes / msg_size)
        verifier = ClusterLinkingProgressVerifier(
            self.test_context,
            self.source_cluster,
            self.target_cluster,
            topic=topics[0].name,
            preallocated_nodes=self.preallocated_nodes,
            logger=self.logger,
            msg_count=msg_count,
            msg_size=msg_size,
        )

        verifier.start()

        success, error_msg = verifier.wait_and_verify()
        assert success, f"Verification failed: {error_msg}"
