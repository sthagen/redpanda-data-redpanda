# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any, cast

from ducktape.mark import parametrize
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.verifiable_producer import VerifiableProducer
from rptest.tests.redpanda_test import RedpandaTest

TOPIC_COMPRESSION_TYPES = ["producer", "none", "gzip", "snappy", "lz4", "zstd"]
PRODUCER_COMPRESSION_TYPES = ["none", "gzip", "snappy", "lz4", "zstd"]


class BrokerSideCompressionTest(RedpandaTest):
    """
    End-to-end tests for broker-side compression on the produce path: batches
    produced to a topic are recompressed to match the topic's effective
    `compression.type`, while a value of `producer` retains the codec chosen
    by the producing client.
    """

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            extra_rp_conf={"kafka_produce_enable_batch_compression": True},
        )
        self.rpk = RpkTool(self.redpanda)
        self.viewer = OfflineLogViewer(self.redpanda)

    def _stored_batch_codecs(self, topic: str) -> list[str]:
        """
        Codecs of the data batches stored on disk for the topic, as decoded
        (and checksum-validated) by the offline log viewer.
        """
        node = self.redpanda.nodes[0]
        partitions = cast(
            list[list[dict[str, Any]]],
            self.viewer.read_kafka_batch_headers(node, topic),
        )
        codecs: list[str] = []
        for partition_headers in partitions:
            for header in partition_headers:
                if header["type_name"] != "raft_data":
                    continue
                codecs.append(header["expanded_attrs"]["compression"])
        return codecs

    def _assert_stored_codec(
        self, topic: str, expected_codec: str, expected_batches: int
    ):
        codecs = self._stored_batch_codecs(topic)
        assert len(codecs) == expected_batches, (
            f"{topic}: expected {expected_batches} data batches, "
            f"got {len(codecs)}: {codecs}"
        )
        assert all(c == expected_codec for c in codecs), (
            f"{topic}: expected all batches stored with {expected_codec}, got {codecs}"
        )

    @staticmethod
    def _message_value(i: int) -> str:
        # Values must be large and compressible enough that clients actually
        # compress the batch: producers (e.g. franz-go) fall back to sending
        # batches uncompressed when compression does not shrink them.
        return f"val{i}-" + "x" * 1024

    def _produce_and_verify(
        self, topic_codec: str, producer_codec: str, num_messages: int = 3
    ):
        topic = f"compress-{topic_codec}-{producer_codec}"
        self.rpk.create_topic(
            topic,
            partitions=1,
            replicas=1,
            config={"compression.type": topic_codec},
        )

        for i in range(num_messages):
            self.rpk.produce(
                topic,
                f"key{i}",
                self._message_value(i),
                compression_type=producer_codec,
            )

        expected_codec = producer_codec if topic_codec == "producer" else topic_codec
        self._assert_stored_codec(topic, expected_codec, num_messages)

        consumed = self.rpk.consume(
            topic, n=num_messages, offset="start", format="%v\n"
        )
        values = consumed.splitlines()
        expected_values = [self._message_value(i) for i in range(num_messages)]
        assert values == expected_values, (
            f"{topic}: expected {expected_values}, consumed {values}"
        )

    @cluster(num_nodes=1)
    def test_compression_type_matrix(self):
        """
        Cover the full matrix of topic `compression.type` x producer codec:
        the codec of the stored batches (and of batches served to consumers)
        is the topic's compression type, unless it is `producer`, in which
        case the producer's codec is retained. Consumers must be able to read
        back the produced values in all combinations.
        """
        for topic_codec in TOPIC_COMPRESSION_TYPES:
            for producer_codec in PRODUCER_COMPRESSION_TYPES:
                self._produce_and_verify(topic_codec, producer_codec)

    @cluster(num_nodes=1)
    def test_batch_compression_disabled(self):
        """
        With kafka_produce_enable_batch_compression disabled, the topic's
        `compression.type` is ignored on the produce path and the producer's
        codec is retained.
        """
        self.redpanda.set_cluster_config(
            {"kafka_produce_enable_batch_compression": False}
        )

        num_messages = 3
        topic = "compress-disabled"
        self.rpk.create_topic(
            topic, partitions=1, replicas=1, config={"compression.type": "zstd"}
        )
        for i in range(num_messages):
            self.rpk.produce(
                topic, f"key{i}", self._message_value(i), compression_type="gzip"
            )

        self._assert_stored_codec(topic, "gzip", num_messages)

    @cluster(num_nodes=2)
    @parametrize(producer_codec="snappy", topic_codec="zstd")
    @parametrize(producer_codec="zstd", topic_codec="snappy")
    def test_java_producer_recompression(self, producer_codec: str, topic_codec: str):
        """
        Produce with an idempotent Java client (VerifiableProducer) using a
        codec that differs from the topic's `compression.type`, and verify
        that batches are stored with the topic's codec and remain consumable.
        The snappy cases matter most: Java clients use xerial-framed snappy,
        which the broker must both decode on the way in and encode
        compatibly on the way out.
        """
        num_messages = 100
        topic = f"compress-java-{producer_codec}-{topic_codec}"
        self.rpk.create_topic(
            topic,
            partitions=1,
            replicas=1,
            config={"compression.type": topic_codec},
        )

        producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=topic,
            max_messages=num_messages,
            compression_types=[producer_codec],
            enable_idempotence=True,
        )
        producer.start()
        try:
            wait_until(
                lambda: producer.num_acked >= num_messages,
                timeout_sec=60,
                backoff_sec=1,
                err_msg=f"Timed out waiting for {num_messages} acked messages.",
            )
        finally:
            producer.stop()
            producer.clean()
            producer.free()

        codecs = self._stored_batch_codecs(topic)
        assert len(codecs) > 0, f"{topic}: no data batches found"
        assert all(c == topic_codec for c in codecs), (
            f"{topic}: expected all batches stored with {topic_codec}, got {codecs}"
        )

        consumed = self.rpk.consume(
            topic, n=num_messages, offset="start", format="%v\n"
        )
        values = consumed.splitlines()
        assert len(values) == num_messages and all(v.isdigit() for v in values), (
            f"{topic}: unexpected consumed values {values}"
        )
