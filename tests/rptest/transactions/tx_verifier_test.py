# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess

from ducktape.errors import DucktapeError
from ducktape.mark import matrix
from ducktape.tests.test import TestContext

from rptest.context.cloud_storage import CloudStorageType
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    SISettings,
    get_cloud_storage_type,
    CLOUD_TOPICS_CONFIG_STR,
)
from rptest.tests.redpanda_test import RedpandaTest

# Expected log errors in tests that test misbehaving
# transactional clients.
TX_ERROR_LOGS = [
    # e.g. tx - [{kafka/topic1/0}] - rm_stm.cc:461 - Can't prepare pid:{producer_identity: id=1, epoch=27} - unknown session
    "tx -.*rm_stm.*unknown session"
]


class TxVerifierTest(RedpandaTest):
    """
    Verify that segment indices are recovered on startup.
    """

    def __init__(self, test_context: TestContext):
        extra_rp_conf = {
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
            CLOUD_TOPICS_CONFIG_STR: True,
        }

        super(TxVerifierTest, self).__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
            si_settings=SISettings(
                test_context=test_context,
                cloud_storage_enable_remote_read=False,
                cloud_storage_enable_remote_write=False,
            ),
        )

    def verify(self, tests: list[str], topic1: str, topic2: str, groupId: str):
        verifier_jar = "/opt/verifiers/verifiers.jar"

        errors = ""

        for test in tests:
            self.redpanda.logger.info('testing txn test "{test}"'.format(test=test))
            try:
                cmd = "{java} -cp {verifier_jar} io.vectorized.tx_verifier.Verifier {test} {brokers} {topic1} {topic2} {groupId}".format(
                    java="java",
                    verifier_jar=verifier_jar,
                    test=test,
                    brokers=self.redpanda.brokers(),
                    topic1=topic1,
                    topic2=topic2,
                    groupId=groupId,
                )
                subprocess.check_output(
                    ["/bin/sh", "-c", cmd], stderr=subprocess.STDOUT, timeout=240
                )
                self.redpanda.logger.info('txn test "{test}" passed'.format(test=test))
            except subprocess.CalledProcessError as e:
                self.redpanda.logger.info('txn test "{test}" failed'.format(test=test))
                errors += test + "\n"
                errors += str(e.output) + "\n"
                errors += "---------------------------\n"

        if len(errors) > 0:
            raise DucktapeError(errors)

    @cluster(num_nodes=3, log_allow_list=TX_ERROR_LOGS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_all_tx_tests(self, cloud_storage_type: CloudStorageType):
        rpk = RpkTool(self.redpanda)

        specs: list[tuple[str, str, str]] = []

        # 2 standard topics
        specs.append(("topic1-std", "topic2-std", "groupId-std"))
        rpk.create_topic(specs[-1][0])
        rpk.create_topic(specs[-1][1])

        # 2 cloud topics
        cloud_topic_config = {
            TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
        }
        specs.append(("topic1-ct", "topic2-ct", "groupId-ct"))
        rpk.create_topic(specs[-1][0], config=cloud_topic_config)
        rpk.create_topic(specs[-1][1], config=cloud_topic_config)

        # 1 standard topic and 1 cloud topic
        specs.append(("topic1-std2", "topic2-ct2", "groupId-mixed"))
        rpk.create_topic(specs[-1][0])
        rpk.create_topic(specs[-1][1], config=cloud_topic_config)

        tests = [
            "init",
            "tx",
            "txes",
            "abort",
            "commuting-txes",
            "conflicting-tx",
            "read-committed-seek",
            "read-uncommitted-seek",
            "read-committed-tx-seek",
            "read-uncommitted-tx-seek",
            "fetch-reads-committed-txs",
            "fetch-skips-aborted-txs",
            "read-committed-seek-waits-ongoing-tx",
            "read-committed-seek-waits-long-hanging-tx",
            "read-committed-seek-reads-short-hanging-tx",
            "read-uncommitted-seek-reads-ongoing-tx",
            "set-group-start-offset",
            "read-process-write",
        ]

        for spec in specs:
            self.verify(tests, *spec)
