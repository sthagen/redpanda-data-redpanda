# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec

from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.kgo_repeater_service import repeater_traffic


class OpenBenchmarkSelfTest(RedpandaTest):
    """
    This test verifies that OpenMessagingBenchmark service
    works as expected.  It is not a test of redpanda itself: this is
    to avoid committing changes that break services that might only
    be used in nightlies and not normally used in tests run on PRS.
    """
    BENCHMARK_WAIT_TIME_MIN = 5

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    @cluster(num_nodes=6)
    @matrix(driver=["SIMPLE_DRIVER"], workload=["SIMPLE_WORKLOAD"])
    def test_default_omb_configuration(self, driver, workload):
        benchmark = OpenMessagingBenchmark(self.test_context, self.redpanda,
                                           driver, workload)
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + self.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        # docker runs have high variance in perf numbers, check only in dedicate node
        # setup.
        benchmark.check_succeed(validate_metrics=self.redpanda.dedicated_nodes)


class KgoRepeaterSelfTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    @cluster(num_nodes=4)
    def test_kgo_repeater(self):
        topic = 'test'
        self.client().create_topic(
            TopicSpec(name=topic,
                      partition_count=16,
                      retention_bytes=16 * 1024 * 1024,
                      segment_bytes=1024 * 1024))
        with repeater_traffic(context=self.test_context,
                              redpanda=self.redpanda,
                              topic=topic,
                              msg_size=4096,
                              workers=1) as repeater:
            repeater.await_group_ready()
            repeater.await_progress(1024, timeout_sec=60)
