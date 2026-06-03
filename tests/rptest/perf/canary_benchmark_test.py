# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.tests.test import Test, TestContext
from ducktape.mark.resource import cluster

from rptest.services.canary_benchmark_service import (
    FioBenchmarkService,
    Iperf3BenchmarkService,
    StressNgBenchmarkService,
)


class CanaryBenchmarkPerf(Test):
    def __init__(self, test_context: TestContext) -> None:
        super().__init__(test_context)

    @cluster(num_nodes=1)
    def test_disk(self) -> None:
        svc = FioBenchmarkService(
            self.test_context,
            filename="/var/lib/redpanda/canary-fio-benchmark-file",
            size="2g",
            warmup_s=30,
            duration_s=60,
        )
        svc.start()
        svc.wait(timeout_sec=300)

        metrics = svc.metrics()
        assert metrics.iops > 0, "Expected fio IOPS > 0"
        assert metrics.p50_latency_ns > 0, "Expected fio p50 latency > 0"
        svc.write_metrics_result(metrics)

    @cluster(num_nodes=1)
    def test_cpu(self) -> None:
        svc = StressNgBenchmarkService(
            self.test_context,
            matrix_workers=0,
            duration_s=60,
        )
        svc.start()
        svc.wait(timeout_sec=180)

        metrics = svc.metrics()
        assert metrics.bogo_ops_per_sec > 0, "Expected stress-ng bogo ops/s > 0"
        svc.write_metrics_result(metrics)

    @cluster(num_nodes=2)
    def test_network(self) -> None:
        svc = Iperf3BenchmarkService(
            self.test_context,
            parallel=4,
            warmup_s=5,
            duration_s=60,
        )
        svc.start()
        svc.wait(timeout_sec=300)

        metrics = svc.metrics()
        assert metrics.throughput_bps > 0, "Expected iperf3 throughput > 0"
        svc.write_metrics_result(metrics)
