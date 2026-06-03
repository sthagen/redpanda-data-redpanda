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


class CanaryBenchmarkServiceSelfTest(Test):
    def __init__(self, test_context: TestContext) -> None:
        super().__init__(test_context)

    @cluster(num_nodes=1)
    def test_fio_smoke(self) -> None:
        svc = FioBenchmarkService(
            self.test_context,
            filename="/tmp/canary-fio-smoke-file",
            size="32m",
            warmup_s=0,
            duration_s=3,
        )
        svc.start()
        svc.wait(timeout_sec=60)

        metrics = svc.metrics()
        self.logger.debug(f"fio smoke metrics: {metrics}")
        assert metrics.iops > 0, "Expected fio IOPS > 0"
        assert metrics.p50_latency_ns > 0, "Expected fio p50 latency > 0"
        svc.write_metrics_result(metrics)

    @cluster(num_nodes=1)
    def test_stress_ng_smoke(self) -> None:
        svc = StressNgBenchmarkService(
            self.test_context,
            matrix_workers=1,
            duration_s=3,
        )
        svc.start()
        svc.wait(timeout_sec=60)

        metrics = svc.metrics()
        self.logger.debug(f"stress-ng smoke metrics: {metrics}")
        assert metrics.bogo_ops_per_sec > 0, "Expected stress-ng bogo ops/s > 0"
        svc.write_metrics_result(metrics)

    @cluster(num_nodes=2)
    def test_iperf3_smoke(self) -> None:
        svc = Iperf3BenchmarkService(
            self.test_context,
            parallel=1,
            warmup_s=0,
            duration_s=3,
        )
        svc.start()
        svc.wait(timeout_sec=60)

        metrics = svc.metrics()
        self.logger.debug(f"iperf3 smoke metrics: {metrics}")
        assert metrics.throughput_bps > 0, "Expected iperf3 throughput > 0"
        svc.write_metrics_result(metrics)
