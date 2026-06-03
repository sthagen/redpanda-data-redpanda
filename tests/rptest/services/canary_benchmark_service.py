# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from __future__ import annotations

import json
import os
import shlex
import signal
from dataclasses import asdict, dataclass
from typing import Any, cast

import yaml
from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until


RESULT_FILE_NAME = "result.json"


def _result_file_path(context: TestContext) -> str:
    result_dir = TestContext.results_dir(context, context.test_index)
    os.makedirs(result_dir, exist_ok=True)
    return os.path.join(result_dir, RESULT_FILE_NAME)


def _write_metrics_result(context: TestContext, metrics: Any) -> None:
    with open(_result_file_path(context), "w", encoding="utf-8") as result_file:
        json.dump(asdict(metrics), result_file, indent=2, sort_keys=True)
        result_file.write("\n")


def _read_remote_text(node: ClusterNode, path: str) -> str:
    output = node.account.ssh_output(f"cat {shlex.quote(path)}", timeout_sec=10)
    return output.decode("utf-8")


def _stop_pid(node: ClusterNode, pid: int) -> None:
    try:
        node.account.signal(pid, signal.SIGKILL, allow_fail=False)
    except RemoteCommandError as e:
        if "No such process" not in str(e.msg):
            raise


@dataclass
class FioBenchmarkMetrics:
    iops: float
    p50_latency_ns: float


class FioBenchmarkService(Service):
    PROCESS_NAME = "fio"
    LOG_PATH = "/tmp/fio_benchmark.log"
    METRICS_PATH = "/tmp/fio_benchmark_metrics.json"

    logs = {
        "fio_benchmark_output": {
            "path": LOG_PATH,
            "collect_default": True,
        }
    }

    def __init__(
        self,
        context: TestContext,
        *,
        filename: str = "/var/lib/redpanda/canary-fio-benchmark-file",
        rw: str = "randwrite",
        block_size: str = "4k",
        ioengine: str = "libaio",
        iodepth: int = 8,
        size: str = "4g",
        warmup_s: int = 10,
        duration_s: int = 60,
    ) -> None:
        super().__init__(context, num_nodes=1)
        self._filename = filename
        self._rw = rw
        self._block_size = block_size
        self._ioengine = ioengine
        self._iodepth = iodepth
        self._size = size
        self._warmup_s = warmup_s
        self._duration_s = duration_s
        self._pids: dict[str, int] = {}

    def _build_cmd(self) -> str:
        return " ".join(
            [
                "fio",
                "--name=canary-disk",
                f"--filename={shlex.quote(self._filename)}",
                f"--rw={self._rw}",
                f"--bs={self._block_size}",
                f"--ioengine={self._ioengine}",
                f"--iodepth={self._iodepth}",
                "--direct=1",
                "--numjobs=1",
                "--time_based",
                f"--runtime={self._duration_s}",
                f"--ramp_time={self._warmup_s}",
                f"--size={self._size}",
                "--group_reporting",
                "--output-format=json",
                f"--output={self.METRICS_PATH}",
            ]
        )

    def start_node(self, node: ClusterNode, **kwargs: Any) -> None:
        self.clean_node(node, **kwargs)
        parent_dir = os.path.dirname(self._filename)
        if parent_dir:
            node.account.mkdirs(parent_dir)
        wrapped_cmd = f"nohup {self._build_cmd()} >> {self.LOG_PATH} 2>&1 & echo $!"
        pid = int(node.account.ssh_output(wrapped_cmd, timeout_sec=10).strip())
        self._pids[node.name] = pid
        self.logger.debug(f"Spawned fio benchmark node={node.name} pid={pid}")

    def wait_node(self, node: ClusterNode, timeout_sec: float | None = None) -> bool:
        pid = self._pids[node.name]
        timeout = timeout_sec or self._duration_s + self._warmup_s + 120
        wait_until(
            lambda: not node.account.exists(f"/proc/{pid}"),
            timeout_sec=timeout,
            backoff_sec=2,
            err_msg=f"fio benchmark did not finish in {timeout}s (pid={pid})",
        )
        return True

    def stop_node(self, node: ClusterNode, **kwargs: Any) -> None:
        pid = self._pids.get(node.name)
        if pid is not None:
            _stop_pid(node, pid)

    def clean_node(self, node: ClusterNode, **kwargs: Any) -> None:
        node.account.kill_process(self.PROCESS_NAME, clean_shutdown=False)
        node.account.remove(self.LOG_PATH, allow_fail=True)
        node.account.remove(self.METRICS_PATH, allow_fail=True)
        node.account.remove(self._filename, allow_fail=True)

    def write_metrics_result(self, metrics: FioBenchmarkMetrics) -> None:
        _write_metrics_result(self.context, metrics)

    def metrics(self) -> FioBenchmarkMetrics:
        node = self.nodes[0]
        if not node.account.exists(self.METRICS_PATH):
            raise RuntimeError(f"fio metrics json not found at {self.METRICS_PATH}")

        output = _read_remote_text(node, self.METRICS_PATH)
        metrics = json.loads(output)
        job = metrics["jobs"][0]
        section = job["read"] if float(job["read"]["iops"]) > 0 else job["write"]
        return FioBenchmarkMetrics(
            iops=float(section["iops"]),
            p50_latency_ns=float(section["clat_ns"]["percentile"]["50.000000"]),
        )


@dataclass
class StressNgBenchmarkMetrics:
    bogo_ops_per_sec: float


class StressNgBenchmarkService(Service):
    PROCESS_NAME = "stress-ng"
    LOG_PATH = "/tmp/stress_ng_benchmark.log"
    METRICS_PATH = "/tmp/stress_ng_benchmark_metrics.yaml"
    MATRIX_METHOD = "prod"
    # Results in ~3MB per worker which means it uses a decent amount of L3 on xlarge
    MATRIX_SIZE = 512

    logs = {
        "stress_ng_benchmark_output": {
            "path": LOG_PATH,
            "collect_default": True,
        }
    }

    def __init__(
        self,
        context: TestContext,
        *,
        matrix_workers: int = 0,
        duration_s: int = 60,
    ) -> None:
        super().__init__(context, num_nodes=1)
        self._matrix_workers = matrix_workers
        self._duration_s = duration_s
        self._pids: dict[str, int] = {}

    def _build_cmd(self) -> str:
        return " ".join(
            [
                "stress-ng",
                f"--matrix {self._matrix_workers}",
                f"--matrix-method {self.MATRIX_METHOD}",
                f"--matrix-size {self.MATRIX_SIZE}",
                f"--timeout {self._duration_s}s",
                "--metrics-brief",
                "--no-rand-seed",
                f"--yaml {self.METRICS_PATH}",
            ]
        )

    def start_node(self, node: ClusterNode, **kwargs: Any) -> None:
        self.clean_node(node, **kwargs)
        wrapped_cmd = f"nohup {self._build_cmd()} >> {self.LOG_PATH} 2>&1 & echo $!"
        pid = int(node.account.ssh_output(wrapped_cmd, timeout_sec=10).strip())
        self._pids[node.name] = pid
        self.logger.debug(f"Spawned stress-ng benchmark node={node.name} pid={pid}")

    def wait_node(self, node: ClusterNode, timeout_sec: float | None = None) -> bool:
        pid = self._pids[node.name]
        timeout = timeout_sec or self._duration_s + 120
        wait_until(
            lambda: not node.account.exists(f"/proc/{pid}"),
            timeout_sec=timeout,
            backoff_sec=2,
            err_msg=f"stress-ng benchmark did not finish in {timeout}s (pid={pid})",
        )
        return True

    def stop_node(self, node: ClusterNode, **kwargs: Any) -> None:
        pid = self._pids.get(node.name)
        if pid is not None:
            _stop_pid(node, pid)

    def clean_node(self, node: ClusterNode, **kwargs: Any) -> None:
        node.account.kill_process(self.PROCESS_NAME, clean_shutdown=False)
        node.account.remove(self.LOG_PATH, allow_fail=True)
        node.account.remove(self.METRICS_PATH, allow_fail=True)

    def write_metrics_result(self, metrics: StressNgBenchmarkMetrics) -> None:
        _write_metrics_result(self.context, metrics)

    def metrics(self) -> StressNgBenchmarkMetrics:
        node = self.nodes[0]
        if not node.account.exists(self.METRICS_PATH):
            raise RuntimeError(
                f"stress-ng metrics yaml not found at {self.METRICS_PATH}"
            )

        parsed = yaml.safe_load(_read_remote_text(node, self.METRICS_PATH))
        metrics = cast(dict[str, list[dict[str, str | int | float]]], parsed)[
            "metrics"
        ][0]
        return StressNgBenchmarkMetrics(
            bogo_ops_per_sec=float(metrics["bogo-ops-per-second-real-time"])
        )


@dataclass
class Iperf3BenchmarkMetrics:
    throughput_bps: float


class Iperf3BenchmarkService(Service):
    PROCESS_NAME = "iperf3"
    SERVER_LOG_PATH = "/tmp/iperf3_benchmark_server.log"
    CLIENT_LOG_PATH = "/tmp/iperf3_benchmark_client.log"
    METRICS_PATH = "/tmp/iperf3_benchmark_metrics.json"

    logs = {
        "iperf3_benchmark_server_output": {
            "path": SERVER_LOG_PATH,
            "collect_default": True,
        },
        "iperf3_benchmark_client_output": {
            "path": CLIENT_LOG_PATH,
            "collect_default": True,
        },
    }

    def __init__(
        self,
        context: TestContext,
        *,
        port: int = 5201,
        parallel: int = 4,
        warmup_s: int = 5,
        duration_s: int = 60,
    ) -> None:
        super().__init__(context, num_nodes=2)
        self._port = port
        self._parallel = parallel
        self._warmup_s = warmup_s
        self._duration_s = duration_s
        self._pids: dict[str, int] = {}

    def _server(self) -> ClusterNode:
        return self.nodes[0]

    def _client(self) -> ClusterNode:
        return self.nodes[1]

    def start_node(self, node: ClusterNode, **kwargs: Any) -> None:
        self.clean_node(node, **kwargs)
        if node == self._server():
            cmd = f"iperf3 -s -p {self._port}"
            wrapped_cmd = f"nohup {cmd} >> {self.SERVER_LOG_PATH} 2>&1 & echo $!"
        else:
            server_host = self._server().account.hostname
            wait_until(
                lambda: self._server_ready(node, server_host),
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"iperf3 server {server_host}:{self._port} did not become ready",
            )
            cmd = (
                f"iperf3 -c {server_host} -p {self._port} -P {self._parallel} "
                f"-O {self._warmup_s} -t {self._duration_s} -J"
            )
            wrapped_cmd = f"nohup {cmd} > {self.METRICS_PATH} 2>> {self.CLIENT_LOG_PATH} & echo $!"

        pid = int(node.account.ssh_output(wrapped_cmd, timeout_sec=10).strip())
        self._pids[node.name] = pid
        self.logger.debug(f"Spawned iperf3 benchmark node={node.name} pid={pid}")

    def _server_ready(self, node: ClusterNode, server_host: str) -> bool:
        try:
            node.account.ssh_output(
                f"nc -z {server_host} {self._port}",
                timeout_sec=5,
            )
            return True
        except RemoteCommandError:
            return False

    def wait_node(self, node: ClusterNode, timeout_sec: float | None = None) -> bool:
        pid = self._pids[node.name]
        timeout = timeout_sec or self._duration_s + self._warmup_s + 120
        if node == self._server():
            client_pid = self._pids[self._client().name]
            wait_until(
                lambda: not self._client().account.exists(f"/proc/{client_pid}"),
                timeout_sec=timeout,
                backoff_sec=2,
                err_msg=f"iperf3 client did not finish in {timeout}s (pid={client_pid})",
            )
            _stop_pid(node, pid)
            return True

        wait_until(
            lambda: not node.account.exists(f"/proc/{pid}"),
            timeout_sec=timeout,
            backoff_sec=2,
            err_msg=f"iperf3 benchmark did not finish in {timeout}s (pid={pid})",
        )
        return True

    def stop_node(self, node: ClusterNode, **kwargs: Any) -> None:
        pid = self._pids.get(node.name)
        if pid is not None:
            _stop_pid(node, pid)

    def clean_node(self, node: ClusterNode, **kwargs: Any) -> None:
        node.account.kill_process(self.PROCESS_NAME, clean_shutdown=False)
        node.account.remove(self.SERVER_LOG_PATH, allow_fail=True)
        node.account.remove(self.CLIENT_LOG_PATH, allow_fail=True)
        node.account.remove(self.METRICS_PATH, allow_fail=True)

    def write_metrics_result(self, metrics: Iperf3BenchmarkMetrics) -> None:
        _write_metrics_result(self.context, metrics)

    def metrics(self) -> Iperf3BenchmarkMetrics:
        node = self._client()
        if not node.account.exists(self.METRICS_PATH):
            raise RuntimeError(f"iperf3 metrics json not found at {self.METRICS_PATH}")

        output = _read_remote_text(node, self.METRICS_PATH)
        metrics = json.loads(output)
        end = metrics["end"]
        if "sum_received" in end:
            throughput_bps = float(end["sum_received"]["bits_per_second"])
        else:
            throughput_bps = float(end["sum_sent"]["bits_per_second"])
        return Iperf3BenchmarkMetrics(throughput_bps=throughput_bps)
