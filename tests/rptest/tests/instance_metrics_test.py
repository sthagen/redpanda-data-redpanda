# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until
from prometheus_client.samples import Sample

from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    MetricsEndpoint,
    get_cloud_provider,
)
from rptest.tests.redpanda_test import RedpandaTest

# The instance-info metrics are registered on both endpoints; the only
# difference is the metric-name prefix.
_ENDPOINTS: list[tuple[MetricsEndpoint, str]] = [
    (MetricsEndpoint.METRICS, "vectorized_"),
    (MetricsEndpoint.PUBLIC_METRICS, "redpanda_"),
]

# Capacity gauges that accompany the info metric when the instance type is
# present in the generated table.
_CAPACITY_GAUGES: list[str] = [
    "vcpus",
    "memory_bytes",
    "disk_bytes",
    "network_bytes_per_sec",
    "read_iops",
    "write_iops",
]


class InstanceMetricsTest(RedpandaTest):
    """Verify the cloud-instance capacity metrics (redpanda_instance_*).

    Instance detection currently relies on the cloud metadata service, so the
    interesting assertions only hold on dedicated (CDT) nodes. In docker the
    test effectively skips.
    """

    def _families(
        self, node: ClusterNode, endpoint: MetricsEndpoint
    ) -> dict[str, list[Sample]]:
        """Return {metric_name: [samples]} for the given endpoint on a node."""
        return {
            f.name: f.samples
            for f in self.redpanda.metrics(node, metrics_endpoint=endpoint)
        }

    def _check_endpoint(
        self,
        node: ClusterNode,
        endpoint: MetricsEndpoint,
        prefix: str,
        provider: str,
    ) -> None:
        info_name = f"{prefix}instance_info"

        # Detection runs in a background fiber at startup, so the series may not
        # be registered the instant the cluster reports healthy. Wait for it.
        wait_until(
            lambda: info_name in self._families(node, endpoint),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"{info_name} never appeared on the {endpoint.value} endpoint",
        )

        families = self._families(node, endpoint)
        info = families[info_name]
        assert len(info) == 1, f"expected 1 {info_name} sample, got {len(info)}"
        labels = info[0].labels

        assert labels["cloud_provider"] == provider, (
            f"expected cloud_provider={provider}, got {labels['cloud_provider']}"
        )
        assert labels["instance_type"] not in ("", "unknown"), (
            f"instance type not detected: {labels['instance_type']!r}"
        )
        assert int(info[0].value) == 1, "info value should be 1 when detected"

        # If this fails on a real CDT instance, the detected instance type is
        # missing from the capacity table. Regenerate it (the cpp-instance-info
        # command of generate_from_instance_data.py) to add the type to
        # src/v/metrics/instance_info_table.cc.
        assert labels["instance_has_capacity"] == "1", (
            f"instance type {labels['instance_type']!r} has no capacity entry; "
            f"update the table in src/v/metrics/instance_info_table.cc"
        )

        values: dict[str, float] = {}
        for gauge in _CAPACITY_GAUGES:
            name = f"{prefix}instance_{gauge}"
            assert name in families, f"missing {name} on {endpoint.value}"
            samples = families[name]
            assert len(samples) == 1, f"expected 1 {name} sample, got {len(samples)}"
            values[gauge] = samples[0].value

        # vcpus, memory and network are always positive for a real instance;
        # disk and IOPS can legitimately be 0 (instances without local SSD).
        assert values["vcpus"] > 0, f"non-positive vcpus: {values['vcpus']}"
        assert values["memory_bytes"] > 0, (
            f"non-positive memory_bytes: {values['memory_bytes']}"
        )
        assert values["network_bytes_per_sec"] > 0, (
            f"non-positive network_bytes_per_sec: {values['network_bytes_per_sec']}"
        )
        for gauge in ("disk_bytes", "read_iops", "write_iops"):
            assert values[gauge] >= 0, f"negative {gauge}: {values[gauge]}"

        # Cross-check the reported vCPU count against the actual machine: on a
        # dedicated node nproc reports the instance's full vCPU count, which is
        # exactly what the table records.
        nproc = int(node.account.ssh_output("nproc").decode().strip())
        assert values["vcpus"] == nproc, (
            f"vcpus {values['vcpus']} != nproc {nproc} for {labels['instance_type']}"
        )

    @cluster(num_nodes=1)
    def test_instance_capacity_metrics(self) -> None:
        """On dedicated (CDT) nodes, verify the instance metrics report the
        correct provider and sane capacity values on both metric endpoints."""
        if not self.redpanda.dedicated_nodes:
            self.logger.info(
                "skipping: instance capacity metrics only meaningful on "
                "dedicated (CDT) nodes; metadata service is unavailable in docker"
            )
            return

        provider = get_cloud_provider()
        # TODO: instance detection is currently AWS-only. Once gcp/azure
        # detection lands, broaden this to cover them; until then a non-aws CDT
        # run should fail loudly rather than silently pass.
        assert provider == "aws", (
            f"instance detection not yet implemented for provider={provider!r}"
        )

        node = self.redpanda.nodes[0]
        for endpoint, prefix in _ENDPOINTS:
            self._check_endpoint(node, endpoint, prefix, provider)
