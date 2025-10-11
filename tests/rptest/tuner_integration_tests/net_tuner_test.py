# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.tests.test import TestContext
from ducktape.cluster.cluster import ClusterNode
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk_remote import RpkRemoteTool

import dataclasses
import re

# only works with --max-parallel 1

CORE_COUNT = 4


class NetTunerTest(RedpandaTest):
    TARGET_RFS_TABLE_SIZE = 32768

    def __init__(self, ctx: TestContext):
        super().__init__(test_context=ctx)

    def setUp(self):
        # Skip starting redpanda, so that test can explicitly start it with rpk
        self.node = self.redpanda.nodes[0]
        self.redpanda.clean_node(self.node)
        self.rpk = RpkRemoteTool(self.redpanda, self.node)
        self.rpk.mode_set("production")

        interfaces = (
            self.redpanda.nodes[0]
            .account.ssh_output("ls /sys/class/net")
            .decode("utf-8")
            .strip()
            .split("\n")
        )
        assert len(interfaces) == 2, f"Unexpected amount of interfaces: {interfaces}"
        for interface in interfaces:
            if interface != "lo":
                self.interface_name = interface
                break

        self.logger.info(f"Found interface {self.interface_name}")

    def teardown(self):
        super().teardown()

        # Reset to what ansible gives you out of the box
        self.node.account.ssh("rm -rf /etc/redpanda/redpanda.yaml")
        self.node.account.ssh("sudo rpk redpanda mode prod")
        self.node.account.ssh("sudo systemctl restart redpanda-tuner")

    def start_rp(self):
        # Need to explicitly pass listener config otherwise RP will complain about 0.0.0.0 listeners
        self.redpanda.start_node_with_rpk(
            self.node,
            additional_args=f"--rpc-addr={self.node.account.hostname} --kafka-addr=dnslistener://{self.node.account.hostname}",
            clean_node=False,
        )

    @dataclasses.dataclass
    class ExpectedInterruptSetup:
        interrupts_masks: list[str]
        redpanda_cores: set[int]
        rps_cpu_mask: str
        rps_cpu_flow_count: int
        rfs_table_size: int
        rx_tx_queue_count: int

    def get_interrupt_match(self) -> str:
        return self.interface_name

    @staticmethod
    def parse_matching_interrupt_num_from_interrupt_file(
        interrupt_file: str, matcher: str
    ) -> list[int]:
        ret: list[int] = []
        pattern = re.compile(matcher)
        for line in interrupt_file.split("\n"):
            if pattern.search(line):
                ret.append(int(line.split(":")[0]))

        return ret

    def _test_irq_balance(self):
        # test irqbalance
        irqbalance_output = self.node.account.ssh_output(
            "systemctl status irqbalance"
        ).decode("utf-8")
        assert "--banirq" in irqbalance_output, (
            f"irqbalance is not configured correctly, got {irqbalance_output}"
        )

    def _get_interrupt_ids(self) -> list[int]:
        interrupts_file = self.node.account.ssh_output("cat /proc/interrupts").decode(
            "utf-8"
        )
        return self.parse_matching_interrupt_num_from_interrupt_file(
            interrupts_file, self.get_interrupt_match()
        )

    def _test_interrupt_config(
        self,
        node: ClusterNode,
        rpk: RpkRemoteTool,
        expected_interrupt_setup: ExpectedInterruptSetup,
    ):
        self._test_irq_balance()

        # test interrupts
        interrupt_ids = self._get_interrupt_ids()

        assert len(interrupt_ids) == len(expected_interrupt_setup.interrupts_masks), (
            f"Got more interrupts/queues than expected, got {interrupt_ids} expected {expected_interrupt_setup.interrupts_masks}"
        )

        for interrupt_id, target_mask in zip(
            interrupt_ids, expected_interrupt_setup.interrupts_masks
        ):
            cpu_affinity = (
                node.account.ssh_output(f"cat /proc/irq/{interrupt_id}/smp_affinity")
                .decode("utf-8")
                .strip()
            )
            assert int(cpu_affinity, 16) == int(target_mask, 16), (
                f"IRQ {interrupt_id} smp_affinity is not set correctly, got {cpu_affinity} expected {target_mask}"
            )

        rx_tx_queue_count = int(
            node.account.ssh_output(
                f"ls /sys/class/net/{self.interface_name}/queues/ | grep rx- | wc -l"
            )
            .decode("utf-8")
            .strip()
        )

        assert rx_tx_queue_count == expected_interrupt_setup.rx_tx_queue_count, (
            f"Got unexpected amount of queues, got {rx_tx_queue_count} expected {expected_interrupt_setup.rx_tx_queue_count}"
        )

        # test RPS
        for i in range(rx_tx_queue_count):
            rps_cpu = (
                node.account.ssh_output(
                    f"cat /sys/class/net/{self.interface_name}/queues/rx-{i}/rps_cpus"
                )
                .decode("utf-8")
                .strip()
            )
            assert rps_cpu == expected_interrupt_setup.rps_cpu_mask, (
                f"rps_cpus for queue {i} is not set correctly, got {rps_cpu} expected {expected_interrupt_setup.rps_cpu_mask}"
            )

        # test RFS
        targetRFSTableSize = expected_interrupt_setup.rfs_table_size

        total_rps_sock_flow_entries = int(
            node.account.ssh_output("cat /proc/sys/net/core/rps_sock_flow_entries")
            .decode("utf-8")
            .strip()
        )
        assert total_rps_sock_flow_entries == targetRFSTableSize, (
            f"rps_sock_flow_entries is not set correctly, got {total_rps_sock_flow_entries} expected {targetRFSTableSize}"
        )

        for i in range(rx_tx_queue_count):
            per_queue_rps_flow_count = int(
                node.account.ssh_output(
                    f"cat /sys/class/net/{self.interface_name}/queues/rx-{i}/rps_flow_cnt"
                )
                .decode("utf-8")
                .strip()
            )
            assert (
                per_queue_rps_flow_count == expected_interrupt_setup.rps_cpu_flow_count
            ), (
                f"rps_flow_cnt for queue {i} is not set correctly, got {per_queue_rps_flow_count} expected {expected_interrupt_setup.rps_cpu_flow_count}"
            )

        # check RP runs on the expected cores
        taskset_output = node.account.ssh_output(
            "taskset -cap $(pidof redpanda)"
        ).decode("utf-8")
        running_on_cores: set[int] = set()
        for thread_info in taskset_output.splitlines():
            core = thread_info.split(" ")[-1]
            running_on_cores.add(int(core))

        assert running_on_cores == expected_interrupt_setup.redpanda_cores, (
            f"Redpanda is not running on the expected cores, got {running_on_cores} expected {expected_interrupt_setup.redpanda_cores}"
        )

        # confirm that we also check cleanly
        check_output = rpk.check().split()

        for line in check_output:
            if line.startswith(f"NIC {self.interface_name}"):
                assert line.endswith("true"), f"NIC check failed: {line}"

    def _test_tune_net_mq(self, expected_interrupt_setup: ExpectedInterruptSetup):
        self.rpk.tune("net")

        self.start_rp()

        self._test_interrupt_config(self.node, self.rpk, expected_interrupt_setup)

    def _test_tune_net_dedicated_core(
        self,
        expected_interrupt_setup: ExpectedInterruptSetup,
        dedicated_cores: int,
        rps_rfs: bool = True,
    ):
        self.rpk.config_set(
            "rpk.cores_per_dedicated_interrupt_core", str(dedicated_cores)
        )
        self.rpk.config_set("rpk.allow_dedicated_interrupt_mode", "true")
        if not rps_rfs:
            self.rpk.config_set("rpk.allow_rps_rfs_tuner", "false")

        self.rpk.tune("net")

        self.start_rp()

        self._test_interrupt_config(self.node, self.rpk, expected_interrupt_setup)


# Targets CORE_COUNT core machines
class AwsNetTunerTest(NetTunerTest):
    @cluster(num_nodes=1)
    def test_tune_net_mq(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["1", "4", "2", "8"],
            redpanda_cores={0, 1, 2, 3},
            rps_cpu_mask="0",
            rps_cpu_flow_count=0,
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=4,
        )

        self._test_tune_net_mq(expected_interrupt_setup)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["8"],
            redpanda_cores={0, 1, 2},
            rps_cpu_mask="7",
            rps_cpu_flow_count=int(self.TARGET_RFS_TABLE_SIZE / 1),
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=1,
        )

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 4)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core_no_rps_rfs(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["8"],
            redpanda_cores={0, 1, 2},
            rps_cpu_mask="0",
            rps_cpu_flow_count=0,
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=1,
        )

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 4, False)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_2_cores(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["4", "8"],
            redpanda_cores={0, 1},
            rps_cpu_mask="3",
            rps_cpu_flow_count=int(self.TARGET_RFS_TABLE_SIZE / 2),
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=2,
        )

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 2)


# Targets CORE_COUNT core virtio (this is what our current ansible targets) machines
class GcpNetTunerTest(NetTunerTest):
    def get_interrupt_match(self) -> str:
        return "virtio1-(input|output)"

    def _test_irq_balance(self):
        # no irq-balance on GCP ubu images
        pass

    @cluster(num_nodes=1)
    def test_tune_net_mq(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["1", "1", "4", "4", "2", "2", "8", "8"],
            redpanda_cores={0, 1, 2, 3},
            rps_cpu_mask="0",
            rps_cpu_flow_count=0,
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=4,
        )

        self._test_tune_net_mq(expected_interrupt_setup)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["8", "8", "8", "8", "8", "8", "8", "8"],
            redpanda_cores={0, 1, 2},
            rps_cpu_mask="7",
            rps_cpu_flow_count=int(self.TARGET_RFS_TABLE_SIZE / 1),
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=1,
        )

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 4)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core_no_rps_rfs(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["8", "8", "8", "8", "8", "8", "8", "8"],
            redpanda_cores={0, 1, 2},
            rps_cpu_mask="0",
            rps_cpu_flow_count=0,
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=1,
        )

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 4, False)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_2_cores(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["4", "4", "8", "8", "4", "4", "8", "8"],
            redpanda_cores={0, 1},
            rps_cpu_mask="3",
            rps_cpu_flow_count=int(self.TARGET_RFS_TABLE_SIZE / 2),
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=2,
        )

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 2)
