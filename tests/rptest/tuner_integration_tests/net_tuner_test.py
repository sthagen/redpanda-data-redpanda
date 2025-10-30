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

CORE_COUNT = 4
NET_TUNER_CONFIG_FILE_PATH = "/var/run/redpanda_node_tuner_state.yaml"


class NetTunerTest(RedpandaTest):
    TARGET_RFS_TABLE_SIZE = 32768

    def __init__(self, ctx: TestContext):
        super().__init__(test_context=ctx)

    def interface_matcher(self, interface: str) -> bool:
        return False

    def setUp(self):
        # Skip starting redpanda, so that test can explicitly start it with rpk
        self.node = self.redpanda.nodes[0]
        self.redpanda.clean_node(self.node)
        self.rpk = RpkRemoteTool(self.redpanda, self.node)
        self.rpk.mode_set("production")

        self.interface_name = ""
        interfaces = (
            self.redpanda.nodes[0]
            .account.ssh_output("ls /sys/class/net")
            .decode("utf-8")
            .strip()
            .split("\n")
        )
        for interface in interfaces:
            if self.interface_matcher(interface):
                self.interface_name = interface
                break

        assert self.interface_name != "", (
            f"No interface matched - interfaces: {interfaces}"
        )

        self.logger.info(f"Found interface {self.interface_name}")

        uname = self.node.account.ssh_output("uname -m").decode("utf-8")
        self.is_arm = "aarch64" in uname

    def teardown(self):
        super().teardown()

        # Reset to what ansible gives you out of the box
        self.node.account.ssh("rm -rf /etc/redpanda/redpanda.yaml")
        self.node.account.ssh("rpk redpanda mode prod")
        self.node.account.ssh("systemctl restart redpanda-tuner")
        self.node.account.ssh(f"rm -rf {NET_TUNER_CONFIG_FILE_PATH}")

    def start_rp(self, additional_args: str = ""):
        # Need to explicitly pass listener config otherwise RP will complain about 0.0.0.0 listeners
        self.redpanda.start_node_with_rpk(
            self.node,
            additional_args=f"--rpc-addr={self.node.account.hostname} --kafka-addr=dnslistener://{self.node.account.hostname} {additional_args}",
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
        pattern = re.compile(matcher)
        irqs: list[tuple[int, str]] = []
        for line in interrupt_file.split("\n"):
            match = pattern.search(line)
            if match:
                id = int(line.split(":")[0])
                irqs.append((id, match.group()))

        return [id for id, _ in sorted(irqs, key=lambda x: x[1])]

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

    def _fix_ssh_connection(self):
        # RX/TX queue lowering can "break" the ssh connection on GCP. The
        # command will have succeeded anyway so such ignore and continue.
        # Close the ssh client to force a reconnect otherwise the next
        # command will timeout/fail again
        self.node.account.ssh_client.close()
        self.logger.debug(
            "Tuner command timed out (likely due to GCP connection dropping), continuing"
        )

    def _test_tune_net_mq(self, expected_interrupt_setup: ExpectedInterruptSetup):
        # Create a dummy file. This should be emptied by the tuner
        self.redpanda.nodes[0].account.ssh(f"echo 123 > {NET_TUNER_CONFIG_FILE_PATH}")
        self.rpk.tune("net")
        self.redpanda.nodes[0].account.ssh(f"test ! -s {NET_TUNER_CONFIG_FILE_PATH}")

        # rpk start should ignore the empty file
        self.start_rp()

        self._test_interrupt_config(self.node, self.rpk, expected_interrupt_setup)

    def _test_tune_net_dedicated_core(
        self,
        expected_interrupt_setup: ExpectedInterruptSetup,
        dedicated_cores: int,
        rps_rfs: bool = True,
        additional_tune_args: list[str] = [],
        additional_start_args: str = "",
    ):
        self.rpk.config_set(
            "rpk.cores_per_dedicated_interrupt_core", str(dedicated_cores)
        )

        if not rps_rfs:
            self.rpk.config_set("rpk.allow_rps_rfs_tuner", "false")

        try:
            self.rpk.tune(
                "net", ["--mode", "dedicated"] + additional_tune_args, timeout=2
            )
        except TimeoutError:
            self._fix_ssh_connection()

        self.start_rp(additional_args=additional_start_args)

        self._test_interrupt_config(self.node, self.rpk, expected_interrupt_setup)

    def _test_tune_net_dedicated_core_auto_detect(
        self,
        expected_interrupt_setup: ExpectedInterruptSetup,
        dedicated_cores: int,
    ):
        self.rpk.config_set(
            "rpk.cores_per_dedicated_interrupt_core", str(dedicated_cores)
        )

        self.rpk.config_set("rpk.allow_dedicated_interrupt_mode", "true")

        try:
            # In this test also confirm that the net tuner config file mode is set correctly
            # Tune with very restrictive umask
            self.node.account.ssh_output(
                "umask 077 && rpk redpanda tune net -v", timeout_sec=2
            )
        except TimeoutError:
            self._fix_ssh_connection()

        # And then confirm the redpanda user can read it
        self.node.account.ssh(
            f"sudo -u redpanda bash -c 'test -r {NET_TUNER_CONFIG_FILE_PATH}'"
        )

        self.start_rp()

        self._test_interrupt_config(self.node, self.rpk, expected_interrupt_setup)


# Targets CORE_COUNT core machines
class AwsNetTunerTest(NetTunerTest):
    def interface_matcher(self, interface: str) -> bool:
        return interface.startswith("ens")

    def get_basic_dedicated_expected(self) -> NetTunerTest.ExpectedInterruptSetup:
        return self.ExpectedInterruptSetup(
            interrupts_masks=["8"],
            redpanda_cores={0, 1, 2},
            rps_cpu_mask="7",
            rps_cpu_flow_count=int(self.TARGET_RFS_TABLE_SIZE / 1),
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=1,
        )

    @cluster(num_nodes=1)
    def test_tune_net_mq(self):
        if self.is_arm:
            self.start_rp()
            return

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
    def test_tune_net_dedicated_explicit_interfaces(self):
        # lo should be ignored
        self._test_tune_net_dedicated_core(
            self.get_basic_dedicated_expected(),
            4,
            additional_tune_args=["--nic", "lo,ens5"],
        )

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core(self):
        expected_interrupt_setup = self.get_basic_dedicated_expected()

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 4)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core_auto_detect(self):
        expected_interrupt_setup = self.get_basic_dedicated_expected()

        self._test_tune_net_dedicated_core_auto_detect(expected_interrupt_setup, 4)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_different_tuner_path(self):
        # if we leak this it's fine as nothing else uses this path
        alternative_path = "/tmp/redpanda_net_tuner_config_123"

        self._test_tune_net_dedicated_core(
            self.get_basic_dedicated_expected(),
            4,
            additional_tune_args=["--node-tuner-state-path", alternative_path],
            additional_start_args=f"--node-tuner-state-path={alternative_path}",
        )

        self.node.account.ssh(f"test -e {alternative_path}")
        self.node.account.ssh(f"rm -rf {alternative_path}")

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
        if self.is_arm:
            self.start_rp()
            return

        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["4", "8"],
            redpanda_cores={0, 1},
            rps_cpu_mask="3",
            rps_cpu_flow_count=int(self.TARGET_RFS_TABLE_SIZE / 2),
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=2,
        )

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 2)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core_extra_rpk_smp_4(self):
        expected_interrupt_setup = self.get_basic_dedicated_expected()

        self.node.account.ssh("rpk redpanda config set rpk.smp 4")

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 4)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core_extra_rpk_smp_3(self):
        expected_interrupt_setup = self.get_basic_dedicated_expected()

        self.node.account.ssh("rpk redpanda config set rpk.smp 3")

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 4)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core_extra_rpk_additional_args_smp_4(self):
        expected_interrupt_setup = self.get_basic_dedicated_expected()

        self.node.account.ssh(
            "rpk redpanda config set rpk.additional_start_flags '[\"--smp=4\"]'"
        )

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 4)


# Targets CORE_COUNT core virtio (this is what our current ansible targets) machines
class GcpNetTunerTest(NetTunerTest):
    def interface_matcher(self, interface: str) -> bool:
        return interface.startswith("ens")

    def get_interrupt_match(self) -> str:
        return "\\d+-edge\\s+virtio1-(input|output)"

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
    def test_tune_net_dedicated_1_core_auto_detect(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["8", "8", "8", "8", "8", "8", "8", "8"],
            redpanda_cores={0, 1, 2},
            rps_cpu_mask="7",
            rps_cpu_flow_count=int(self.TARGET_RFS_TABLE_SIZE / 1),
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=1,
        )

        self._test_tune_net_dedicated_core_auto_detect(expected_interrupt_setup, 4)

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


# Targets 8 core machines (Standard_L8s_v3)
class AzureNetTunerTest(NetTunerTest):
    def interface_matcher(self, interface: str) -> bool:
        return interface.startswith("enP")

    def get_interrupt_match(self) -> str:
        return "mlx5_comp\\d+"

    @cluster(num_nodes=1)
    def test_tune_net_mq(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["01", "02", "04", "08", "10", "20", "40", "80"],
            redpanda_cores={0, 1, 2, 3, 4, 5, 6, 7},
            rps_cpu_mask="00",
            rps_cpu_flow_count=0,
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=8,
        )

        self._test_tune_net_mq(expected_interrupt_setup)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["80", "80", "80", "80", "80", "80", "80", "80"],
            redpanda_cores={0, 1, 2, 3, 4, 5, 6},
            rps_cpu_mask="7f",
            rps_cpu_flow_count=int(self.TARGET_RFS_TABLE_SIZE / 1),
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=1,
        )

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 8)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core_no_rps_rfs(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["80", "80", "80", "80", "80", "80", "80", "80"],
            redpanda_cores={0, 1, 2, 3, 4, 5, 6},
            rps_cpu_mask="00",
            rps_cpu_flow_count=0,
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=1,
        )

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 8, False)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_2_cores(self):
        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["08", "80", "08", "08", "08", "80", "80", "80"],
            redpanda_cores={0, 1, 2, 4, 5, 6},
            rps_cpu_mask="77",
            rps_cpu_flow_count=int(self.TARGET_RFS_TABLE_SIZE / 2),
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
            rx_tx_queue_count=2,
        )

        self._test_tune_net_dedicated_core(expected_interrupt_setup, 4)
