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

# only works with --max-parallel 1


# TODO: generalize, only works on 4 core AWS for now.
class NetTunerTest(RedpandaTest):
    def __init__(self, ctx: TestContext):
        super().__init__(test_context=ctx)

    def setUp(self):
        # Skip starting redpanda, so that test can explicitly start it with rpk
        self.node = self.redpanda.nodes[0]
        self.redpanda.clean_node(self.node)
        self.rpk = RpkRemoteTool(self.redpanda, self.node)
        self.rpk.mode_set("production")

    def start_rp(self):
        # Need to explicitly pass listener config otherwise RP will complain about 0.0.0.0 listeners
        self.redpanda.start_node_with_rpk(
            self.node,
            additional_args=f"--rpc-addr={self.node.account.hostname} --kafka-addr=dnslistener://{self.node.account.hostname}",
            clean_node=False,
        )

    @staticmethod
    def parse_matching_interrupt_num_from_interrupt_file(
        interrupt_file: str, matcher: str
    ) -> list[int]:
        ret: list[int] = []
        for line in interrupt_file.split("\n"):
            if matcher in line:
                ret.append(int(line.split(":")[0]))

        return ret

    TARGET_RFS_TABLE_SIZE = 32768

    @dataclasses.dataclass
    class ExpectedInterruptSetup:
        interrupts_masks: list[str]
        redpanda_cores: set[int]
        rps_cpu_mask: str
        rps_cpu_flow_count: int
        rfs_table_size: int

    def _test_interrupt_config(
        self,
        node: ClusterNode,
        rpk: RpkRemoteTool,
        expected_interrupt_setup: ExpectedInterruptSetup,
    ):
        # test irqbalance
        irqbalance_output = node.account.ssh_output(
            "systemctl status irqbalance"
        ).decode("utf-8")
        assert "--banirq" in irqbalance_output, (
            f"irqbalance is not configured correctly, got {irqbalance_output}"
        )

        # test interrupts
        interrupts_file = node.account.ssh_output("cat /proc/interrupts").decode(
            "utf-8"
        )
        interrupt_ids = self.parse_matching_interrupt_num_from_interrupt_file(
            interrupts_file, "ens5"
        )

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

        # test RPS
        for i in range(len(interrupt_ids)):
            rps_cpu = (
                node.account.ssh_output(
                    f"cat /sys/class/net/ens5/queues/rx-{i}/rps_cpus"
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

        for i in range(len(interrupt_ids)):
            per_queue_rps_flow_count = int(
                node.account.ssh_output(
                    f"cat /sys/class/net/ens5/queues/rx-{i}/rps_flow_cnt"
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
            if line.startswith("NIC ens5"):
                assert line.endswith("true"), f"NIC check failed: {line}"

    @cluster(num_nodes=1)
    def test_tune_net_mq(self):
        self.rpk.tune("net")

        self.start_rp()

        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["1", "4", "2", "8"],
            redpanda_cores={0, 1, 2, 3},
            rps_cpu_mask="0",
            rps_cpu_flow_count=0,
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
        )

        self._test_interrupt_config(self.node, self.rpk, expected_interrupt_setup)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core(self):
        self.rpk.config_set("rpk.cores_per_dedicated_interrupt_core", "4")
        self.rpk.config_set("rpk.allow_dedicated_interrupt_mode", "true")

        self.rpk.tune("net")

        self.start_rp()

        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["8"],
            redpanda_cores={0, 1, 2},
            rps_cpu_mask="7",
            rps_cpu_flow_count=int(self.TARGET_RFS_TABLE_SIZE / 1),
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
        )

        self._test_interrupt_config(self.node, self.rpk, expected_interrupt_setup)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_1_core_no_rps_rfs(self):
        self.rpk.config_set("rpk.cores_per_dedicated_interrupt_core", "4")
        self.rpk.config_set("rpk.allow_dedicated_interrupt_mode", "true")
        self.rpk.config_set("rpk.allow_rps_rfs_tuner", "false")

        self.rpk.tune("net")

        self.start_rp()

        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["8"],
            redpanda_cores={0, 1, 2},
            rps_cpu_mask="0",
            rps_cpu_flow_count=0,
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
        )

        self._test_interrupt_config(self.node, self.rpk, expected_interrupt_setup)

    @cluster(num_nodes=1)
    def test_tune_net_dedicated_2_cores(self):
        self.rpk.config_set("rpk.cores_per_dedicated_interrupt_core", "2")
        self.rpk.config_set("rpk.allow_dedicated_interrupt_mode", "true")

        self.rpk.tune("net")

        self.start_rp()

        expected_interrupt_setup = self.ExpectedInterruptSetup(
            interrupts_masks=["4", "8"],
            redpanda_cores={0, 1},
            rps_cpu_mask="3",
            rps_cpu_flow_count=int(self.TARGET_RFS_TABLE_SIZE / 2),
            rfs_table_size=self.TARGET_RFS_TABLE_SIZE,
        )

        self._test_interrupt_config(self.node, self.rpk, expected_interrupt_setup)
