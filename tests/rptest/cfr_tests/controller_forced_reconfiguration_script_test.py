# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""Ducktape tests that exercise the invoke_controller_reconfiguration CLI
script against a real cluster where the controller has lost quorum."""

import subprocess
from typing import Any

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.cfr_tests.cfr_test_base import (
    MEDIUM_TIMEOUT,
    REALLY_LONG_TIMEOUT,
    ControllerForcedReconfigurationTestBase,
    SimpleTLSProvider,
)
from rptest.clients.rpk import RpkTool
from rptest.services import tls as tls_mod
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService, SecurityConfig
from rptest.tests.redpanda_test import RedpandaTest

# Name of the script on PATH, installed via the script_package bazel rule.
CFR_SCRIPT = "invoke_controller_reconfiguration"

# Substring from the script's --help output used to verify correct packaging.
CFR_HELP_DESCRIPTION = (
    "Helper script to orchestrate invoking Controller Forced Reconfiguration"
)

# Interactive confirmation the script expects on stdin before proceeding.
CFR_CONFIRMATION_INPUT = "yes\n"

# Name of the TLS-enabled admin API listener defined in the redpanda.yaml
# template.  TLS certs are bound to this listener; the default listener on
# port 9644 remains plaintext.
ADMIN_TLS_LISTENER_NAME = "iplistener"

# Topic created after CFR to verify the controller is fully functional.
VALIDATION_TOPIC = "cfr-validation-topic"

# ── CFR script CLI flags ─────────────────────────────────────────────────
CFR_SUBCMD_BAREMETAL = "baremetal"
CFR_FLAG_NODES = "--nodes"
CFR_FLAG_DEAD_NODES = "--dead-nodes"
CFR_FLAG_SURVIVING_COUNT = "--surviving-count"
CFR_FLAG_CHECK_LIVENESS = "--check-liveness"
CFR_FLAG_PORT = "--port"
CFR_FLAG_USERNAME = "--username"
CFR_FLAG_PASSWORD = "--password"
CFR_FLAG_TLS_CA_CERT = "--tls-ca-cert"
CFR_FLAG_HELP = "--help"


class ControllerForcedReconfigurationScriptTest(RedpandaTest):
    """
    Validates that the invoke_controller_reconfiguration helper script
    is packaged correctly and accessible on PATH in the test environment.
    """

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(test_context=test_context, num_brokers=1)

    @cluster(num_nodes=1)
    def test_cfr_script_on_path(self) -> None:
        result = subprocess.run(
            [CFR_SCRIPT, CFR_FLAG_HELP],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"Script exited with {result.returncode}: {result.stderr}"
        )

        assert CFR_HELP_DESCRIPTION in result.stdout, (
            f"Expected description not found in help output:\n{result.stdout}"
        )

        assert "baremetal" in result.stdout, (
            f"Expected 'baremetal' subcommand not found in help output:\n{result.stdout}"
        )

        assert "kubernetes" in result.stdout, (
            f"Expected 'kubernetes' subcommand not found in help output:\n{result.stdout}"
        )


class ControllerForcedReconfigurationBasicTest(
    ControllerForcedReconfigurationTestBase,
):
    """Exercises the invoke_controller_reconfiguration CLI script against a
    real cluster where the controller has lost quorum.

    Inherits cluster lifecycle helpers from
    ControllerForcedReconfigurationTestBase and adds the script-specific
    invocation and auth/TLS wiring.
    """

    CLUSTER_SIZE = 3

    # Admin API TLS config applied to the alternate listener only
    # (server-side TLS, no mTLS).
    ADMIN_TLS_CONFIG: dict[str, Any] = dict(
        name=ADMIN_TLS_LISTENER_NAME,
        enabled=True,
        require_client_auth=False,
        key_file=RedpandaService.TLS_SERVER_KEY_FILE,
        cert_file=RedpandaService.TLS_SERVER_CRT_FILE,
        truststore_file=RedpandaService.TLS_CA_CRT_FILE,
    )

    def __init__(self, test_context: TestContext) -> None:
        security = SecurityConfig()
        security.enable_sasl = True
        security.http_authentication = ["BASIC"]
        security.endpoint_authn_method = "sasl"
        security.require_client_auth = False

        self._tls_manager = tls_mod.TLSCertManager(test_context.logger)
        self._tls_provider = SimpleTLSProvider(self._tls_manager)

        super().__init__(
            test_context,
            cluster_size=self.CLUSTER_SIZE,
            security=security,
        )

        self._superuser = self.redpanda.SUPERUSER_CREDENTIALS

    # ── helpers ───────────────────────────────────────────────────────────

    def _start_redpanda(
        self,
        is_auth_enabled: bool,
        is_tls_enabled: bool,
    ) -> None:
        """Bootstrap the cluster with the requested security posture.

        Args:
            is_auth_enabled: require HTTP basic-auth on the admin API.
            is_tls_enabled: enable TLS on the alternate admin API listener.
        """
        self.redpanda.add_extra_rp_conf(
            {"internal_topic_replication_factor": self.cluster_size}
        )

        if is_tls_enabled:
            # sets up tls on all nodes, we need init_tls because
            # the membership check on startup now needs to be tls gnostic
            self.redpanda._security.tls_provider = self._tls_provider
            self.redpanda._init_tls()
            for node in self.redpanda.nodes:
                self.redpanda.set_extra_node_conf(
                    node, dict(admin_api_tls=self.ADMIN_TLS_CONFIG)
                )

        self.redpanda.start()

        if is_auth_enabled:
            self.redpanda.set_cluster_config({"admin_api_require_auth": True})

    def _dead_nodes_detected(
        self,
        survivor: ClusterNode,
        expected_dead_count: int,
    ) -> bool:
        """True once the survivor's /v1/brokers reports at least
        ``expected_dead_count`` brokers as not alive."""
        admin = self.redpanda._admin
        try:
            brokers = admin.get_brokers(node=survivor)
            dead = [b for b in brokers if not b.get("is_alive", True)]
            return len(dead) >= expected_dead_count
        except Exception:
            return False

    def _common_auth_tls_args(
        self,
        is_auth_enabled: bool,
        is_tls_enabled: bool,
    ) -> list[str]:
        """Build the --username/--password and --port/--tls-ca-cert CLI
        flags shared by both manual and liveness modes."""
        args: list[str] = []
        if is_auth_enabled:
            args += [
                CFR_FLAG_USERNAME,
                self._superuser.username,
                CFR_FLAG_PASSWORD,
                self._superuser.password,
            ]
        if is_tls_enabled:
            # TLS is on the alternate admin listener, not port 9644.
            args += [
                CFR_FLAG_PORT,
                str(RedpandaService.ADMIN_ALTERNATE_PORT),
                CFR_FLAG_TLS_CA_CERT,
                self._tls_manager.ca.crt,
            ]
        return args

    def _kill_majority_and_enter_recovery(
        self,
    ) -> tuple[list[int], list[ClusterNode]]:
        """Stop a majority of nodes and reboot the survivors into recovery
        mode.

        Returns:
            (killed_ids, survivors): the node-ids that were killed and the
            ClusterNode handles of the survivors now in recovery mode.
        """
        nodes_to_kill = self.redpanda.nodes[: self.majority_to_kill]
        survivors = self.redpanda.nodes[self.majority_to_kill :]

        killed_ids = [self.redpanda.node_id(n) for n in nodes_to_kill]
        survivor_ids = [self.redpanda.node_id(n) for n in survivors]
        self.logger.info(f"Killing nodes {killed_ids}, survivors: {survivor_ids}")

        for node in nodes_to_kill:
            self.redpanda.stop_node(node)

        self._bulk_toggle_recovery_mode(
            nodes=survivors,
            timeout=MEDIUM_TIMEOUT,
            recovery_mode_enabled=True,
        )

        return killed_ids, survivors

    def _run_cfr_script(self, cmd: list[str]) -> None:
        """Execute the CFR script, log its output, and assert exit code 0."""
        self.logger.info(f"Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            input=CFR_CONFIRMATION_INPUT,
            capture_output=True,
            text=True,
            timeout=MEDIUM_TIMEOUT.timeout_s,
        )
        self.logger.info(f"CFR stdout:\n{result.stdout}")
        if result.stderr:
            self.logger.info(f"CFR stderr:\n{result.stderr}")

        assert result.returncode == 0, (
            f"CFR script failed (rc={result.returncode}):\n{result.stdout}\n{result.stderr}"
        )

    def _verify_recovery(
        self,
        killed_ids: list[int],
        survivors: list[ClusterNode],
    ) -> None:
        """Wait for the controller to come back, exit recovery mode on all
        survivors, then create a topic to prove the controller is fully
        operational."""
        surviving_count = len(survivors)

        wait_until(
            lambda: self._controller_recovered(killed_ids),
            timeout_sec=REALLY_LONG_TIMEOUT.timeout_s,
            backoff_sec=REALLY_LONG_TIMEOUT.backoff_s,
            err_msg="Controller did not recover after CFR",
        )

        self._bulk_toggle_recovery_mode(
            nodes=survivors,
            timeout=MEDIUM_TIMEOUT,
            recovery_mode_enabled=False,
        )

        wait_until(
            lambda: self._controller_recovered(killed_ids),
            timeout_sec=MEDIUM_TIMEOUT.timeout_s,
            backoff_sec=MEDIUM_TIMEOUT.backoff_s,
            err_msg="Controller did not recover after exiting recovery mode",
        )

        # Pass the service's internal TLS client cert (None when TLS is off)
        # along with explicit SASL credentials.  Both are needed because
        # RpkTool treats tls_cert-without-SASL as mTLS-only auth.
        rpk = RpkTool(
            self.redpanda,
            tls_cert=self.redpanda._tls_cert,
            username=self._superuser.username,
            password=self._superuser.password,
            sasl_mechanism=self._superuser.algorithm,
        )
        rpk.create_topic(
            VALIDATION_TOPIC,
            partitions=1,
            replicas=surviving_count,
        )
        assert VALIDATION_TOPIC in rpk.list_topics()

    # ── tests ────────────────────────────────────────────────────────────

    @cluster(num_nodes=CLUSTER_SIZE)
    @matrix(authn=[False, True], tls=[False, True])
    def test_cfr_script_manual_mode(self, authn: bool, tls: bool) -> None:
        """Run the CFR script with explicit --dead-nodes and
        --surviving-count.

        Args:
            authn: whether admin API basic-auth is required.
            tls: whether admin API TLS is enabled on the alternate listener.
        """
        is_auth_enabled = authn
        is_tls_enabled = tls

        self._start_redpanda(
            is_auth_enabled=is_auth_enabled,
            is_tls_enabled=is_tls_enabled,
        )
        killed_ids, survivors = self._kill_majority_and_enter_recovery()
        surviving_ips: list[str] = []
        for n in survivors:
            assert n.account.hostname is not None
            surviving_ips.append(n.account.hostname)
        surviving_count = len(survivors)

        cmd = [
            CFR_SCRIPT,
            CFR_SUBCMD_BAREMETAL,
            CFR_FLAG_NODES,
            *surviving_ips,
            CFR_FLAG_DEAD_NODES,
            *[str(i) for i in killed_ids],
            CFR_FLAG_SURVIVING_COUNT,
            str(surviving_count),
            *self._common_auth_tls_args(
                is_auth_enabled=is_auth_enabled,
                is_tls_enabled=is_tls_enabled,
            ),
        ]

        self._run_cfr_script(cmd)
        self._verify_recovery(killed_ids, survivors)

    @cluster(num_nodes=CLUSTER_SIZE)
    @matrix(authn=[False, True], tls=[False, True])
    def test_cfr_script_liveness_check(self, authn: bool, tls: bool) -> None:
        """Run the CFR script with --check-liveness.  The script queries
        /v1/brokers on every supplied IP to auto-detect which nodes are
        dead and derives the CFR parameters on its own.

        Args:
            authn: whether admin API basic-auth is required.
            tls: whether admin API TLS is enabled on the alternate listener.
        """
        is_auth_enabled = authn
        is_tls_enabled = tls

        self._start_redpanda(
            is_auth_enabled=is_auth_enabled,
            is_tls_enabled=is_tls_enabled,
        )
        killed_ids, survivors = self._kill_majority_and_enter_recovery()

        # The liveness check queries /v1/brokers to classify nodes.
        # Wait until every survivor agrees the killed nodes are dead.
        for survivor in survivors:
            wait_until(
                lambda: self._dead_nodes_detected(
                    survivor=survivor,
                    expected_dead_count=len(killed_ids),
                ),
                timeout_sec=MEDIUM_TIMEOUT.timeout_s,
                backoff_sec=MEDIUM_TIMEOUT.backoff_s,
                err_msg="Surviving node did not detect dead brokers in time",
            )

        all_ips: list[str] = []
        for n in self.redpanda.nodes:
            assert n.account.hostname is not None
            all_ips.append(n.account.hostname)
        cmd = [
            CFR_SCRIPT,
            CFR_SUBCMD_BAREMETAL,
            CFR_FLAG_NODES,
            *all_ips,
            CFR_FLAG_CHECK_LIVENESS,
            *self._common_auth_tls_args(
                is_auth_enabled=is_auth_enabled,
                is_tls_enabled=is_tls_enabled,
            ),
        ]

        self._run_cfr_script(cmd)
        self._verify_recovery(killed_ids, survivors)
