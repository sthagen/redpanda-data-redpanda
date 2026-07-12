# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Random node operations test for Antithesis.
#
# Subclasses RandomNodeOperationsBase to exercise admin ops fuzzing,
# kgo-verifier producer/consumer pairs, and node add/decommission
# operations. Parameterized to run with and without AT fault injection.

from __future__ import annotations

from typing import Any

from ducktape.tests.test import TestContext
from ducktape.mark import matrix

from rptest.services.apache_iceberg_catalog import IcebergRESTCatalog
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    LoggingConfig,
    PandaproxyConfig,
    SchemaRegistryConfig,
    SISettings,
    get_cloud_storage_type,
)
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.random_node_operations_smoke_test import (
    RandomNodeOperationsBase,
    CompactionMode,
    RNOT_ALLOW_LIST,
)

from rptest.antithesis.antithesis_utils import (
    AntithesisTimeoutMixin,
    always,
    reachable,
    retry_call,
)


class AntithesisRandomNodeOpsTest(AntithesisTimeoutMixin, RandomNodeOperationsBase):
    """Random node operations test for Antithesis.

    Exercises the full RandomNodeOperations workload: admin ops
    fuzzing, kgo-verifier producer/consumer pairs on multiple topics
    (delete, compact, fast-move), and node add/decommission.

    Skips RandomNodeOperationsBase.__init__ because it queries
    GitHub for prior release versions (AT has no internet) and
    we don't run mixed-version tests.
    """

    def __init__(self, test_context: TestContext) -> None:
        PreallocNodesTest.__init__(
            self,
            test_context=test_context,
            num_brokers=5,
            extra_rp_conf={
                "default_topic_replications": 3,
                "raft_learner_recovery_rate": 512 * (1024 * 1024),
                "partition_autobalancing_mode": "node_add",
                "raft_io_timeout_ms": 20000,
                "compacted_log_segment_size": 1024 * 1024,
                "log_segment_size": 2 * 1024 * 1024,
                "retention_local_trim_interval": 5000,
                "storage_min_free_bytes": 10000000,
            },
            node_prealloc_count=3,
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            log_config=LoggingConfig("info"),
        )

        self.admin_fuzz = None
        self.should_skip = False
        self.is_smoke_test = True
        self.nodes_with_prev_version = []
        self.previous_version = None
        self.installer = self.redpanda._installer
        self._si_settings = SISettings(
            self.test_context,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True,
            fast_uploads=True,
        )
        self.catalog_service = IcebergRESTCatalog(
            test_context,
            cloud_storage_bucket=self._si_settings.cloud_storage_bucket,
            filesystem_wrapper_mode=False,
        )

    def setUp(self) -> None:
        super().setUp()

        # Monkey-patch DefaultClient and RpkTool so topic create/alter
        # in the base class retry under AT fault injection
        # (NOT_CONTROLLER, REQUEST_TIMED_OUT). Originals saved on the
        # instance and restored in tearDown so subsequent tests in the
        # same ducktape worker see the unpatched class methods.
        from rptest.clients.default import DefaultClient
        from rptest.clients.rpk import RpkTool

        self._patched_methods = [
            (DefaultClient, "create_topic", DefaultClient.create_topic),
            (DefaultClient, "alter_topic_config", DefaultClient.alter_topic_config),
            (RpkTool, "create_topic", RpkTool.create_topic),
            (RpkTool, "alter_topic_config", RpkTool.alter_topic_config),
        ]
        for cls, name, orig in self._patched_methods:
            setattr(
                cls,
                name,
                retry_call(
                    orig,
                    attempts=5,
                    sleep_sec=5.0,
                    label=f"{cls.__name__}.{name}",
                ),
            )

    def tearDown(self) -> None:
        for cls, name, orig in getattr(self, "_patched_methods", ()):
            setattr(cls, name, orig)
        super().tearDown()

    @cluster(num_nodes=9, log_allow_list=RNOT_ALLOW_LIST)
    @matrix(
        enable_failures=[True, False],
        with_iceberg=[False],
        compaction_mode=[
            CompactionMode.SLIDING_WINDOW,
            CompactionMode.ADJACENT_MERGE,
        ],
        cloud_storage_type=get_cloud_storage_type()[:1],
    )
    def test_random_node_ops(self, **kwargs: Any) -> None:
        self._do_test_node_operations(
            mixed_versions=False,
            **kwargs,
        )

        executed = self.admin_fuzz.executed if self.admin_fuzz else 0
        error = self.admin_fuzz.error if self.admin_fuzz else None

        always(
            error is None,
            "No errors during random admin operations",
            {"executed": executed, "error": str(error)},
        )

        reachable("Random node ops test completed", {"executed": executed})
