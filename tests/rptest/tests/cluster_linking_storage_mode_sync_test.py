# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from rptest.tests.cluster_linking_test_base import (
    SecondaryClusterArgs,
    ShadowLinkTestBase,
)


class ShadowLinkStorageModeSyncTest(ShadowLinkTestBase):
    """
    Verify that the storage mode of a source topic survives the shadow-link
    property sync even when the two clusters disagree on what the 'tiered'
    alias means (different default_redpanda_storage_mode_tiered_impl values).

    The sync transports the describe output, where both tiered variants
    display as 'tiered'; the read-only redpanda.storage.mode.impl
    property carries the variant, and the target must reconstruct the
    source topic's exact variant from the pair rather than re-interpreting
    'tiered' through its own cluster config.
    """

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )

        injected = test_context.injected_args or {}
        source_default = injected.get("source_default", "tiered_v2")
        target_default = injected.get("target_default", "tiered_v1")

        super().__init__(
            test_context,
            si_settings=si_settings,
            extra_rp_conf={
                "enable_cluster_metadata_upload_loop": False,
                "default_redpanda_storage_mode_tiered_impl": target_default,
            },
            secondary_cluster_args=SecondaryClusterArgs(
                si_settings=si_settings,
                extra_rp_conf={
                    "enable_shadow_linking": True,
                    "enable_cluster_metadata_upload_loop": False,
                    "default_redpanda_storage_mode_tiered_impl": source_default,
                },
            ),
            *args,
            **kwargs,
        )

    def _create_source_topic(self, name: str, config: dict[str, str]):
        source_rpk = RpkTool(self.source_cluster.service)

        def try_create():
            try:
                source_rpk.create_topic(
                    topic=name, partitions=1, replicas=1, config=config
                )
                return True
            except Exception as e:
                if "INVALID_CONFIG" in str(e):
                    return False
                raise

        # Retry topic creation: feature flag propagation may lag behind
        # the admin API response on some nodes.
        wait_until(
            try_create,
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"Failed to create source topic {name}",
        )

    def _target_storage_mode(self, topic: str) -> tuple[str | None, str | None]:
        try:
            configs = RpkTool(self.target_cluster.service).describe_topic_configs(topic)
        except RpkException as e:
            # The mirror topic may not exist (yet) or its metadata may not
            # have propagated to the broker rpk picked.
            self.logger.debug(f"describe {topic} failed: {e}")
            return (None, None)
        mode = configs.get(TopicSpec.PROPERTY_STORAGE_MODE)
        version = configs.get(TopicSpec.PROPERTY_STORAGE_MODE_IMPL)
        return (
            mode[0] if mode is not None else None,
            version[0] if version is not None else None,
        )

    @cluster(num_nodes=6)
    @matrix(
        source_default=["tiered_v1", "tiered_v2"],
        target_default=["tiered_v1", "tiered_v2"],
    )
    def test_storage_mode_sync_across_defaults(
        self, source_default: str, target_default: str
    ):
        self.source_cluster_service.set_feature_active(
            "tiered_cloud_topics", True, timeout_sec=30
        )
        self.target_cluster.service.set_feature_active(
            "tiered_cloud_topics", True, timeout_sec=30
        )

        # topic -> (create config, expected impl on the target); the impl
        # property is always present and unambiguous
        cases: dict[str, tuple[dict[str, str], str]] = {
            "sync-cloud": (
                {TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD},
                TopicSpec.STORAGE_MODE_CLOUD,
            ),
            # The alias resolves on the SOURCE at creation; the synced topic
            # must land on the source's variant, not the target's.
            "sync-tiered-alias": (
                {TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED},
                source_default,
            ),
            "sync-tiered-v1": (
                TopicSpec.storage_mode_config(TopicSpec.STORAGE_MODE_IMPL_TIERED_V1),
                TopicSpec.STORAGE_MODE_IMPL_TIERED_V1,
            ),
            "sync-tiered-v2": (
                TopicSpec.storage_mode_config(TopicSpec.STORAGE_MODE_IMPL_TIERED_V2),
                TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
            ),
        }

        for name, (config, _) in cases.items():
            self._create_source_topic(name, config)

        self.create_link("storage-mode-sync-link")

        for name, (_, expected_impl) in cases.items():
            expected_mode = (
                TopicSpec.STORAGE_MODE_TIERED
                if expected_impl
                in (
                    TopicSpec.STORAGE_MODE_IMPL_TIERED_V1,
                    TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
                )
                else expected_impl
            )

            def synced(
                name: str = name,
                expected_mode: str = expected_mode,
                expected_version: str | None = expected_impl,
            ) -> bool:
                if not self.topic_exists_in_target(topic=name, partition_count=1):
                    return False
                mode, version = self._target_storage_mode(name)
                return mode == expected_mode and version == expected_version

            self.target_cluster.service.wait_until(
                synced,
                timeout_sec=60,
                backoff_sec=2,
                err_msg=(
                    f"{name}: target did not reach mode={expected_mode} "
                    f"impl={expected_impl}"
                ),
            )

        # Update path: altering the source topic's mode must propagate the
        # variant through the (mode, version) pair as well. The conversion
        # goes through the 'tiered' alias, which on the source only reaches
        # the tiered_v2 variant (cloud -> tiered_v1 is not a permitted
        # transition), so this phase only applies when the source alias
        # points at tiered_v2.
        if source_default == "tiered_v2":
            RpkTool(self.source_cluster.service).alter_topic_config(
                "sync-cloud",
                TopicSpec.PROPERTY_STORAGE_MODE,
                TopicSpec.STORAGE_MODE_TIERED,
            )

            def converted() -> bool:
                mode, version = self._target_storage_mode("sync-cloud")
                return (
                    mode == TopicSpec.STORAGE_MODE_TIERED
                    and version == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2
                )

            self.target_cluster.service.wait_until(
                converted,
                timeout_sec=60,
                backoff_sec=2,
                err_msg=(
                    "sync-cloud: cloud -> tiered_v2 conversion did not "
                    "propagate to the target"
                ),
            )
