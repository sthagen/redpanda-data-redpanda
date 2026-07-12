# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

from connectrpc.errors import ConnectError, ConnectErrorCode
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import shadow_link_pb2
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import TestContext, cluster
from rptest.services.multi_cluster_services import SecondaryClusterArgs
from rptest.services.redpanda import SchemaRegistryConfig
from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase
from rptest.util import expect_exception

_SCHEMA = (
    '{"type":"record","name":"preflight_record",'
    '"fields":[{"name":"f1","type":"string"}]}'
)


class ShadowLinkSchemaRegistryPreflightTest(ShadowLinkTestBase):
    """End-to-end coverage for the Schema Registry API-sync
    preflight checks that run on shadow-link creation (including the
    validate_only pre-flight): source Schema Registry reachability and target
    context emptiness. Both clusters run Schema Registry so the source is a real
    reachable endpoint and the target registry is queryable."""

    LINK_NAME = "sr-preflight-link"

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            secondary_cluster_args=SecondaryClusterArgs(
                schema_registry_config=SchemaRegistryConfig()
            ),
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    def _source_sr_url(self) -> str:
        return self.source_cluster_service.schema_reg(limit=1)

    def _validate_request(
        self,
        source_url: str,
        destination: shadow_link_pb2.SchemaRegistryContextDestination | None = None,
        validate_only: bool = True,
    ) -> shadow_link_pb2.CreateShadowLinkRequest:
        req = self.create_default_link_request(
            link_name=self.LINK_NAME,
            mirror_all_acls=False,
            mirror_all_groups=False,
            mirror_all_topics=False,
        )
        api = shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryApi(
            source_url=source_url
        )
        if destination is not None:
            api.destination.CopyFrom(destination)
        req.shadow_link.configurations.schema_registry_sync_options.shadow_schema_registry_api.CopyFrom(
            api
        )
        # validate_only runs the preflight without creating the link; when False
        # the same checks gate a real create.
        req.validate_only = validate_only
        return req

    def _seed_subject(self, rpk: RpkTool, subject: str) -> None:
        rpk.create_schema_from_str(subject, _SCHEMA)

    def setUp(self):
        super().setUp()
        # The shadow_link_sr_api_sync feature auto-activates once every broker
        # is at least v26.2, which holds for this cluster, so no manual
        # activation is needed for the request to reach the preflight checks.
        #
        # Wait for the shadow-link admin service to be ready so the first
        # create/validate request does not race a still-initializing controller
        # and return a transient 'unavailable'.
        wait_until(
            lambda: self.list_links() is not None,
            timeout_sec=30,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="shadow link admin service did not become ready",
        )

    @cluster(num_nodes=6)  # 3 target + 3 source brokers
    def test_validate_reachable_empty_target_ok(self):
        """Source reachable and the (identity-mapped) target context empty:
        validation succeeds."""
        req = self._validate_request(self._source_sr_url())
        # Raises ConnectError on failure; success returns normally.
        self.create_link_with_request(req=req)

        # validate_only must not have created the link.
        assert self.LINK_NAME not in [link.name for link in self.list_links()], (
            "validate_only unexpectedly created the link"
        )

    @cluster(num_nodes=6)
    def test_validate_source_unreachable(self):
        """An unreachable source Schema Registry URL fails preflight."""
        req = self._validate_request("http://schema-registry.invalid:8081")
        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.FAILED_PRECONDITION
            and "source schema registry" in str(e),
        ):
            self.create_link_with_request(req=req)

    @cluster(num_nodes=6)
    def test_validate_target_context_not_empty(self):
        """A populated target context fails preflight. Emptiness is checked
        against the target registry, so a subject present only on the target
        (the source is reachable but empty) is enough to trip the check."""
        self._seed_subject(self.target_cluster_rpk, "target-only-value")

        req = self._validate_request(self._source_sr_url())
        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.FAILED_PRECONDITION
            and "context(s) not empty" in str(e),
        ):
            self.create_link_with_request(req=req)

    @cluster(num_nodes=6)
    def test_create_blocked_when_target_context_not_empty(self):
        """The same preflight gates a real create (validate_only=False), not
        just the dry run: a populated target context must reject the create and
        leave no link behind."""
        self._seed_subject(self.target_cluster_rpk, "target-only-value")

        req = self._validate_request(self._source_sr_url(), validate_only=False)
        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.FAILED_PRECONDITION
            and "context(s) not empty" in str(e),
        ):
            self.create_link_with_request(req=req)

        # A rejected create must not leave a partially-created link behind.
        assert self.LINK_NAME not in [link.name for link in self.list_links()], (
            "preflight-rejected create unexpectedly left the link behind"
        )

    @cluster(num_nodes=6)
    def test_create_succeeds_when_reachable_and_target_empty(self):
        """The positive real-create path: with a reachable source and an empty
        target context, a create (validate_only=False) passes preflight and the
        link is actually persisted. Guards against preflight over-blocking a
        valid SR-API link on the create path."""
        req = self._validate_request(self._source_sr_url(), validate_only=False)
        # Raises ConnectError on failure; success returns normally.
        self.create_link_with_request(req=req)

        assert self.LINK_NAME in [link.name for link in self.list_links()], (
            "create passed preflight but the link was not persisted"
        )

    @cluster(num_nodes=6)
    def test_validate_exact_mapping_checks_destination_context(self):
        """Exact mapping keys emptiness off the destination context: a subject
        in the target's default context fails when it is a mapping
        destination."""
        self._seed_subject(self.target_cluster_rpk, "dest-value")

        destination = shadow_link_pb2.SchemaRegistryContextDestination(
            exact=shadow_link_pb2.SchemaRegistryExactContextMappings(
                mappings=[
                    shadow_link_pb2.SchemaRegistryContextMap(
                        source=".source-ctx", destination="."
                    )
                ]
            )
        )
        req = self._validate_request(self._source_sr_url(), destination=destination)
        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.FAILED_PRECONDITION
            and "context(s) not empty" in str(e),
        ):
            self.create_link_with_request(req=req)
