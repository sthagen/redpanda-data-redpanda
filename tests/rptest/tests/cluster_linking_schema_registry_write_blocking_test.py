# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.admin.proto.redpanda.core.admin.v2 import shadow_link_pb2
from rptest.services.cluster import cluster
from rptest.tests.cluster_linking_topic_syncing_test import (
    ClusterLinkingSchemaRegistryBase,
)
from rptest.tests.schema_registry_test import SchemaRegistryRedpandaClient

from ducktape.utils.util import wait_until

import google.protobuf.field_mask_pb2
import json


class ClusterLinkingSchemaRegistryWriteBlocking(ClusterLinkingSchemaRegistryBase):
    """
    Verify granular Schema Registry client-write blocking under API-mode
    shadowing: only the contexts selected by source_filter are write-protected
    on the target, while writes to out-of-scope contexts still succeed.
    """

    def post_schema_to_context(
        self,
        sr_client: SchemaRegistryRedpandaClient,
        context: str,
        subject: str,
        schema: str,
    ):
        """Post a schema to a context-scoped subject, returning the raw
        response so the caller can assert on the status code."""
        result_raw = sr_client.post_subjects_subject_versions(
            subject=subject,
            data=json.dumps({"schema": schema, "schemaType": "PROTOBUF"}),
            base_path=f"contexts/{context}",
        )
        self.logger.debug(
            f"post_schema_to_context {context}/{subject} result: {result_raw}"
        )
        return result_raw

    def wait_for_context_write_blocked(
        self,
        sr_client: SchemaRegistryRedpandaClient,
        context: str,
        schema: str,
    ):
        """Poll client writes to `context` until one is blocked (HTTP 412).

        Write protection activates asynchronously: update_link returns once the
        controller accepts the update, but the Schema Registry serving node
        applies the new link metadata on its own schedule. Each attempt uses a
        fresh subject because registering a subject+schema that already exists
        short-circuits in the handler and returns 200 without ever consulting
        the write-protection check -- so reusing one subject could register
        before the block activates and then never observe it."""
        attempt = 0

        def blocked() -> bool:
            nonlocal attempt
            attempt += 1
            return (
                self.post_schema_to_context(
                    sr_client, context, f"probe-{attempt}-value", schema
                ).status_code
                == 412
            )

        wait_until(
            blocked,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"client write to context '{context}' never became blocked (412)",
        )

    def wait_for_context_write_allowed(
        self,
        sr_client: SchemaRegistryRedpandaClient,
        context: str,
        schema: str,
    ):
        """Poll client writes to `context` until one succeeds (HTTP 200).

        The mirror image of wait_for_context_write_blocked: after a failover or
        a manual pause, write protection is lifted asynchronously as the serving
        node applies the new link metadata. A fresh subject per attempt avoids
        the already-registered short-circuit."""
        attempt = 0

        def allowed() -> bool:
            nonlocal attempt
            attempt += 1
            return (
                self.post_schema_to_context(
                    sr_client, context, f"unblock-{attempt}-value", schema
                ).status_code
                == 200
            )

        wait_until(
            allowed,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"client write to context '{context}' never became allowed (200)",
        )

    def create_prod_api_link(self) -> shadow_link_pb2.ShadowLink:
        """Create a link that shadows only the ".prod" context over the Schema
        Registry API with identity context mapping, and return the updated
        link. The owned ".prod" context becomes write-protected on the target."""
        created_link = self.create_link("test-link")
        api = created_link.configurations.schema_registry_sync_options.shadow_schema_registry_api
        api.source_url = "http://schema-registry.example.com:8081"
        api.source_filter.contexts.append(".prod")
        api.destination.identity.SetInParent()
        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=["configurations.schema_registry_sync_options"]
        )
        updated_link = self.update_link(created_link, update_mask)
        assert list(
            updated_link.configurations.schema_registry_sync_options.shadow_schema_registry_api.source_filter.contexts
        ) == [".prod"], "source_filter not applied after update"
        return updated_link

    @cluster(num_nodes=6)
    def test_schema_registry_api_manual_pause_toggles_blocking(self):
        """
        `paused` is user-settable via update_shadow_link, independent of
        failover: setting it lifts the write protection on owned contexts, and
        clearing it restores it.
        """
        target_sr_client = self.target_sr_client()
        assert len(self.get_subjects(target_sr_client)) == 0, (
            "Expected no subjects on the target before shadowing"
        )

        link = self.create_prod_api_link()
        self.wait_for_context_write_blocked(
            target_sr_client, ".prod", self.simple_proto_def
        )

        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=["configurations.schema_registry_sync_options"]
        )

        # Pausing lifts the block on ".prod".
        link.configurations.schema_registry_sync_options.shadow_schema_registry_api.paused = True
        paused_link = self.update_link(link, update_mask)
        assert paused_link.configurations.schema_registry_sync_options.shadow_schema_registry_api.paused, (
            "paused not set after update"
        )
        self.wait_for_context_write_allowed(
            target_sr_client, ".prod", self.simple_a_proto_def
        )

        # Un-pausing restores the block on ".prod".
        paused_link.configurations.schema_registry_sync_options.shadow_schema_registry_api.paused = False
        resumed_link = self.update_link(paused_link, update_mask)
        assert not resumed_link.configurations.schema_registry_sync_options.shadow_schema_registry_api.paused, (
            "paused not cleared after update"
        )
        self.wait_for_context_write_blocked(
            target_sr_client, ".prod", self.simple_b_proto_def
        )

    @cluster(num_nodes=6)
    def test_schema_registry_api_granular_write_blocking(self):
        """
        With API-mode Schema Registry shadowing, only the contexts selected by
        source_filter are write-protected on the target. Writes to contexts
        outside the filter must still succeed.

        The sync task does not need to be running for this test: the write
        protection policy is a pure function of the link configuration, decided
        locally on each write attempt.
        """
        # Touch the target schema registry to materialize the local _schemas
        # topic (creation is not blocked in API mode) and confirm it is empty.
        target_sr_client = self.target_sr_client()
        assert len(self.get_subjects(target_sr_client)) == 0, (
            "Expected no subjects on the target before shadowing"
        )

        # Create a shadow link that shadows only the ".prod" context via the
        # Schema Registry API, mapping contexts identically onto the target.
        created_link = self.create_link("test-link")
        api = created_link.configurations.schema_registry_sync_options.shadow_schema_registry_api
        api.source_url = "http://schema-registry.example.com:8081"
        api.source_filter.contexts.append(".prod")
        api.destination.identity.SetInParent()
        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=["configurations.schema_registry_sync_options"]
        )
        updated_link = self.update_link(created_link, update_mask)
        assert list(
            updated_link.configurations.schema_registry_sync_options.shadow_schema_registry_api.source_filter.contexts
        ) == [".prod"], "source_filter not applied after update"

        # A client write to the in-scope ".prod" context is blocked (HTTP 412)
        # once write protection has propagated to the serving node.
        self.wait_for_context_write_blocked(
            target_sr_client, ".prod", self.simple_proto_def
        )

        # A client write to the out-of-scope ".staging" context succeeds (200).
        allowed = self.post_schema_to_context(
            target_sr_client, ".staging", "orders-value", self.simple_a_proto_def
        )
        assert allowed.status_code == 200, (
            f"Expected 200 for out-of-scope context '.staging'. "
            f"Got {allowed.status_code}: {allowed.text}"
        )

    @cluster(num_nodes=6)
    def test_schema_registry_api_exact_context_mapping_write_blocking(self):
        """
        With API-mode Schema Registry shadowing using an exact
        source->destination context mapping, write protection follows the
        *destination* context name: the mapped destination is blocked, while the
        source-named context (which is not a local destination) and out-of-scope
        contexts remain writable.
        """
        # Touch the target schema registry to materialize the local _schemas
        # topic (creation is not blocked in API mode) and confirm it is empty.
        target_sr_client = self.target_sr_client()
        assert len(self.get_subjects(target_sr_client)) == 0, (
            "Expected no subjects on the target before shadowing"
        )

        # Shadow only the ".prod" source context, remapping it onto
        # ".prod-mirror" on the target.
        created_link = self.create_link("test-link")
        api = created_link.configurations.schema_registry_sync_options.shadow_schema_registry_api
        api.source_url = "http://schema-registry.example.com:8081"
        api.source_filter.contexts.append(".prod")
        api.destination.exact.mappings.add(source=".prod", destination=".prod-mirror")
        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=["configurations.schema_registry_sync_options"]
        )
        updated_link = self.update_link(created_link, update_mask)
        updated_api = updated_link.configurations.schema_registry_sync_options.shadow_schema_registry_api
        assert list(updated_api.source_filter.contexts) == [".prod"], (
            "source_filter not applied after update"
        )
        assert [
            (m.source, m.destination) for m in updated_api.destination.exact.mappings
        ] == [(".prod", ".prod-mirror")], "exact mapping not applied after update"

        # A client write to the mapped destination ".prod-mirror" is owned and so
        # is blocked (HTTP 412) once write protection has propagated.
        self.wait_for_context_write_blocked(
            target_sr_client, ".prod-mirror", self.simple_proto_def
        )

        # The source-named ".prod" is not a local destination, so it is writable.
        allowed_source = self.post_schema_to_context(
            target_sr_client, ".prod", "orders-value", self.simple_a_proto_def
        )
        assert allowed_source.status_code == 200, (
            f"Expected 200 for source-named context '.prod'. "
            f"Got {allowed_source.status_code}: {allowed_source.text}"
        )

        # An out-of-scope context succeeds (200).
        allowed_out = self.post_schema_to_context(
            target_sr_client, ".staging", "orders-value", self.simple_b_proto_def
        )
        assert allowed_out.status_code == 200, (
            f"Expected 200 for out-of-scope context '.staging'. "
            f"Got {allowed_out.status_code}: {allowed_out.text}"
        )
