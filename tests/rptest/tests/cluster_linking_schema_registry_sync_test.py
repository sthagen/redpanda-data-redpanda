# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# End-to-end coverage for the shadow-link Schema Registry "API mode" sync: a
# reference-heavy set of source schemas is replicated into the destination
# cluster's Schema Registry over the HTTP source reader, including schemas added
# while a sync is in flight and schemas added after a sync (picked up by the
# next full sync). The layered diamond DAG mirrors the reconciler
# `concurrent_stress` unit test at a larger scale, so the concurrent fetch/import
# path is exercised against a real registry. Plaintext, default context, no auth
# (auth/TLS coverage is tracked separately).

import json
from typing import Any

import google.protobuf.duration_pb2
import google.protobuf.field_mask_pb2
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import shadow_link_pb2
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import TestContext, cluster
from rptest.services.confluent_schema_registry import ConfluentSchemaRegistryService
from rptest.services.multi_cluster_services import (
    SecondaryClusterArgs,
    SecondaryClusterSpec,
    ServiceType,
)
from rptest.services.redpanda import SchemaRegistryConfig
from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase
from rptest.tests.schema_registry_test import SchemaRegistryRedpandaClient
from rptest.util import firewall_blocked

NS = "com.acme"
LINK_NAME = "sr-sync"

Pair = tuple[str, int]


class SchemaRegistrySyncMixin:
    """Shared shadow-link Schema Registry "API mode" sync suite: source cluster
    SR -> destination cluster SR over the HTTP source reader, at scale and with
    concurrent / post-sync additions.

    Not a Test: mixed into a ShadowLinkTestBase leaf per source vendor (see
    SchemaRegistrySyncE2ETest / ConfluentSchemaRegistrySyncE2ETest below). Each
    leaf supplies the source registry via _make_source_client and declares its
    own node budget; ducktape collects the leaves, never this class."""

    # Members supplied by ShadowLinkTestBase once mixed into a leaf.
    logger: Any
    target_cluster_service: Any
    create_default_link_request: Any
    create_link_with_request: Any
    get_link: Any

    # A layered diamond DAG (top -> mid -> leaf), like the reconciler
    # concurrent_stress unit test but larger: adjacent referrers share referents,
    # so the reconciler must import referents-first while several fibers fetch
    # over the (serialized) HTTP source connection.
    LEAVES = 30
    MIDS = 15
    TOPS = 8
    # Added to the source right after the link is created, racing the first
    # in-flight sync.
    EXTRA_LEAVES = 10
    EXTRA_MIDS = 10

    # --- source Schema Registry hook ---------------------------------------
    # Each leaf points this at its vendor's Schema Registry; the destination
    # side stays Redpanda throughout.

    def _make_source_client(self) -> SchemaRegistryRedpandaClient:
        raise NotImplementedError

    def _source_sr_url(self) -> str:
        # Derive from the seeding client so the link source_url and the client
        # can never point at different registries.
        return self._make_source_client().base_uri()

    # --- schema builders ---------------------------------------------------

    def _record(self, name: str, fields: list[dict]) -> dict:
        return {"type": "record", "name": name, "namespace": NS, "fields": fields}

    def _leaf_schema(self, i: int, evolve: bool = False) -> dict:
        fields: list[dict] = [{"name": "v", "type": "string"}]
        if evolve:
            # Backward-compatible new version: an added field with a default.
            fields.append({"name": "extra", "type": "long", "default": 0})
        return self._record(f"Leaf{i}", fields)

    def _mid_schema(self, i: int, la: int, lb: int) -> dict:
        return self._record(
            f"Mid{i}",
            [
                {"name": "a", "type": f"{NS}.Leaf{la}"},
                {"name": "b", "type": f"{NS}.Leaf{lb}"},
            ],
        )

    def _top_schema(self, i: int, ma: int, mb: int) -> dict:
        return self._record(
            f"Top{i}",
            [
                {"name": "x", "type": f"{NS}.Mid{ma}"},
                {"name": "y", "type": f"{NS}.Mid{mb}"},
            ],
        )

    def _large_schema(self, i: int, default_bytes: int) -> dict:
        # Inflate the raw stored definition with a big field default. Defaults
        # are kept verbatim in the stored body the reconciler budgets by
        # (unlike doc, which canonicalization may drop), so the memory budget
        # sees a genuinely large body.
        return self._record(
            f"Big{i}",
            [{"name": "v", "type": "string", "default": "x" * default_bytes}],
        )

    # --- registration helpers ----------------------------------------------

    def _register(
        self,
        client: SchemaRegistryRedpandaClient,
        subject: str,
        schema: dict,
        references: list[dict] | None = None,
    ) -> int:
        payload: dict[str, Any] = {"schema": json.dumps(schema)}
        if references:
            payload["references"] = references
        resp = client.post_subjects_subject_versions(
            subject=subject, data=json.dumps(payload)
        )
        assert resp.status_code == 200, f"register {subject} failed: {resp.text}"
        return resp.json()["id"]

    def _ref(self, name: str, subject: str, version: int = 1) -> dict:
        return {"name": name, "subject": subject, "version": version}

    def _add_leaf(
        self,
        src: SchemaRegistryRedpandaClient,
        i: int,
        evolve: bool = False,
        version: int = 1,
    ) -> Pair:
        # `version` is the subject version this registration produces (1 for a
        # fresh subject, 2 for the evolved leaf-0); it is not the schema id that
        # _register returns.
        self._register(src, f"leaf-{i}-value", self._leaf_schema(i, evolve))
        return (f"leaf-{i}-value", version)

    def _add_mid(
        self, src: SchemaRegistryRedpandaClient, i: int, la: int, lb: int
    ) -> Pair:
        self._register(
            src,
            f"mid-{i}-value",
            self._mid_schema(i, la, lb),
            references=[
                self._ref(f"{NS}.Leaf{la}", f"leaf-{la}-value"),
                self._ref(f"{NS}.Leaf{lb}", f"leaf-{lb}-value"),
            ],
        )
        return (f"mid-{i}-value", 1)

    def _add_top(
        self, src: SchemaRegistryRedpandaClient, i: int, ma: int, mb: int
    ) -> Pair:
        self._register(
            src,
            f"top-{i}-value",
            self._top_schema(i, ma, mb),
            references=[
                self._ref(f"{NS}.Mid{ma}", f"mid-{ma}-value"),
                self._ref(f"{NS}.Mid{mb}", f"mid-{mb}-value"),
            ],
        )
        return (f"top-{i}-value", 1)

    def _seed_initial_dag(self, src: SchemaRegistryRedpandaClient) -> list[Pair]:
        # Register referents before referrers (the source SR requires it); the
        # reconciler rediscovers the order itself on the destination.
        pairs: list[Pair] = []
        for i in range(self.LEAVES):
            pairs.append(self._add_leaf(src, i))
        for i in range(self.MIDS):
            pairs.append(
                self._add_mid(src, i, (2 * i) % self.LEAVES, (2 * i + 1) % self.LEAVES)
            )
        for i in range(self.TOPS):
            pairs.append(
                self._add_top(src, i, (2 * i) % self.MIDS, (2 * i + 1) % self.MIDS)
            )
        return pairs

    # --- link creation -----------------------------------------------------

    def _create_sr_link(
        self,
        source_url: str | None = None,
        full_sync_interval_sec: int = 2,
        source_filter_subjects: list[str] | None = None,
        source_filter_contexts: list[str] | None = None,
        exact_context_map: dict[str, str] | None = None,
    ) -> str:
        # Create a shadow link that syncs only the Schema Registry, in API mode,
        # pointing at the source cluster's SR endpoint (or an explicit URL, e.g.
        # a bad one to exercise the unavailable path). Short intervals so
        # subsequent full syncs land quickly. An optional exact-subject filter
        # scopes replication. Returns the source URL used.
        if source_url is None:
            source_url = self._source_sr_url()
        req = self.create_default_link_request(
            link_name=LINK_NAME,
            mirror_all_topics=False,
            mirror_all_groups=False,
            mirror_all_acls=False,
        )
        api = shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryApi(
            source_url=source_url,
            tail_interval=google.protobuf.duration_pb2.Duration(seconds=2),
            full_sync_interval=google.protobuf.duration_pb2.Duration(
                seconds=full_sync_interval_sec
            ),
        )
        if source_filter_subjects is not None:
            api.source_filter.subjects.extend(source_filter_subjects)
        if source_filter_contexts is not None:
            api.source_filter.contexts.extend(source_filter_contexts)
        if exact_context_map is not None:
            for source, destination in exact_context_map.items():
                api.destination.exact.mappings.add(
                    source=source, destination=destination
                )
        req.shadow_link.configurations.schema_registry_sync_options.shadow_schema_registry_api.CopyFrom(
            api
        )
        self.create_link_with_request(req=req)
        return source_url

    # --- verification ------------------------------------------------------

    def _schema_view(
        self, client: SchemaRegistryRedpandaClient, subject: str, version: int
    ) -> dict | None:
        # The canonical stored form of one (subject, version): the schema ID,
        # the parsed schema body, and its references. Returns None when absent.
        resp = client.get_subjects_subject_versions_version(subject, version)
        if resp.status_code != 200:
            return None
        body = resp.json()
        return {
            "id": body["id"],
            "schema": json.loads(body["schema"]),
            "references": [
                {"name": r["name"], "subject": r["subject"], "version": r["version"]}
                for r in body.get("references", [])
            ],
        }

    def _wait_synced(
        self,
        src: SchemaRegistryRedpandaClient,
        dest: SchemaRegistryRedpandaClient,
        pairs: list[Pair],
        timeout_sec: int = 120,
    ):
        # Compares the destination against the source's stored form (schema ID,
        # body, and references) so the assertion does not depend on whether the
        # SR canonicalizes on registration. A preserved ID is the point of
        # IMPORT-mode replication.
        def synced() -> bool:
            missing = 0
            for subject, version in pairs:
                want = self._schema_view(src, subject, version)
                assert want is not None, f"source missing {subject} v{version}"
                if self._schema_view(dest, subject, version) != want:
                    missing += 1
            if missing:
                self.logger.debug(f"{missing}/{len(pairs)} not yet synced")
            return missing == 0

        wait_until(
            synced,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg="schemas did not sync to destination Schema Registry",
        )

    def _sr_versions(
        self, client: SchemaRegistryRedpandaClient, subject: str, deleted: bool
    ) -> set[int]:
        resp = client.get_subjects_subject_versions(subject, deleted=deleted)
        return set(resp.json()) if resp.status_code == 200 else set()

    def _version_state(
        self, client: SchemaRegistryRedpandaClient, subject: str
    ) -> tuple[frozenset[int], frozenset[int]]:
        # (active, soft-deleted) versions of a subject. A soft-deleted version
        # is listed only with deleted=true; a fully soft-deleted subject has an
        # empty active set.
        active = self._sr_versions(client, subject, deleted=False)
        every = self._sr_versions(client, subject, deleted=True)
        return frozenset(active), frozenset(every - active)

    def _wait_delete_synced(
        self,
        src: SchemaRegistryRedpandaClient,
        dest: SchemaRegistryRedpandaClient,
        subjects: list[str],
        timeout_sec: int = 120,
    ):
        # The destination's per-subject (active, deleted) version partition must
        # converge to the source's.
        def synced() -> bool:
            for subject in subjects:
                if self._version_state(dest, subject) != self._version_state(
                    src, subject
                ):
                    return False
            return True

        wait_until(
            synced,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg="soft-delete state did not converge to the source",
        )

    # --- status-counter checks ---------------------------------------------

    # Counters expected to stay zero in the override-free DAG tests: those DAGs
    # register no mode or compatibility overrides, so mode/config replication is
    # a no-op, and unsupported-feature handling is unimplemented. A test that
    # sets overrides (test_schema_registry_api_sync_compatibility) asserts the
    # compatibility counter advances.
    EXPECTED_ZERO_COUNTERS = (
        "compatibility_configs_changed",
        "modes_changed",
        "unsupported_features_removed",
    )

    def _admin_sr_status(self):
        link = self.get_link(LINK_NAME)
        return link.status.schema_registry_sync_status

    def _observe_current_full_sync(self, timeout_sec: int = 60):
        # While a sync runs, the status exposes current_sync; capture it during
        # the first (longest-running) full sync of the seeded DAG and confirm it
        # is a FULL sync. Tail sync is not implemented yet, so FULL is the only
        # type produced -- this pins the reported sync_type regardless.
        observed: dict[str, Any] = {}

        def in_progress() -> bool:
            sr = self._admin_sr_status()
            if sr.HasField("current_sync"):
                observed["sync_type"] = sr.current_sync.sync_type
                return True
            return False

        wait_until(
            in_progress,
            timeout_sec=timeout_sec,
            backoff_sec=0.2,
            err_msg="never observed a Schema Registry sync in progress",
        )
        assert (
            observed["sync_type"] == shadow_link_pb2.SCHEMA_REGISTRY_SYNC_TYPE_FULL
        ), observed

    def _verify_counters(self, all_pairs: list[Pair]):
        expected_versions = len(all_pairs)
        expected_subjects = len({s for s, _ in all_pairs})

        # The counters reflect the most recently completed full sync; give the
        # task a couple of full-sync cycles to re-scan after the last addition.
        def counters_ready() -> bool:
            sr = self._admin_sr_status()
            inv = sr.inventory
            totals = sr.totals_since_task_start
            return (
                inv.selected_source_subjects == expected_subjects
                and inv.selected_source_subject_versions == expected_versions
                and inv.destination_subjects == expected_subjects
                and inv.destination_subject_versions == expected_versions
                and totals.subject_versions_changed == expected_versions
                and totals.errors == 0
            )

        wait_until(
            counters_ready,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="SR sync status counters did not reach the expected values",
        )

        sr = self._admin_sr_status()
        self._log_counters("admin API", sr)

        # Timestamps: the last completed full sync is present with finish at or
        # after start; the cumulative summary carries the task start time and,
        # being open-ended, has no finish time.
        assert sr.HasField("last_full_sync"), sr
        lfs = sr.last_full_sync
        assert lfs.HasField("start_time") and lfs.HasField("finish_time"), lfs
        assert lfs.finish_time.ToNanoseconds() >= lfs.start_time.ToNanoseconds(), lfs

        # Mapped-but-unimplemented counters must be zero (flagged above).
        totals = sr.totals_since_task_start
        # The cumulative summary carries the task start time (stamped once on the
        # task's first run) and, being open-ended, has no finish time.
        assert totals.HasField("start_time"), totals
        assert totals.start_time.seconds > 0, totals
        assert not totals.HasField("finish_time"), totals
        for name in self.EXPECTED_ZERO_COUNTERS:
            assert getattr(totals, name) == 0, (
                f"unexpected {name}={getattr(totals, name)}"
            )

        # Cross-check the rpk `shadow status` rendering against the admin API.
        self._verify_rpk_status(expected_subjects, expected_versions)

    def _log_counters(self, source: str, sr):
        inv = sr.inventory
        totals = sr.totals_since_task_start
        self.logger.info(
            f"[{source}] inventory: "
            f"selected_source_subjects={inv.selected_source_subjects} "
            f"selected_source_subject_versions={inv.selected_source_subject_versions} "
            f"destination_subjects={inv.destination_subjects} "
            f"destination_subject_versions={inv.destination_subject_versions}"
        )
        self.logger.info(
            f"[{source}] totals_since_task_start: "
            f"subject_versions_changed={totals.subject_versions_changed} "
            f"compatibility_configs_changed={totals.compatibility_configs_changed} "
            f"modes_changed={totals.modes_changed} "
            f"unsupported_features_removed={totals.unsupported_features_removed} "
            f"errors={totals.errors}"
        )

    def _verify_rpk_status(self, expected_subjects: int, expected_versions: int):
        # `rpk shadow status --format json` renders the same admin-v2
        # SchemaRegistrySyncStatus; assert it agrees with the admin API.
        rpk = RpkTool(self.target_cluster_service)
        status = rpk.shadow_status(LINK_NAME)
        self.logger.info(f"rpk shadow status: {json.dumps(status)}")
        sr = status["schema_registry"]
        inv = sr["inventory"]
        totals = sr["totals_since_task_start"]
        assert inv["selected_source_subjects"] == expected_subjects, status
        assert inv["selected_source_subject_versions"] == expected_versions, status
        assert inv["destination_subjects"] == expected_subjects, status
        assert inv["destination_subject_versions"] == expected_versions, status
        assert totals["subject_versions_changed"] == expected_versions, status
        assert totals["errors"] == 0, status
        for name in self.EXPECTED_ZERO_COUNTERS:
            assert totals[name] == 0, status

    def _test_schema_registry_api_sync_e2e(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # The destination `_schemas` topic is NOT warmed up here on purpose:
        # creating the SR-API shadow link (below) bootstraps it, so this
        # exercises that path.

        # 1. Seed the source SR with the large reference DAG.
        initial = self._seed_initial_dag(src)

        # 2. Create a shadow link that only syncs the Schema Registry.
        source_sr_url = self._create_sr_link()
        self.logger.info(f"source SR: {source_sr_url}; seeded {len(initial)} subjects")

        # While the first full sync of the seeded DAG runs, confirm the status
        # reports an in-progress FULL sync.
        self._observe_current_full_sync()

        # 3. SCENARIO A -- additions racing an in-flight sync. Immediately after
        #    creating the link (the first full sync is now running), register a
        #    second wave of leaves + mids that reference them. Whether they are
        #    enumerated by the current sync or the next one, they must all land.
        mid_sync: list[Pair] = []
        for i in range(self.LEAVES, self.LEAVES + self.EXTRA_LEAVES):
            mid_sync.append(self._add_leaf(src, i))
        for i in range(self.MIDS, self.MIDS + self.EXTRA_MIDS):
            j = i - self.MIDS
            la = self.LEAVES + (2 * j) % self.EXTRA_LEAVES
            lb = self.LEAVES + (2 * j + 1) % self.EXTRA_LEAVES
            mid_sync.append(self._add_mid(src, i, la, lb))

        # Wait for the initial DAG and the mid-sync additions to all replicate,
        # with bodies, references, and IDs matching the source.
        self._wait_synced(src, dest, initial + mid_sync)
        self.logger.info(
            f"initial + mid-sync waves synced ({len(initial) + len(mid_sync)} subjects)"
        )

        # 4. SCENARIO B -- additions after a completed sync, picked up by the
        #    next full sync. Add a new backward-compatible version of an existing
        #    subject, a brand-new standalone subject, and new tops referencing
        #    already-synced mids.
        after_sync: list[Pair] = []
        after_sync.append(self._add_leaf(src, 0, evolve=True, version=2))
        self._register(
            src, "late-value", self._record("Late", [{"name": "n", "type": "int"}])
        )
        after_sync.append(("late-value", 1))
        for i in range(self.TOPS, self.TOPS + 4):
            after_sync.append(
                self._add_top(src, i, (2 * i) % self.MIDS, (2 * i + 1) % self.MIDS)
            )

        # Re-verify leaf-0 v1 alongside its new v2: a backward-compatible new
        # version must not clobber the original. Kept out of all_pairs below so
        # the counter totals are not double-counted.
        self._wait_synced(src, dest, after_sync + [("leaf-0-value", 1)])
        self.logger.info("post-sync additions replicated to destination")

        # 5. Sanity-check the destination subject inventory covers every wave.
        all_pairs = initial + mid_sync + after_sync
        all_subjects = {s for s, _ in all_pairs}
        dest_subjects = set(dest.get_subjects().json())
        # Exact equality (not subset): the destination must hold the synced
        # subjects and nothing more -- catches spurious or duplicated imports.
        assert dest_subjects == all_subjects, (
            f"missing: {all_subjects - dest_subjects}; "
            f"unexpected: {dest_subjects - all_subjects}"
        )

        # 6. Surface and verify the Schema Registry sync status counters, both
        #    through the shadow-link admin API and the rpk `shadow status`
        #    rendering of it.
        self._verify_counters(all_pairs)

    def _test_schema_registry_api_sync_soft_delete(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # seeded-value: mixed active/deleted before the first sync.
        # after-value: version-level delete after a sync.
        # whole-value: subject-level delete after a sync (source's
        #   active-only listing then 404s).
        seeded, after, whole = "seeded-value", "after-value", "whole-value"

        def _v1(name: str) -> dict:
            return self._record(name, [{"name": "v", "type": "string"}])

        def _v2(name: str) -> dict:
            # Backward-compatible evolution (added field with a default).
            return self._record(
                name,
                [
                    {"name": "v", "type": "string"},
                    {"name": "e", "type": "long", "default": 0},
                ],
            )

        self._register(src, seeded, _v1("Seeded"))
        self._register(src, seeded, _v2("Seeded"))
        assert src.delete_subject_version(seeded, "2").status_code == 200

        self._register(src, after, _v1("After"))
        self._register(src, after, _v2("After"))

        self._register(src, whole, _v1("Whole"))

        self._create_sr_link()

        subjects = [seeded, after, whole]
        self._wait_delete_synced(src, dest, subjects)

        assert src.delete_subject_version(after, "2").status_code == 200
        assert src.delete_subject(whole).status_code == 200
        self._wait_delete_synced(src, dest, subjects)

    def _test_schema_registry_api_sync_compatibility(self):
        # A subject-level compatibility override round-tripping through the
        # destination's write API -- a path the reconciler unit fakes cannot
        # cover -- then un-setting it so the destination reverts to the
        # inherited default. Mode replication is covered e2e by
        # test_schema_registry_api_sync_context_remap.
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        keep = "keep-value"
        schema = self._record("V", [{"name": "v", "type": "string"}])
        self._register(src, keep, schema)
        # A subject-level compatibility override on the source, to be mirrored.
        assert (
            src.set_config_subject(
                keep, data=json.dumps({"compatibility": "FULL"})
            ).status_code
            == 200
        )

        self._create_sr_link()
        self._wait_synced(src, dest, [(keep, 1)])

        # The compatibility override replicates to the destination and moves the
        # compatibility_configs_changed counter.
        def config_synced() -> bool:
            c = dest.get_config_subject(keep)
            return c.status_code == 200 and c.json().get("compatibilityLevel") == "FULL"

        wait_until(
            config_synced,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="compatibility config did not replicate to the destination",
        )
        wait_until(
            lambda: self._admin_sr_status().totals_since_task_start.compatibility_configs_changed
            >= 1,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="compatibility_configs_changed counter did not advance",
        )

        # Un-setting the source override propagates as a delete, reverting the
        # destination subject to the inherited (global) default rather than
        # leaving the stale FULL override behind.
        before = self._admin_sr_status().totals_since_task_start.compatibility_configs_changed
        assert src.delete_config_subject(keep).status_code == 200

        def config_delete_synced() -> bool:
            # With no subject-level override remaining, a non-fallback GET on the
            # destination reports it missing (40401); a fallback GET now yields
            # the global default rather than the removed FULL override.
            missing = dest.get_config_subject(keep).status_code == 404
            fallback = dest.get_config_subject(keep, fallback=True)
            reverted = (
                fallback.status_code == 200
                and fallback.json().get("compatibilityLevel") != "FULL"
            )
            return missing and reverted

        wait_until(
            config_delete_synced,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="config override deletion did not replicate to the destination",
        )
        wait_until(
            lambda: self._admin_sr_status().totals_since_task_start.compatibility_configs_changed
            > before,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="compatibility_configs_changed counter did not advance for the delete",
        )

    def _test_schema_registry_api_sync_hard_delete(self):
        # A source hard-delete (which requires a soft-delete first on the
        # destination) being propagated as a purge -- a path the reconciler unit
        # fakes cannot cover.
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        keep, gone = "keep-value", "gone-value"
        schema = self._record("V", [{"name": "v", "type": "string"}])
        self._register(src, keep, schema)
        self._register(src, gone, schema)

        self._create_sr_link()
        self._wait_synced(src, dest, [(keep, 1), (gone, 1)])

        # Hard-delete gone-value at the source (soft then permanent). The next
        # full sync must purge it from the destination, soft-deleting the still
        # active destination version before the permanent delete.
        assert src.delete_subject(gone).status_code == 200
        assert src.delete_subject(gone, permanent=True).status_code == 200

        def gone_purged() -> bool:
            active, deleted = self._version_state(dest, gone)
            return not active and not deleted

        wait_until(
            gone_purged,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="source hard-delete was not propagated to the destination",
        )

        # The in-source subject is untouched by the purge.
        assert self._schema_view(dest, keep, 1) is not None

    def _test_schema_registry_api_sync_context_remap(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        def qualified(ctx: str, name: str) -> str:
            return f":{ctx}:{name}"

        def by_name(refs: list[dict]) -> list[dict]:
            return sorted(refs, key=lambda r: r["name"])

        base_src = qualified(".prod", "base-value")
        leaf_src = qualified(".prod", "leaf-value")
        orders_src = qualified(".prod", "orders-value")
        base_dst = qualified(".dest", "base-value")
        leaf_dst = qualified(".dest", "leaf-value")
        orders_dst = qualified(".dest", "orders-value")

        base_schema = self._record("Base", [{"name": "b", "type": "string"}])
        leaf_schema = self._record("Leaf", [{"name": "l", "type": "string"}])
        orders_schema = self._record(
            "Orders",
            [
                {"name": "base", "type": f"{NS}.Base"},
                {"name": "leaf", "type": f"{NS}.Leaf"},
            ],
        )

        # Register referents first (the source SR validates references on
        # registration). orders references base with a context-qualified
        # reference (":.prod:base-value") and leaf with an unqualified one (the
        # bare "leaf-value", which the source resolves against orders' own .prod
        # context).
        self._register(src, base_src, base_schema)
        self._register(src, leaf_src, leaf_schema)
        self._register(
            src,
            orders_src,
            orders_schema,
            references=[
                self._ref(f"{NS}.Base", base_src),
                self._ref(f"{NS}.Leaf", "leaf-value"),
            ],
        )

        # Subject-level compatibility and mode overrides on the referrer, both to
        # be mirrored under the remapped context. Set the mode last: READONLY
        # blocks no further writes here, and the destination import bypasses it.
        assert (
            src.set_config_subject(
                orders_src, data=json.dumps({"compatibility": "FULL"})
            ).status_code
            == 200
        )
        assert (
            src.set_mode_subject(
                orders_src, data=json.dumps({"mode": "READONLY"})
            ).status_code
            == 200
        )

        # The source's canonical stored forms; the destination is compared
        # against these (not against the registered dicts) so the assertions do
        # not depend on SR canonicalization. Capturing them also guards the test
        # premise: the qualified reference is stored qualified and the
        # unqualified one is NOT canonicalized into a qualified subject.
        src_base = self._schema_view(src, base_src, 1)
        src_leaf = self._schema_view(src, leaf_src, 1)
        src_orders = self._schema_view(src, orders_src, 1)
        assert src_base is not None and src_leaf is not None
        assert src_orders is not None
        assert by_name(src_orders["references"]) == by_name(
            [
                {"name": f"{NS}.Base", "subject": base_src, "version": 1},
                {"name": f"{NS}.Leaf", "subject": "leaf-value", "version": 1},
            ]
        ), src_orders["references"]

        # Replicate only .prod, remapping it onto the destination .dest context.
        self._create_sr_link(
            source_filter_contexts=[".prod"],
            exact_context_map={".prod": ".dest"},
        )

        expected_orders_refs = by_name(
            [
                # The qualified reference's own context is remapped .prod -> .dest.
                {"name": f"{NS}.Base", "subject": base_dst, "version": 1},
                # The unqualified reference is untouched: on the destination it
                # resolves against orders' remapped .dest parent.
                {"name": f"{NS}.Leaf", "subject": "leaf-value", "version": 1},
            ]
        )

        def remapped() -> bool:
            b = self._schema_view(dest, base_dst, 1)
            leaf = self._schema_view(dest, leaf_dst, 1)
            o = self._schema_view(dest, orders_dst, 1)
            if b is None or leaf is None or o is None:
                return False
            # Referents (no references of their own) replicate verbatim.
            if b != src_base or leaf != src_leaf:
                return False
            # The referrer keeps its ID and body; only its reference contexts
            # remap.
            return (
                o["id"] == src_orders["id"]
                and o["schema"] == src_orders["schema"]
                and by_name(o["references"]) == expected_orders_refs
            )

        wait_until(
            remapped,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="schemas/references did not remap into the .dest context",
        )

        # Nothing is written under the original .prod context on the destination.
        for sub in (base_src, leaf_src, orders_src):
            assert self._schema_view(dest, sub, 1) is None, sub

        # The compatibility and mode overrides replicate under the remapped
        # context (read the explicit override, not the global fallback).
        def overrides_remapped() -> bool:
            c = dest.get_config_subject(orders_dst)
            m = dest.get_mode_subject(orders_dst, fallback=False)
            return (
                c.status_code == 200
                and c.json().get("compatibilityLevel") == "FULL"
                and m.status_code == 200
                and m.json().get("mode") == "READONLY"
            )

        wait_until(
            overrides_remapped,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="compatibility/mode overrides did not remap into .dest",
        )

        # Both overrides are counted, and the remap completed error-free.
        def counters_advanced() -> bool:
            totals = self._admin_sr_status().totals_since_task_start
            return (
                totals.compatibility_configs_changed >= 1
                and totals.modes_changed >= 1
                and totals.errors == 0
            )

        wait_until(
            counters_advanced,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="mode/compatibility counters did not advance under remap",
        )

    def _test_schema_registry_api_sync_out_of_scope_reference(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # leaf-0 is a referent; mid-0 references it; ok-value is independent.
        self._add_leaf(src, 0)
        self._add_mid(src, 0, 0, 0)
        self._register(
            src, "ok-value", self._record("Ok", [{"name": "v", "type": "string"}])
        )

        # Select the referrer and the independent subject but NOT the referent,
        # so mid-0's reference is out of scope. Importing mid-0 must fail and be
        # counted, while ok-value still syncs and the full sync completes (the
        # link neither parks nor faults).
        self._create_sr_link(source_filter_subjects=["mid-0-value", "ok-value"])

        self._wait_synced(src, dest, [("ok-value", 1)])

        # last_full_sync.errors >= 1 proves the error was counted within a
        # completed full sync (best-effort), not that the task parked/faulted.
        def errored() -> bool:
            sr = self._admin_sr_status()
            return (
                sr.HasField("last_full_sync")
                and sr.last_full_sync.errors >= 1
                and sr.totals_since_task_start.errors >= 1
            )

        wait_until(
            errored,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="out-of-scope reference did not surface as a counted error",
        )

        # The referrer never imported; the in-scope independent subject did.
        dest_subjects = set(dest.get_subjects().json())
        assert "ok-value" in dest_subjects, dest_subjects
        assert "mid-0-value" not in dest_subjects, dest_subjects

    def _test_schema_registry_api_sync_recovers_after_source_unavailable(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        self._register(
            src,
            "orders-value",
            self._record("Orders", [{"name": "v", "type": "string"}]),
        )

        # Link creation preflight-probes the source for reachability, so the
        # link must be created against the live source; let its first full sync
        # land before the source is cut off.
        self._create_sr_link()
        self._wait_synced(src, dest, [("orders-value", 1)])

        wait_until(
            lambda: self._admin_sr_status().HasField("last_full_sync"),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="link did not complete its first full sync",
        )

        # Sever the destination cluster's egress to the source Schema Registry
        # (which listens on 8081): the running sync's list_contexts fails with
        # source_unavailable, so the task records last_error_message and leaves
        # _last_full_sync unadvanced -- it keeps retrying on the normal cadence,
        # so recovery needs no config change, just the source coming back.
        with firewall_blocked(self.target_cluster_service.nodes, 8081):
            wait_until(
                lambda: self._admin_sr_status().last_error_message != "",
                timeout_sec=90,
                backoff_sec=1,
                err_msg="link did not report an error while the source was unreachable",
            )
            # Frozen while unreachable (no full sync can complete), so a later
            # finish_time proves a fresh sync ran once connectivity returned.
            # last_error_message is sticky (never cleared on success), so it
            # cannot serve as the recovery signal.
            synced_before = (
                self._admin_sr_status().last_full_sync.finish_time.ToNanoseconds()
            )

        # Connectivity restored: the next full sync reaches the source on the
        # normal cadence and completes.
        wait_until(
            lambda: self._admin_sr_status().last_full_sync.finish_time.ToNanoseconds()
            > synced_before,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="link did not recover after the source became reachable again",
        )

    def _test_schema_registry_api_sync_memory_backpressure(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # Shrink the reconcile memory budget to its 1 MiB floor and raise
        # parallelism so many bodies are fetched at once; their combined size
        # exceeds the budget, forcing the byte-budget semaphore to serialize.
        # Each body stays under the 200 KiB per-allocation cap the ducktape log
        # checker enforces, so no single import trips an oversized-allocation
        # failure while the aggregate still exercises backpressure.
        self.target_cluster_service.set_cluster_config(
            {
                "schema_registry_sync_memory_bytes": 1024 * 1024,
                "schema_registry_sync_parallelism": 14,
            }
        )

        # ~100 KiB per body x 16 subjects: 14 fetched concurrently is ~1.4 MiB,
        # above the 1 MiB budget.
        body = 100 * 1024
        pairs: list[Pair] = []
        for i in range(16):
            subject = f"big-{i}-value"
            self._register(src, subject, self._large_schema(i, body))
            pairs.append((subject, 1))

        self._create_sr_link()
        self._wait_synced(src, dest, pairs)

    def _test_schema_registry_api_sync_survives_leadership_change(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)
        admin = Admin(self.target_cluster_service)

        # A small reference DAG so the first task instance does real import work.
        pairs: list[Pair] = []
        for i in range(5):
            pairs.append(self._add_leaf(src, i))
        for i in range(3):
            pairs.append(self._add_mid(src, i, (2 * i) % 5, (2 * i + 1) % 5))
        expected_versions = len(pairs)
        expected_subjects = len({s for s, _ in pairs})

        self._create_sr_link()
        self._wait_synced(src, dest, pairs)

        # Snapshot the first task instance once it has fully synced.
        def first_synced() -> bool:
            sr = self._admin_sr_status()
            return (
                sr.HasField("last_full_sync")
                and sr.inventory.destination_subject_versions == expected_versions
                and sr.totals_since_task_start.subject_versions_changed
                >= expected_versions
            )

        wait_until(
            first_synced,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="first task instance did not fully sync",
        )
        before_start = (
            self._admin_sr_status().totals_since_task_start.start_time.ToNanoseconds()
        )

        # Move _schemas/0 leadership to another destination broker. The sync
        # task follows leadership, so a fresh instance takes over there.
        info = admin.get_partitions(namespace="kafka", topic="_schemas", partition=0)
        leader = info["leader_id"]
        replicas = [r["node_id"] for r in info["replicas"]]
        assert len(replicas) > 1, f"_schemas/0 has no failover target: {info}"
        target = next(n for n in replicas if n != leader)
        admin.partition_transfer_leadership(
            namespace="kafka", topic="_schemas", partition=0, target_id=target
        )
        admin.await_stable_leader(
            topic="_schemas",
            partition=0,
            namespace="kafka",
            timeout_s=60,
            check=lambda node_id: node_id == target,
        )

        # The new instance resets its cumulative counters (documented on
        # totals_since_task_start): a fresh, later start_time and
        # subject_versions_changed back at 0. It re-derives the destination
        # inventory from the replicated _schemas log and completes a full sync
        # that imports nothing (last_full_sync == 0) -- everything is already
        # present. This relies on the destination scan syncing the local store
        # first: without it, the freshly-elected leader could see a stale view
        # of its own store, spuriously re-import already-present versions, and
        # permanently inflate subject_versions_changed for this instance (it
        # only ever accumulates, so a transient race here would never recover).
        def re_derived() -> bool:
            sr = self._admin_sr_status()
            totals = sr.totals_since_task_start
            return (
                sr.HasField("last_full_sync")
                and totals.start_time.ToNanoseconds() > before_start
                and totals.subject_versions_changed == 0
                and totals.errors == 0
                and sr.last_full_sync.subject_versions_changed == 0
                and sr.last_full_sync.errors == 0
                and sr.inventory.selected_source_subjects == expected_subjects
                and sr.inventory.destination_subject_versions == expected_versions
            )

        wait_until(
            re_derived,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="new leader did not re-derive counters after the bounce",
        )

        # The new leader keeps syncing: a subject added now is imported by it,
        # advancing the new instance's cumulative change counter.
        base_changed = (
            self._admin_sr_status().totals_since_task_start.subject_versions_changed
        )
        self._register(
            src,
            "post-bounce-value",
            self._record("PostBounce", [{"name": "v", "type": "string"}]),
        )
        self._wait_synced(src, dest, [("post-bounce-value", 1)])
        wait_until(
            lambda: self._admin_sr_status().totals_since_task_start.subject_versions_changed
            > base_changed,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="new leader did not count the post-bounce import",
        )

        # Now bounce leadership back to the original broker (A -> B -> A). Its
        # task instance ran before, was stopped on losing leadership, and is now
        # reused -- not a fresh object. stop() clears the in-memory sync state,
        # so the returning leader must look just as fresh as B did: a later
        # start_time, zeroed cumulative counters, and an immediate full sync (a
        # stale _last_full_sync would otherwise defer it up to the full-sync
        # interval). Without the reset it would report its pre-B totals instead.
        expected_subjects += 1  # post-bounce-value
        expected_versions += 1
        before_return_start = (
            self._admin_sr_status().totals_since_task_start.start_time.ToNanoseconds()
        )
        admin.partition_transfer_leadership(
            namespace="kafka", topic="_schemas", partition=0, target_id=leader
        )
        admin.await_stable_leader(
            topic="_schemas",
            partition=0,
            namespace="kafka",
            timeout_s=60,
            check=lambda node_id: node_id == leader,
        )

        def returning_leader_fresh() -> bool:
            sr = self._admin_sr_status()
            totals = sr.totals_since_task_start
            return (
                sr.HasField("last_full_sync")
                and totals.start_time.ToNanoseconds() > before_return_start
                and totals.subject_versions_changed == 0
                and totals.errors == 0
                and sr.last_full_sync.subject_versions_changed == 0
                and sr.last_full_sync.errors == 0
                and sr.inventory.selected_source_subjects == expected_subjects
                and sr.inventory.destination_subject_versions == expected_versions
            )

        wait_until(
            returning_leader_fresh,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="reused instance did not reset state on regaining leadership",
        )


class SchemaRegistrySyncE2ETest(ShadowLinkTestBase, SchemaRegistrySyncMixin):
    """Redpanda source: a secondary Redpanda cluster provides the source Schema
    Registry (co-located on its brokers).

    Node budget: 3 (destination Redpanda) + 3 (source Redpanda) = 6."""

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        source_sr_config = SchemaRegistryConfig()
        source_sr_config.mode_mutability = True
        super().__init__(
            test_context,
            # Source (secondary) cluster runs a Schema Registry too.
            secondary_cluster_args=SecondaryClusterArgs(
                schema_registry_config=source_sr_config
            ),
            # Destination (primary) cluster Schema Registry.
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    def _make_source_client(self) -> SchemaRegistryRedpandaClient:
        return SchemaRegistryRedpandaClient(self.source_cluster_service)

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_e2e(self):
        self._test_schema_registry_api_sync_e2e()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_soft_delete(self):
        self._test_schema_registry_api_sync_soft_delete()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_compatibility(self):
        self._test_schema_registry_api_sync_compatibility()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_hard_delete(self):
        self._test_schema_registry_api_sync_hard_delete()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_context_remap(self):
        self._test_schema_registry_api_sync_context_remap()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_out_of_scope_reference(self):
        self._test_schema_registry_api_sync_out_of_scope_reference()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_recovers_after_source_unavailable(self):
        self._test_schema_registry_api_sync_recovers_after_source_unavailable()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_memory_backpressure(self):
        self._test_schema_registry_api_sync_memory_backpressure()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_survives_leadership_change(self):
        self._test_schema_registry_api_sync_survives_leadership_change()


class ConfluentSchemaRegistrySyncE2ETest(ShadowLinkTestBase, SchemaRegistrySyncMixin):
    """Confluent Platform Schema Registry (backed by an Apache Kafka source
    cluster) as the shadow-link source; the destination stays Redpanda.
    See CORE-16776 / CORE-16357.

    Node budget: 3 (destination Redpanda) + 1 (Apache Kafka source) + 1
    (Confluent SR) = 5. The Confluent SR is a separate service that needs its
    own node (a Redpanda source's SR is co-located on its brokers), so the Kafka
    source is held to a single broker -- see the SecondaryClusterArgs below.

    Runs the full suite. Against a Confluent source, global compat/mode
    replication has a known cross-vendor gap -- see EXPECTED_ZERO_COUNTERS."""

    # Full-sync compat/mode replication reads the registry-wide global config
    # via Redpanda's ":.__GLOBAL:" pseudo-subject. A Confluent source 404s it
    # (it exposes the global config at GET /config, with no subject), so the
    # sync reads "no override" and deletes the destination's global config,
    # landing compatibility_configs_changed at 1 instead of 0. Known cross-vendor
    # gap in global compat replication, orthogonal to what this suite exercises,
    # so drop that one counter from the zero-check here. (modes_changed stays 0
    # on the pinned Confluent version.)
    EXPECTED_ZERO_COUNTERS = ("modes_changed", "unsupported_features_removed")

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(
            test_context,
            # Destination (primary) cluster Schema Registry; the source SR is
            # the separate Confluent service started in setUp.
            schema_registry_config=SchemaRegistryConfig(),
            # Single-broker Kafka source: the Confluent SR takes a node of its
            # own, so a one-broker source keeps the topology at 5 nodes (3 dest
            # + 1 kafka + 1 SR). A one-broker source is fine here -- the SR's
            # _schemas topic is RF=1.
            secondary_cluster_args=SecondaryClusterArgs(num_brokers=1),
            *args,
            **kwargs,
        )
        self._confluent_sr: ConfluentSchemaRegistryService | None = None

    def get_source_cluster_spec(self) -> SecondaryClusterSpec:
        # Source cluster is Apache Kafka (KRaft); the Confluent SR is attached to
        # it in setUp. Mirrors the Kafka-source topology used by
        # cluster_linking_e2e_test.
        return SecondaryClusterSpec(
            ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
        )

    def setUp(self):
        # Starts the Apache Kafka source + Redpanda destination clusters.
        super().setUp()
        # Attach a Confluent Schema Registry to the Kafka source and start it.
        self._confluent_sr = ConfluentSchemaRegistryService(
            self.test_context, bootstrap_provider=self.source_cluster_service
        )
        self._confluent_sr.start()

    def _confluent(self) -> ConfluentSchemaRegistryService:
        assert self._confluent_sr is not None, "Confluent SR not started"
        return self._confluent_sr

    def _make_source_client(self) -> SchemaRegistryRedpandaClient:
        sr = self._confluent()
        # The base client hardcodes port 8081 and only needs
        # .nodes[*].account.hostname + .logger, so the ducktape service
        # duck-types cleanly; the Confluent SR must use the default port.
        assert sr.port == 8081, "Confluent SR must listen on 8081 for the SR client"
        return SchemaRegistryRedpandaClient(sr)  # type: ignore[arg-type]

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_e2e(self):
        self._test_schema_registry_api_sync_e2e()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_soft_delete(self):
        self._test_schema_registry_api_sync_soft_delete()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_out_of_scope_reference(self):
        self._test_schema_registry_api_sync_out_of_scope_reference()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_recovers_after_source_unavailable(self):
        self._test_schema_registry_api_sync_recovers_after_source_unavailable()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_memory_backpressure(self):
        self._test_schema_registry_api_sync_memory_backpressure()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_survives_leadership_change(self):
        self._test_schema_registry_api_sync_survives_leadership_change()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_compatibility(self):
        self._test_schema_registry_api_sync_compatibility()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_hard_delete(self):
        self._test_schema_registry_api_sync_hard_delete()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_context_remap(self):
        self._test_schema_registry_api_sync_context_remap()
