# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient

from rptest.services.admin import OutboundDataMigration, InboundDataMigration, NamespacedTopic, InboundTopic, MigrationAction
from rptest.services.redpanda import RedpandaService
from requests.exceptions import ConnectionError

import time
import requests


def now():
    return int(time.time() * 1000)


class DataMigrationTestMixin:
    def wait_partitions_appear(self,
                               topics: list[TopicSpec],
                               redpanda: RedpandaService | None = None):
        if redpanda is None:
            redpanda = self.redpanda
        client = DefaultClient(redpanda)

        # we may be unlucky to query a slow node
        def topic_has_all_partitions(t: TopicSpec):
            part_cnt = len(client.describe_topic(t.name).partitions)
            redpanda.logger.debug(
                f"topic {t.name} has {part_cnt} partitions out of {t.partition_count} expected"
            )
            return t.partition_count == part_cnt

        def err_msg():
            msg = "Failed waiting for partitions to appear:\n"
            for t in topics:
                msg += f"   {t.name} expected {t.partition_count} partitions, "
                msg += f"got {len(client.describe_topic(t.name).partitions)} partitions\n"
            return msg

        wait_until(lambda: all(topic_has_all_partitions(t) for t in topics),
                   timeout_sec=90,
                   backoff_sec=1,
                   err_msg=err_msg)

    def wait_partitions_disappear(self,
                                  topics: list[str],
                                  redpanda: RedpandaService | None = None):
        if redpanda is None:
            redpanda = self.redpanda
        client = DefaultClient(redpanda)

        # we may be unlucky to query a slow node
        wait_until(lambda: all(
            client.describe_topic(t).partitions == [] for t in topics),
                   timeout_sec=90,
                   backoff_sec=1,
                   err_msg=f"Failed waiting for partitions to disappear")

    def get_migration(self,
                      id,
                      node=None,
                      redpanda: RedpandaService | None = None):
        if redpanda is None:
            redpanda = self.redpanda

        try:
            return redpanda._admin.get_data_migration(id, node).json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise

    def get_migrations_map(self,
                           node=None,
                           redpanda: RedpandaService | None = None):
        if redpanda is None:
            redpanda = self.redpanda

        redpanda.logger.debug("calling self.admin.list_data_migrations")
        migrations = redpanda._admin.list_data_migrations(node).json()
        redpanda.logger.debug(
            "received self.admin.list_data_migrations result")
        return {migration["id"]: migration for migration in migrations}

    def on_all_live_nodes(self,
                          migration_id,
                          predicate,
                          redpanda: RedpandaService | None = None):
        if redpanda is None:
            redpanda = self.redpanda

        success_cnt = 0
        exception_cnt = 0
        for n in redpanda.nodes:
            try:
                map = self.get_migrations_map(n, redpanda=redpanda)
                redpanda.logger.debug(f"migrations on node {n.name}: {map}")
                list_item = map[migration_id] if migration_id in map else None
                individual = self.get_migration(migration_id,
                                                n,
                                                redpanda=redpanda)

                if predicate(list_item) and predicate(individual):
                    success_cnt += 1
                else:
                    return False
            except ConnectionError:
                exception_cnt += 1
        return success_cnt > exception_cnt

    def validate_timing(self, time_before, happened_at):
        time_now = now()
        self.logger.debug(f"{time_before=}, {happened_at=}, {time_now=}")
        err_ms = 5  # allow for ntp error across nodes
        assert time_before - err_ms <= happened_at <= time_now + err_ms

    def wait_migration_appear(self,
                              migration_id,
                              assure_created_after,
                              redpanda: RedpandaService | None = None):
        if redpanda is None:
            redpanda = self.redpanda

        def migration_present_on_node(m):
            if m is None:
                return False
            self.validate_timing(assure_created_after, m['created_timestamp'])
            return True

        def migration_is_present(id: int):
            return self.on_all_live_nodes(id,
                                          migration_present_on_node,
                                          redpanda=redpanda)

        wait_until(
            lambda: migration_is_present(migration_id),
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"Expected migration with id {migration_id} is present")

    def create_and_wait(self,
                        migration: InboundDataMigration
                        | OutboundDataMigration,
                        redpanda: RedpandaService | None = None):
        if redpanda is None:
            redpanda = self.redpanda

        def migration_id_if_exists():
            for n in redpanda.nodes:
                for m in redpanda._admin.list_data_migrations(n).json():
                    if m == migration:
                        return m[id]
            return None

        time_before_creation = now()
        try:
            reply = redpanda._admin.create_data_migration(migration).json()
            redpanda.logger.info(f"create migration reply: {reply}")
            migration_id = reply["id"]
        except requests.exceptions.HTTPError as e:
            maybe_id = migration_id_if_exists()
            if maybe_id is None:
                raise
            migration_id = maybe_id
            redpanda.logger.info(f"create migration failed "
                                 f"but migration {migration_id} present: {e}")

        self.wait_migration_appear(migration_id,
                                   time_before_creation,
                                   redpanda=redpanda)

        return migration_id

    def assure_not_deletable(self,
                             id,
                             node=None,
                             redpanda: RedpandaService | None = None):
        if redpanda is None:
            redpanda = self.redpanda

        try:
            redpanda._admin.delete_data_migration(id, node)
            assert False
        except requests.exceptions.HTTPError:
            pass

    def wait_for_migration_states(self,
                                  id: int,
                                  states: list[str],
                                  assure_completed_after: int = 0,
                                  redpanda: RedpandaService | None = None):
        if redpanda is None:
            redpanda = self.redpanda

        def migration_in_one_of_states_on_node(m):
            if m is None:
                return False
            completed_at = m.get("completed_timestamp")
            if m["state"] in ("finished", "cancelled"):
                self.validate_timing(assure_completed_after, completed_at)
            else:
                assert "completed_timestamp" not in m
            return m["state"] in states

        def migration_in_one_of_states():
            return self.on_all_live_nodes(id,
                                          migration_in_one_of_states_on_node,
                                          redpanda=redpanda)

        self.logger.info(f'waiting for {" or ".join(states)}')
        wait_until(
            migration_in_one_of_states,
            timeout_sec=90,
            backoff_sec=1,
            err_msg=
            f"Failed waiting for migration {id} to reach one of {states} states"
        )
        if all(state not in ('planned', 'finished', 'cancelled')
               for state in states):
            self.assure_not_deletable(id, redpanda=redpanda)

    def migrate_between_clusters(self, topics: list[NamespacedTopic],
                                 source: RedpandaService,
                                 dest: RedpandaService) -> None:
        assert source != dest

        out_migration = OutboundDataMigration(topics=topics,
                                              consumer_groups=[])

        out_migration_id = self.create_and_wait(out_migration, redpanda=source)
        source.logger.info(
            f"created outbound migration, id {out_migration_id}")

        # TODO: Outbound migrations should provide a better way of determining
        # location hints for migrated topic instances. Currently if we don't know the
        # originating cluster UUID, we can't determine the location hint at all.
        # Here we assume that the topic was first created on self.redpanda.
        # Getting the remote revision by calling an unrelated API is inefficient
        # and awkward as well.
        orig_cluster_uuid = self.redpanda._admin.get_cluster_uuid()
        in_topics = []
        for topic in topics:
            anomalies = source._admin.get_cloud_storage_anomalies(
                namespace="kafka", topic=topic.topic, partition=0)
            remote_revision_id = anomalies["revision_id"]

            in_topics.append(
                InboundTopic(topic,
                             cluster_uuid=orig_cluster_uuid,
                             remote_revision=remote_revision_id))

        in_migration = InboundDataMigration(topics=in_topics,
                                            consumer_groups=[])
        in_migration_id = self.create_and_wait(in_migration, redpanda=dest)
        dest.logger.info(f"created inbound migration, id {in_migration_id}")

        source._admin.execute_data_migration_action(out_migration_id,
                                                    MigrationAction.prepare)
        self.wait_for_migration_states(out_migration_id, ['prepared'],
                                       redpanda=source)
        self.logger.info(f"prepared on source")

        source._admin.execute_data_migration_action(out_migration_id,
                                                    MigrationAction.execute)
        self.wait_for_migration_states(out_migration_id, ['executed'],
                                       redpanda=source)
        self.logger.info(f"executed on source")

        source._admin.execute_data_migration_action(out_migration_id,
                                                    MigrationAction.finish)
        self.wait_for_migration_states(out_migration_id, ['finished'],
                                       redpanda=source)
        self.logger.info(f"finished on source")

        # TODO: currently migrations need to be executed sequentially (on source
        # then on destination). Ideally the implementation should allow for concurrent
        # execution (i.e. we could start some preparations on destination while
        # source migration is still being executed).

        dest._admin.execute_data_migration_action(in_migration_id,
                                                  MigrationAction.prepare)
        self.wait_for_migration_states(in_migration_id, ['prepared'],
                                       redpanda=dest)
        self.logger.info(f"prepared on dest")

        dest._admin.execute_data_migration_action(in_migration_id,
                                                  MigrationAction.execute)
        self.wait_for_migration_states(in_migration_id, ['executed'],
                                       redpanda=dest)
        self.logger.info(f"executed on dest")

        dest._admin.execute_data_migration_action(in_migration_id,
                                                  MigrationAction.finish)
        self.wait_for_migration_states(in_migration_id, ['finished'],
                                       redpanda=dest)
        self.logger.info(f"finished on dest")
