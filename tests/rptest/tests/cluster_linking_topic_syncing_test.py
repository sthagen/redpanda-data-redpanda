# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.admin.proto.redpanda.core.admin.v2 import shadow_link_pb2
from rptest.clients.admin.proto.redpanda.core.common.v1 import tls_pb2
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.multi_cluster_services import SecondaryClusterArgs
from rptest.services.redpanda import (
    LoggingConfig,
    SchemaRegistryConfig,
    SecurityConfig,
    TLSProvider,
)
from rptest.services.tls import CertificateAuthority, Certificate, TLSCertManager
from rptest.tests.cluster_linking_test_base import (
    ClusterLinkingTLSProvider,
    ShadowLinkTestBase,
)
from rptest.tests.schema_registry_test import SchemaRegistryRedpandaClient
from rptest.util import expect_exception, wait_until

import ducktape.errors
import google.protobuf.field_mask_pb2
import json
import re
import socket


class ClusterLinkingTopicSyncingTestBase(ShadowLinkTestBase):
    """
    Base test class that will verify that topics can be synced from the target
    cluster to the source cluster and that properties are properly synced
    """

    def __init__(self, test_context, *args, **kwargs):
        self.default_topic_replication = 1
        extra_rp_conf = {"default_topic_replications": self.default_topic_replication}
        if "extra_rp_conf" in kwargs:
            extra_rp_conf.update(kwargs["extra_rp_conf"])
            kwargs.pop("extra_rp_conf")

        super().__init__(
            test_context,
            extra_rp_conf=extra_rp_conf,
            *args,
            **kwargs,
        )

    def validate_created_link(self, shadow_link: shadow_link_pb2.ShadowLink) -> None:
        pass

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        return shadow_link

    def create_default_link_request(
        self, link_name: str, *args, **kwargs
    ) -> shadow_link_pb2.CreateShadowLinkRequest:
        req = super().create_default_link_request(link_name=link_name, *args, **kwargs)
        req.shadow_link.CopyFrom(self.add_credentials_to_link(req.shadow_link))
        return req

    def create_link(
        self, link_name: str, *args, **kwargs
    ) -> shadow_link_pb2.ShadowLink:
        req = self.create_default_link_request(link_name=link_name, *args, **kwargs)
        req.shadow_link.CopyFrom(self.add_credentials_to_link(req.shadow_link))
        return self.create_link_with_request(req=req)

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(self.source_cluster.service)

    def get_target_cluster_rpk(self) -> RpkTool:
        return RpkTool(self.target_cluster.service)

    def _topics_are_present(
        self,
        rpk: RpkTool,
        topics_to_expect: list[TopicSpec],
        check_for_validity: bool = False,
        topics_not_expected: list[str] = [],
    ):
        topics_in_cluster = list(rpk.list_topics(detailed=True))
        self.logger.info(f"topics_in_cluster: {topics_in_cluster}")

        def find_by_name(name: str, results: list[list[str]]) -> list[str] | None:
            for r in results:
                if r[0] == name:
                    return r
            return None

        for t in topics_to_expect:
            found = find_by_name(t.name, topics_in_cluster)
            if not found:
                return False
            self.logger.debug(f"Found topic {t.name} in cluster: {found}")
            if check_for_validity:
                if int(found[1]) != t.partition_count:
                    self.logger.info(
                        f"Topic {t.name} partition count {found[1]} does not match expected {t.partition_count}"
                    )
                    return False
                if int(found[2]) != t.replication_factor:
                    self.logger.info(
                        f"Topic {t.name} replication factor {found[2]} does not match expected {t.replication_factor}"
                    )
                    return False

        for t in topics_not_expected:
            if any(t == topic[0] for topic in topics_in_cluster):
                assert False, f"Topic {t} found in cluster but should not be"

        return True

    @cluster(num_nodes=6)
    def test_topic_syncing_include_exclude(self):
        """
        This test verifies that the filters work appropriately
        """
        exclude_topic_prefix = "include-topic-1"
        include_topic_prefix = "include-topic-"
        exclude_topic_specific = "include-topic-0"
        include_topic_specific = "include-this-topic"

        self.logger.info("Creating topics on source cluster")
        topics: list[TopicSpec] = []
        for i in range(11):
            # Create include-topic-{0-10}
            cleanup_policy = "delete" if i % 2 == 0 else "compact"
            topic = TopicSpec(
                name=f"{include_topic_prefix}{i}",
                partition_count=i + 3,
                replication_factor=3,
                cleanup_policy=cleanup_policy,
            )
            self.get_source_cluster_rpk().create_topic(
                topic=topic.name,
                partitions=topic.partition_count,
                replicas=topic.replication_factor,
                config={"cleanup.policy": topic.cleanup_policy},
            )
            topics.append(topic)

        include_specific = TopicSpec(
            name=include_topic_specific,
            partition_count=3,
            replication_factor=3,
            cleanup_policy="delete",
        )

        self.get_source_cluster_rpk().create_topic(
            topic=include_specific.name,
            partitions=include_specific.partition_count,
            replicas=include_specific.replication_factor,
            config={"cleanup.policy": include_specific.cleanup_policy},
        )
        topics.append(include_specific)

        wait_until(
            lambda: self._topics_are_present(
                self.get_source_cluster_rpk(), topics, check_for_validity=True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Not all topics created on source cluster",
        )

        shadow_link_req = self.create_default_link_request(
            "test-link", mirror_all_topics=False, mirror_all_groups=False
        )

        shadow_link_req.shadow_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters.extend(
            [
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name=include_topic_prefix,
                ),
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                    filter_type=shadow_link_pb2.FILTER_TYPE_EXCLUDE,
                    name=exclude_topic_prefix,
                ),
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name=include_topic_specific,
                ),
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                    filter_type=shadow_link_pb2.FILTER_TYPE_EXCLUDE,
                    name=exclude_topic_specific,
                ),
            ]
        )

        self.logger.info(f"Creating shadow link with request: {shadow_link_req}")
        created_link = self.create_link_with_request(req=shadow_link_req)
        self.validate_created_link(created_link)

        self.logger.info("Verifying topics synced to target cluster")
        expected_topics = [
            t
            for t in topics
            if t.name.startswith(include_topic_prefix)
            and not t.name.startswith(exclude_topic_prefix)
            and not t.name == exclude_topic_specific
        ]
        expected_topics.append(include_specific)

        for t in expected_topics:
            t.replication_factor = 3

        self.logger.info(
            f"expected_topics: {[{'name': t.name, 'partition_count': t.partition_count, 'replication_factor': t.replication_factor} for t in expected_topics]}"
        )
        unexpected_topics = [
            t.name
            for t in topics
            if t.name.startswith(exclude_topic_prefix)
            or t.name == exclude_topic_specific
        ]
        self.logger.info(f"unexpected_topics: {unexpected_topics}")

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), expected_topics, True, unexpected_topics
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Not all topics created on target cluster",
        )

        mirror_topics = self.list_shadow_topics(shadow_link_name="test-link")
        assert len(mirror_topics) == len(expected_topics), (
            f"Expected {len(expected_topics)} topics in mirror but found {len(mirror_topics)}: {mirror_topics}"
        )

    @cluster(num_nodes=6)
    def test_topic_partition_count_sync(self):
        topic_name = "test-topic"

        topic = TopicSpec(name=topic_name, partition_count=3, replication_factor=1)

        self.get_source_cluster_rpk().create_topic(
            topic=topic.name,
            partitions=topic.partition_count,
            replicas=topic.replication_factor,
        )

        created_link = self.create_link("test-link")
        self.validate_created_link(created_link)

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), [topic], True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Topic not created on target cluster",
        )

        self.logger.info("Increasing partition count on source topic")

        self.get_source_cluster_rpk().add_partitions(topic_name, 3)

        topic.partition_count = 6

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), [topic], True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Topic partition count not updated",
        )

    @cluster(num_nodes=6)
    def test_topic_properties_sync(self):
        topic_name = "test-topic"

        topic = TopicSpec(name=topic_name, partition_count=3, replication_factor=1)

        self.get_source_cluster_rpk().create_topic(
            topic=topic.name,
            partitions=topic.partition_count,
            replicas=topic.replication_factor,
        )

        shadow_link_req = self.create_default_link_request("test-link")

        shadow_link_req.shadow_link.configurations.topic_metadata_sync_options.synced_shadow_topic_properties.append(
            "replication.factor"
        )

        created_link = self.create_link_with_request(req=shadow_link_req)
        self.validate_created_link(created_link)

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), [topic], True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Topic not created on target cluster",
        )

        self.get_source_cluster_rpk().alter_topic_config(
            topic_name, "replication.factor", 3
        )

        topic.replication_factor = 3

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), [topic], True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Replication factor never updated",
        )


class ClusterLinkingTopicSyncingTestNoSecurity(ClusterLinkingTopicSyncingTestBase):
    """
    Runs the base test with no security settings
    """

    pass


class ClusterLinkingTopicSyncingWithScram(ClusterLinkingTopicSyncingTestBase):
    """
    Run the same battery of tests as ClusterLinkingTopicSyncingTestBase but with SCRAM enabled
    """

    def __init__(self, test_context, *args, **kwargs):
        security = SecurityConfig()
        security.enable_sasl = True
        secondary_args: SecondaryClusterArgs = SecondaryClusterArgs(security=security)
        self.cluster_link_user = "cluster-link-user"
        self.cluster_link_password = "cluster-link-password"
        self.cluster_link_mechanism = shadow_link_pb2.SCRAM_MECHANISM_SCRAM_SHA_256

        super().__init__(
            test_context=test_context,
            secondary_cluster_args=secondary_args,
            *args,
            **kwargs,
        )

    def validate_created_link(self, shadow_link: shadow_link_pb2.ShadowLink) -> None:
        assert (
            shadow_link.configurations.client_options.authentication_configuration.WhichOneof(
                "authentication"
            )
            == "scram_configuration"
        ), (
            f"Expected 'scram_configuration' but got {shadow_link.configurations.client_options.authentication_configuration.WhichOneof('authentication')}"
        )

        scram_config = shadow_link.configurations.client_options.authentication_configuration.scram_configuration
        assert scram_config.password_set, "Password not set in scram configuration"

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        self.logger.debug(
            f"Adding credentials for user {self.cluster_link_user} to link"
        )
        shadow_link.configurations.client_options.authentication_configuration.scram_configuration.CopyFrom(
            shadow_link_pb2.ScramConfig(
                username=self.cluster_link_user,
                password=self.cluster_link_password,
                scram_mechanism=self.cluster_link_mechanism,
            )
        )
        return shadow_link

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.source_cluster.service,
            username=self.redpanda.SUPERUSER_CREDENTIALS.username,
            password=self.redpanda.SUPERUSER_CREDENTIALS.password,
            sasl_mechanism=self.redpanda.SUPERUSER_CREDENTIALS.mechanism,
        )

    def setUp(self):
        super().setUp()
        self.get_source_cluster_rpk().sasl_create_user(
            self.cluster_link_user, self.cluster_link_password
        )
        self.source_cluster.service.set_cluster_config(
            {
                "superusers": [
                    self.redpanda.SUPERUSER_CREDENTIALS.username,
                    self.cluster_link_user,
                ]
            }
        )


class ClusterLinkingTopicSyncingWithTlsFiles(ClusterLinkingTopicSyncingTestBase):
    """
    Runs the base tests with TLS enabled on both endpoints
    """

    def __init__(self, test_context, *args, **kwargs):
        self.test_context = test_context
        self.security = SecurityConfig()
        self.tls = TLSCertManager(self.logger)
        self.security.tls_provider = ClusterLinkingTLSProvider(self.tls)
        self.security.require_client_auth = False

        super().__init__(
            test_context=self.test_context,
            secondary_cluster_args=SecondaryClusterArgs(security=self.security),
            security=self.security,
            *args,
            **kwargs,
        )

    def validate_created_link(self, shadow_link: shadow_link_pb2.ShadowLink) -> None:
        assert (
            shadow_link.configurations.client_options.tls_settings.WhichOneof(
                "tls_settings"
            )
            == "tls_file_settings"
        ), (
            f"Expected 'tls_file_settings' but got {shadow_link.configurations.client_options.tls_settings.WhichOneof('tls_settings')}"
        )

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        self.logger.debug("Adding TLS files to link")

        shadow_link.configurations.client_options.tls_settings.CopyFrom(
            tls_pb2.TLSSettings(
                enabled=True,
                tls_file_settings=tls_pb2.TLSFileSettings(
                    ca_path=self.redpanda.TLS_CA_CRT_FILE,
                    key_path=self.redpanda.TLS_SERVER_KEY_FILE,
                    cert_path=self.redpanda.TLS_SERVER_CRT_FILE,
                ),
            )
        )

        return shadow_link

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.source_cluster.service, tls_cert=self.tls.create_cert("source-rpk")
        )

    def get_target_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.target_cluster.service, tls_cert=self.tls.create_cert("target-rpk")
        )


class ClusterLinkingTopicSyncingWithTlsValues(ClusterLinkingTopicSyncingTestBase):
    """
    Runs the base tests with TLS enabled on both endpoints and using TLS values rather than files
    """

    def __init__(self, test_context, *args, **kwargs):
        self.test_context = test_context
        self.security = SecurityConfig()
        self.tls = TLSCertManager(self.logger)
        self.security.tls_provider = ClusterLinkingTLSProvider(self.tls)
        self.security.require_client_auth = False
        self.client_cert: Certificate | None = None

        super().__init__(
            test_context=self.test_context,
            secondary_cluster_args=SecondaryClusterArgs(security=self.security),
            security=self.security,
            *args,
            **kwargs,
        )

    def validate_created_link(self, shadow_link: shadow_link_pb2.ShadowLink) -> None:
        assert (
            shadow_link.configurations.client_options.tls_settings.WhichOneof(
                "tls_settings"
            )
            == "tls_pem_settings"
        ), (
            f"Expected 'tls_pem_settings' but got {shadow_link.configurations.client_options.tls_settings.WhichOneof('tls_settings')}"
        )

        # TODO: Add key fingerprint

    def setUp(self):
        super().setUp()
        self.client_cert = self.tls.create_cert("cluster-link-client")

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        self.logger.debug("Adding TLS values to link")

        assert self.client_cert is not None

        ca_content = open(self.client_cert.ca.crt, "r").read()
        self.logger.debug(f"ca: {ca_content}")
        cert_content = open(self.client_cert.crt, "r").read()
        self.logger.debug(f"cert: {cert_content}")
        key_content = open(self.client_cert.key, "r").read()
        self.logger.debug(f"key: {key_content}")

        shadow_link.configurations.client_options.tls_settings.CopyFrom(
            tls_pb2.TLSSettings(
                enabled=True,
                tls_pem_settings=tls_pb2.TLSPEMSettings(
                    ca=ca_content, key=key_content, cert=cert_content
                ),
            )
        )

        return shadow_link

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.source_cluster.service, tls_cert=self.tls.create_cert("source-rpk")
        )

    def get_target_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.target_cluster.service, tls_cert=self.tls.create_cert("target-rpk")
        )


class ClusterLinkingSchemaRegistry(ShadowLinkTestBase):
    """
    These tests verify the behavior of syncing schema registry
    """

    simple_proto_def = """
syntax = "proto3";

message Simple {
  string id = 1;
}"""

    simple_a_proto_def = """
syntax = "proto3";

message AType {
  float f = 1;
}"""

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context=test_context,
            schema_registry_config=SchemaRegistryConfig(),
            secondary_cluster_args=SecondaryClusterArgs(
                schema_registry_config=SchemaRegistryConfig()
            ),
            log_config=LoggingConfig(
                "info",
                logger_levels={
                    "cluster_link": "trace",
                    "kafka/client": "trace",
                    "kafka": "trace",
                    "schemaregistry": "trace",
                    "schemaregistry/requests": "trace",
                    "shadow_link_service": "trace",
                },
            ),
        )

    def source_sr_client(self) -> SchemaRegistryRedpandaClient:
        return SchemaRegistryRedpandaClient(self.source_cluster_service)

    def target_sr_client(self) -> SchemaRegistryRedpandaClient:
        return SchemaRegistryRedpandaClient(self.target_cluster_service)

    def post_schema_to_subject(
        self, sr_client: SchemaRegistryRedpandaClient, subject: str, schema: str
    ) -> int:
        result_raw = sr_client.post_subjects_subject_versions(
            subject=subject,
            data=json.dumps({"schema": schema, "schemaType": "PROTOBUF"}),
        )
        self.logger.debug(f"post_schema_to_subject result: {result_raw}")
        assert result_raw.status_code == 200, (
            f"Failed to post schema to subject {subject}: {result_raw.text}"
        )
        result = result_raw.json()
        assert "id" in result, f"No 'id' in response: {result}"
        return result["id"]

    def get_subjects(self, sr_client: SchemaRegistryRedpandaClient) -> list[str]:
        result_raw = sr_client.get_subjects()
        self.logger.debug(f"get_subjects result: {result_raw}")
        assert result_raw.status_code == 200, (
            f"Failed to get subjects: {result_raw.text}"
        )
        result = result_raw.json()
        assert isinstance(result, list), f"Expected list but got {result}"
        return result

    @cluster(num_nodes=6)
    def test_schema_registry_basic(self):
        """
        This test will verify that the _schemas topic is not created
        until the link has been updated to enable mirroring schema linking
        and then will verify that schemas are replicated from the source to
        the shadow cluster
        """
        topics = [t for t in self.target_cluster_rpk.list_topics()]
        assert "_schemas" not in topics, (
            f"_schemas found in target cluster before link creation: {topics}"
        )

        # Populate source cluster schema registry
        source_sr_client = self.source_sr_client()

        first_id = self.post_schema_to_subject(
            source_sr_client, "first", self.simple_proto_def
        )
        self.logger.debug(f"First id: {first_id}")

        second_id = self.post_schema_to_subject(
            source_sr_client, "second", self.simple_a_proto_def
        )
        self.logger.debug(f"Second id: {second_id}")

        self.logger.info("Creating shadow link")
        created_link = self.create_link("test-link")

        self.logger.info(
            "Waiting 5 seconds and then verifying that _schemas topic is not on target cluster"
        )

        def schemas_in_target() -> bool:
            topics = [t for t in self.target_cluster_rpk.list_topics()]
            self.logger.debug(f"Topics in target cluster: {topics}")
            return "_schemas" in topics

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(schemas_in_target, timeout_sec=5, backoff_sec=1)

        self.logger.info(
            "Verify that the schemas topic gets created when we attempt to access the target schema registry"
        )
        target_sr_client = self.target_sr_client()
        subjects = self.get_subjects(target_sr_client)
        assert len(subjects) == 0, f"Expected no subjects but got {subjects}"
        assert schemas_in_target(), "_schemas topic not found in target cluster"

        self.logger.info("Enabling schema registry mirroring on the link")
        created_link.configurations.schema_registry_sync_options.shadow_schema_registry_topic.CopyFrom(
            shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryTopic()
        )
        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=["configurations.schema_registry_sync_options"]
        )
        updated_link = self.update_link(created_link, update_mask)
        assert updated_link.configurations.schema_registry_sync_options.shadow_schema_registry_topic, (
            "shadow_entire_schema_registry not set after update"
        )

        def subjects_match():
            source_subjects = self.get_subjects(source_sr_client)
            target_subjects = self.get_subjects(target_sr_client)
            self.logger.debug(
                f"source_subjects: {source_subjects}, target_subjects: {target_subjects}"
            )
            return set(source_subjects) == set(target_subjects)

        wait_until(
            subjects_match,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Subjects do not match",
        )

        # Verify we can query the shadow topic
        self.get_shadow_topic(
            shadow_link_name="test-link", shadow_topic_name="_schemas"
        )

    @cluster(
        num_nodes=6,
        log_allow_list=[
            re.compile(".*Schema registry failed to initialize.*"),
            re.compile(
                ".*Shadow Linking actively mirroring schema registry topic.  Topic will not be created.*"
            ),
        ],
    )
    def test_schema_registry_no_create(self):
        """
        This test will verify that the _schemas topic is not created when shadowing is enabled
        but no schemas topic is created on the source
        """
        create_link = self.create_default_link_request("test-link")
        create_link.shadow_link.configurations.schema_registry_sync_options.shadow_schema_registry_topic.CopyFrom(
            shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryTopic()
        )

        self.create_link_with_request(req=create_link)

        def schemas_in_target() -> bool:
            topics = [t for t in self.target_cluster_rpk.list_topics()]
            self.logger.debug(f"Topics in target cluster: {topics}")
            return "_schemas" in topics

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(schemas_in_target, timeout_sec=5, backoff_sec=1)

        self.logger.info("Verify that the schemas topic is not created on the source")
        result = self.target_sr_client().get_subjects()
        assert result.status_code == 500, f"Expected 500 but got {result.status_code}"

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(schemas_in_target, timeout_sec=5, backoff_sec=1)

        # Now create the topic and wait for subjects to show up

        # Populate source cluster schema registry
        source_sr_client = self.source_sr_client()

        first_id = self.post_schema_to_subject(
            source_sr_client, "first", self.simple_proto_def
        )
        self.logger.debug(f"First id: {first_id}")

        second_id = self.post_schema_to_subject(
            source_sr_client, "second", self.simple_a_proto_def
        )
        self.logger.debug(f"Second id: {second_id}")

        wait_until(
            schemas_in_target,
            timeout_sec=5,
            backoff_sec=1,
            err_msg="_schemas topic not created on target cluster",
        )

        target_sr_client = self.target_sr_client()

        def subjects_match():
            source_subjects = self.get_subjects(source_sr_client)
            target_subjects = self.get_subjects(target_sr_client)
            self.logger.debug(
                f"source_subjects: {source_subjects}, target_subjects: {target_subjects}"
            )
            return set(source_subjects) == set(target_subjects)

        wait_until(
            subjects_match,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Subjects do not match",
        )
