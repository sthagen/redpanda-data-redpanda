from __future__ import annotations

from typing import Any

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.error import KafkaException
from ducktape.tests.test import Test, TestContext
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import security_pb2
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.python_librdkafka import PythonLibrdkafka
from confluent_kafka.admin import (
    AclBinding,
    AclOperation,
    AclPermissionType,
    ResourcePatternType,
    ResourceType,
)
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    LoggingConfig,
    SecurityConfig,
    make_redpanda_service,
)
from rptest.services.stub_oidc_provider import StubOIDCProvider


class StubOIDCTestBase(Test):
    """Base class for GBAC tests using StubOIDCProvider instead of Keycloak."""

    def __init__(
        self, test_context: TestContext, num_nodes: int = 4, **kwargs: Any
    ) -> None:
        super().__init__(test_context, **kwargs)
        num_brokers = num_nodes - 1
        self.stub_idp = StubOIDCProvider(test_context)

        security = SecurityConfig()
        security.enable_sasl = True
        security.sasl_mechanisms = ["SCRAM", "OAUTHBEARER"]
        security.http_authentication = ["BASIC", "OIDC"]

        stub_node = self.stub_idp.nodes[0]

        self.redpanda = make_redpanda_service(
            test_context,
            num_brokers,
            extra_rp_conf={
                "oidc_discovery_url": self.stub_idp.get_discovery_url(stub_node),
                "oidc_token_audience": "redpanda",
            },
            security=security,
            log_config=LoggingConfig(
                "info",
                logger_levels={
                    "security": "trace",
                    "kafka/client": "trace",
                    "kafka": "debug",
                },
            ),
        )

        self.su_username, self.su_password, self.su_algorithm = (
            self.redpanda.SUPERUSER_CREDENTIALS
        )

        self.rpk = RpkTool(
            self.redpanda,
            username=self.su_username,
            password=self.su_password,
            sasl_mechanism=self.su_algorithm,
        )

    def setUp(self) -> None:
        self.stub_idp.start()
        self.stub_idp.wait_ready()
        self.redpanda.start()

    def make_producer(
        self, client_id: str, client_secret: str = "stub-secret"
    ) -> Producer:
        """Create a Kafka producer authenticated via the stub OIDC provider."""
        stub_node = self.stub_idp.nodes[0]
        cfg = self.stub_idp.generate_oauth_config(stub_node, client_id, client_secret)
        k_client = PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
        )
        producer = k_client.get_producer()
        producer.poll(0.0)
        return producer

    def _try_produce(self, producer: Producer, topic: str) -> KafkaError | None:
        """Produce a message and return the delivery error, or None on success."""
        errors: list[KafkaError | None] = []

        def _on_delivery(err: KafkaError | None, _msg: Message) -> None:
            errors.append(err)

        producer.produce(
            topic,
            b"test",
            on_delivery=_on_delivery,
        )
        producer.flush(timeout=10)
        assert len(errors) > 0, "Expected delivery callback but got none"
        return errors[0]

    def wait_until_produce_succeeds(
        self, producer: Producer, topic: str, err_msg: str
    ) -> None:
        """Retry producing until it succeeds (ACL propagation may be delayed)."""
        wait_until(
            lambda: self._try_produce(producer, topic) is None,
            timeout_sec=10,
            backoff_sec=1,
            err_msg=err_msg,
        )

    def assert_produce_denied(self, producer: Producer, topic: str) -> None:
        """Assert that producing to the topic fails with TOPIC_AUTHORIZATION_FAILED."""
        err = self._try_produce(producer, topic)
        assert err is not None, (
            f"Expected produce to {topic} to be denied, but it succeeded"
        )
        assert err.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED, (
            f"Expected TOPIC_AUTHORIZATION_FAILED, got {err}"
        )

    def resolve_oidc_identity(
        self, client_id: str, client_secret: str = "stub-secret"
    ) -> security_pb2.ResolveOidcIdentityResponse:
        """Get a token and resolve the OIDC identity via Admin API v2."""
        token = self.stub_idp.get_access_token(client_id, client_secret)
        admin_v2 = AdminV2(self.redpanda)
        req = security_pb2.ResolveOidcIdentityRequest()
        return admin_v2.security().resolve_oidc_identity(
            req,
            extra_headers={"Authorization": f"Bearer {token}"},
        )


class GbacGroupClaimFormatTest(StubOIDCTestBase):
    """Tests for various group claim formats in OIDC tokens."""

    @cluster(num_nodes=4)
    def test_json_array_groups(self) -> None:
        """Groups as JSON array. Each element becomes a group."""
        client_id = "array-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "array-user",
                "groups": ["eng", "fin"],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == ["eng", "fin"]

        topic = "array-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "JSON array group should grant topic access",
        )

    @cluster(num_nodes=4)
    def test_csv_groups(self) -> None:
        """Groups as CSV string. Split into individual groups."""
        client_id = "csv-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "csv-user",
                "groups": "eng,fin",
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == ["eng", "fin"]

        topic = "csv-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "CSV group string should grant topic access",
        )

    @cluster(num_nodes=4)
    def test_csv_groups_with_whitespace(self) -> None:
        """CSV string with whitespace around entries. Whitespace is
        trimmed."""
        client_id = "csv-ws-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "csv-ws-user",
                "groups": "eng , fin",
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == ["eng", "fin"]

        topic = "csv-ws-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "CSV with whitespace should be trimmed and grant access",
        )

    @cluster(num_nodes=4)
    def test_csv_groups_with_empty_entries(self) -> None:
        """CSV string with consecutive commas. Empty entries produce empty
        group strings, but valid groups still match."""
        client_id = "csv-empty-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "csv-empty-user",
                "groups": "eng,,,fin",
            },
        )

        expected_groups = ["", "", "eng", "fin"]
        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == expected_groups

        topic = "csv-empty-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "CSV with empty entries should still parse valid groups",
        )

    @cluster(num_nodes=4)
    def test_empty_array_groups(self) -> None:
        """Empty group array. No groups extracted."""
        client_id = "empty-array-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "empty-user",
                "groups": [],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 0

        topic = "empty-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_very_long_group_name(self) -> None:
        """Very long group name (1000+ chars)."""
        long_group = "a" * 1000
        client_id = "long-name-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "long-user",
                "groups": [long_group],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == [long_group]

        topic = "long-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(f"Group:{long_group}", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "Very long group name should work",
        )

    @cluster(num_nodes=4)
    def test_groups_as_objects(self) -> None:
        """Groups as array of objects. Entire group claim discarded."""
        client_id = "obj-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "obj-user",
                "groups": [{"name": "eng"}],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 0

        topic = "obj-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)


class GbacGroupClaimPathTest(StubOIDCTestBase):
    """Tests for group claim path resolution with various oidc_group_claim_path configs."""

    @cluster(num_nodes=4)
    def test_nested_claim_path(self) -> None:
        """Nested claim path set. Groups extracted successfully."""
        self.redpanda.set_cluster_config(
            {
                "oidc_group_claim_path": "$.realm_access.groups",
            }
        )

        client_id = "nested-path-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "nested-user",
                "realm_access": {"groups": ["admin"]},
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == ["admin"]

        topic = "nested-path-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:admin", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "Nested claim path should extract groups",
        )

    @cluster(num_nodes=4)
    def test_claim_path_missing(self) -> None:
        """Claim path missing from token. No groups extracted."""
        client_id = "no-groups-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "no-groups-user",
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 0

        topic = "no-groups-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)


class GbacMalformedGroupClaimTest(StubOIDCTestBase):
    """Tests for invalid or malformed group claims (fail-safe behavior)."""

    @cluster(num_nodes=4)
    def test_arbitrary_string(self) -> None:
        """Groups as non-CSV string with semicolon. Treated as single
        literal group, not split."""
        client_id = "arb-str-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "arb-str-user",
                "groups": "eng;fin",
            },
        )

        # Not CSV (no comma), so treated as single group "eng;fin".
        resp = self.resolve_oidc_identity(client_id)
        assert "eng" not in resp.groups

        topic = "arb-str-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

        topic = "arb-str-topic2"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng;fin", ["all"], "topic", topic)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "Group name with semicolon should match literally",
        )

    @cluster(num_nodes=4)
    def test_groups_as_number(self) -> None:
        """Groups claim is a number. No groups extracted."""
        client_id = "num-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "num-user",
                "groups": 42,
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 0

        topic = "num-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:42", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_groups_as_object(self) -> None:
        """Groups claim is an object. No groups extracted."""
        client_id = "obj-bad-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "obj-bad-user",
                "groups": {"key": "val"},
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 0

        topic = "obj-bad-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:val", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_mixed_type_array(self) -> None:
        """Mixed-type group array with strings, numbers, and nulls. Entire
        array rejected."""
        client_id = "mixed-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "mixed-user",
                "groups": ["eng", 42, None],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 0

        topic = "mixed-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_very_large_group_list(self) -> None:
        """Very large group list (1050 groups). Stable parsing, access
        granted for matching group."""
        groups = [f"g{i}" for i in range(1050)]
        client_id = "large-list-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "large-user",
                "groups": groups,
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 1050
        assert "g500" in resp.groups

        topic = "large-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:g500", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "Large group list should still grant access for matching group",
        )


class GbacGroupNameEdgeCaseTest(StubOIDCTestBase):
    """Tests for group name edge cases (case, unicode, special chars, etc)."""

    @cluster(num_nodes=4)
    def test_case_mismatch(self) -> None:
        """Case mismatch between token group and ACL. Exact match required."""
        client_id = "case-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "case-user",
                "groups": ["Eng"],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == ["Eng"]

        topic = "case-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_unicode_group_name(self) -> None:
        """Unicode group name supported."""
        group = "ingeniería"
        client_id = "unicode-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "unicode-user",
                "groups": [group],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == [group]

        topic = "unicode-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(f"Group:{group}", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "Unicode group name should match correctly",
        )

    @cluster(num_nodes=4)
    def test_special_characters(self) -> None:
        """Special characters in group name supported."""
        group = "eng@dev#1"
        client_id = "special-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "special-user",
                "groups": [group],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == [group]

        topic = "special-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(f"Group:{group}", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "Special character group name should match correctly",
        )

    @cluster(num_nodes=4)
    def test_comma_in_group_name_array(self) -> None:
        """Group name with comma in JSON array form. Comma preserved as
        part of group name."""
        group = "eng,fin"
        client_id = "comma-array-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "comma-array-user",
                "groups": [group],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == [group]

        topic = "comma-array-topic"
        self.rpk.create_topic(topic)
        # rpk's --allow-principal flag treats commas as delimiters between
        # multiple principals. Without quoting, "Group:eng,fin" would be
        # split into two ACL entries: Group:eng and User:fin (the second
        # defaulting to User type since it lacks a prefix). This is
        # rpk-specific behavior — the underlying Kafka protocol handles
        # commas in principal names without issue. CSV-style double quotes
        # prevent rpk from splitting on the comma.
        self.rpk.sasl_allow_principal(f'"Group:{group}"', ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "Comma in group name (array form) should be supported",
        )

    @cluster(num_nodes=4)
    def test_newline_tab_in_group_name(self) -> None:
        """Group names with newline/tab characters. Parsed without crashing,
        but access denied because the broker rejects ACL creation for
        principals containing control characters."""
        nl_group = "eng\nfin"
        tab_group = "admin\tstaff"
        client_id = "ctrl-char-test"
        groups = [nl_group, tab_group]
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "ctrl-char-user",
                "groups": groups,
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == sorted(groups), (
            f"Expected groups {groups}, got {list(resp.groups)}"
        )

        # The broker rejects ACL creation for principals with control characters,
        # so no matching ACL can exist and access is denied. Instead, create an
        # ACL for the literal group name to verify it doesn't match the control chars
        # and grant access.
        topic = "ctrl-char-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_empty_string_group(self) -> None:
        """Empty string group. Passed through as-is, but access denied
        because the broker rejects ACL creation for empty principal names."""
        client_id = "empty-str-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "empty-str-user",
                "groups": [""],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert resp.groups == [""]

        topic = "empty-str-topic"
        self.rpk.create_topic(topic)

        # Use the Kafka API directly to verify the broker rejects an ACL with
        # an empty principal name. Using Kafka API instead of rpk because rpk
        # silently exits 0 for this case, but the Kafka protocol returns INVALID_REQUEST.
        su_client = PythonLibrdkafka(
            self.redpanda,
            username=self.su_username,
            password=self.su_password,
            algorithm=self.su_algorithm,
        )
        admin = su_client.get_client()
        binding = AclBinding(
            restype=ResourceType.TOPIC,
            name=topic,
            resource_pattern_type=ResourcePatternType.LITERAL,
            principal="Group:",
            host="*",
            operation=AclOperation.ALL,
            permission_type=AclPermissionType.ALLOW,
        )
        results = admin.create_acls([binding])  # type: ignore[reportUnknownMemberType]
        for _, fut in results.items():
            try:
                fut.result()
                assert False, "Expected KafkaException for empty principal name"
            except KafkaException as e:
                assert "INVALID_REQUEST" in str(e)

        # No ACL was created, so produce is denied.
        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_duplicate_groups(self) -> None:
        """Duplicate groups in array are not deduplicated."""
        client_id = "dup-test"
        groups = ["admin", "admin"]
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "dup-user",
                "groups": groups,
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == groups

        allowed_topic = "allowed-topic"
        self.rpk.create_topic(allowed_topic)
        self.rpk.sasl_allow_principal("Group:admin", ["all"], "topic", allowed_topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            allowed_topic,
            "Produce should succeed.",
        )


class GbacNestedGroupTest(StubOIDCTestBase):
    """Tests for nested_group_behavior config (none vs suffix modes)."""

    @cluster(num_nodes=4)
    def test_none_mode_full_path_match(self) -> None:
        """Nested path with nested_group_behavior set to none. Full path is the literal group name."""
        self.redpanda.set_cluster_config({"nested_group_behavior": "none"})

        client_id = "none-full-path-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "none-full-path-user",
                "groups": ["a/b/c"],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == ["a/b/c"]

        topic = "none-full-path-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:a/b/c", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "Full path group 'a/b/c' should match ACL on Group:a/b/c in none mode",
        )

    @cluster(num_nodes=4)
    def test_none_mode_suffix_no_match(self) -> None:
        """Nested path with nested_group_behavior set to none. ACL on suffix only.
        Suffix extraction does not happen."""
        self.redpanda.set_cluster_config({"nested_group_behavior": "none"})

        client_id = "none-suffix-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "none-suffix-user",
                "groups": ["a/b/c"],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == ["a/b/c"]

        topic = "none-suffix-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:c", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_suffix_mode_extracts_last_segment(self) -> None:
        """Nested path with nested_group_behavior set to suffix. Last segment is extracted."""
        self.redpanda.set_cluster_config({"nested_group_behavior": "suffix"})

        client_id = "sfx-extract-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "sfx-extract-user",
                "groups": ["a/b/c"],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == ["c"]

        topic = "sfx-extract-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:c", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "Suffix mode should extract last segment 'c' from 'a/b/c' and grant access",
        )

    @cluster(num_nodes=4)
    def test_suffix_mode_trailing_slash(self) -> None:
        """Trailing slash in suffix mode. Last segment is empty."""
        self.redpanda.set_cluster_config({"nested_group_behavior": "suffix"})

        client_id = "sfx-trailing-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "sfx-trailing-user",
                "groups": ["a/b/"],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == [""]

        topic = "sfx-trailing-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:b", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_suffix_mode_collision(self) -> None:
        """Multiple nested paths with same suffix in suffix mode. Both
        collapse to same group (duplicated)."""
        self.redpanda.set_cluster_config({"nested_group_behavior": "suffix"})

        client_id = "sfx-collide-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "sfx-collide-user",
                "groups": ["deptA/admin", "deptB/admin"],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == ["admin", "admin"]

        topic = "sfx-collide-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:admin", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "Suffix 'admin' from both 'deptA/admin' and 'deptB/admin' should grant access",
        )


class GbacMultiGroupTest(StubOIDCTestBase):
    """Tests for multiple group membership and ACL union behavior."""

    @cluster(num_nodes=4)
    def test_union_of_group_permissions(self) -> None:
        """Two groups with ACLs on different topics. Union of permissions
        grants access to both."""
        client_id = "union-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "union-user",
                "groups": ["eng", "fin"],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == ["eng", "fin"]

        topic_eng = "union-eng-topic"
        self.rpk.create_topic(topic_eng)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic_eng)

        topic_fin = "union-fin-topic"
        self.rpk.create_topic(topic_fin)
        self.rpk.sasl_allow_principal("Group:fin", ["all"], "topic", topic_fin)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic_eng,
            "eng group ACL should grant access to eng topic",
        )
        self.wait_until_produce_succeeds(
            producer,
            topic_fin,
            "fin group ACL should grant access to fin topic",
        )

    @cluster(num_nodes=4)
    def test_one_matching_group_grants_access(self) -> None:
        """Two groups, only one has an ACL. One matching group is sufficient
        for access."""
        client_id = "one-match-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "one-match-user",
                "groups": ["eng", "noacl"],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == ["eng", "noacl"]

        topic = "one-match-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer,
            topic,
            "One matching group should be sufficient for access",
        )

    @cluster(num_nodes=4)
    def test_group_without_acl_no_access(self) -> None:
        """Group with no matching ACL. Access denied."""
        client_id = "no-acl-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "no-acl-user",
                "groups": ["noacl"],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == ["noacl"]

        topic = "no-acl-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["all"], "topic", topic)

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_wildcard_group_acl_rejected(self) -> None:
        """Wildcard Group:* ACL creation. Rejected by broker, only User:*
        wildcards are allowed."""
        client_id = "wildcard-test"
        self.stub_idp.register_client(
            client_id,
            claims={
                "sub": "wildcard-user",
                "groups": ["eng"],
            },
        )

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == ["eng"]

        topic = "wildcard-topic"
        self.rpk.create_topic(topic)

        # Use the Kafka admin API directly to verify the broker rejects
        # Group:* ACL creation. rpk does not surface this error clearly.
        su_client = PythonLibrdkafka(
            self.redpanda,
            username=self.su_username,
            password=self.su_password,
            algorithm=self.su_algorithm,
        )
        admin = su_client.get_client()
        binding = AclBinding(
            restype=ResourceType.TOPIC,
            name=topic,
            resource_pattern_type=ResourcePatternType.LITERAL,
            principal="Group:*",
            host="*",
            operation=AclOperation.ALL,
            permission_type=AclPermissionType.ALLOW,
        )
        results = admin.create_acls([binding])  # type: ignore[reportUnknownMemberType]
        for _, fut in results.items():
            try:
                fut.result()
                assert False, "Expected KafkaException for wildcard Group:*"
            except KafkaException as e:
                assert "INVALID_REQUEST" in str(e)

        # No ACL was created, so produce is denied.
        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)
