# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import socket
import tempfile

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkException, RpkTool
from rptest.services import tls
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import SchemaRegistryConfig, SecurityConfig
from rptest.tests.pandaproxy_test import PandaProxyTLSProvider, User
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception

AVRO = '{"type":"record","name":"r","fields":[{"name":"f1","type":"string"}]}'


class RpkPermissionTest(RedpandaTest):
    """Verify the Kafka ACLs `rpk topic produce`/`consume` require, as a NON-superuser
    principal, under authorization. Focus: the minimal grant for each path, and that the
    schema-context config lookup (via DescribeConfigs, added by the schema-context feature)
    needs the distinct DESCRIBE_CONFIGS ACL — but only when the context is derived from the
    topic config (i.e. --schema-context is NOT passed).

    Harness mirrors RpkRegistryTest: SASL/SCRAM Kafka with authorization enabled, Schema
    Registry with http_basic authn behind mTLS. Schema Registry ACL authorization is left at
    its default (off), so schema fetches are not subject-ACL gated and only the Kafka-side
    ACLs vary. A restricted user reuses the CA-signed client cert for the TLS transport and
    authenticates with its own SASL/basic-auth credentials.
    """

    password = "panda012345678"
    mechanism = "SCRAM-SHA-256"

    def __init__(self, ctx, schema_registry_config=SchemaRegistryConfig()):
        super().__init__(
            test_context=ctx,
            schema_registry_config=schema_registry_config,
            extra_rp_conf={
                "schema_registry_use_rpc": False,
                # the context test uses a qualified (:.ctx:subject) schema
                "schema_registry_enable_qualified_subjects": True,
            },
            node_ready_timeout_s=60,
        )
        self.security = SecurityConfig()
        su_user, su_pass, su_algo = self.redpanda.SUPERUSER_CREDENTIALS
        self.admin_user = User(0)
        self.admin_user.username = su_user
        self.admin_user.password = su_pass
        self.admin_user.algorithm = su_algo

        self.schema_registry_config = SchemaRegistryConfig()
        self.schema_registry_config.require_client_auth = True
        self.schema_registry_config.mode_mutability = True

    def setUp(self):
        tls_manager = tls.TLSCertManager(self.logger)
        self.security.require_client_auth = True
        self.security.kafka_enable_authorization = True
        self.security.endpoint_authn_method = "sasl"
        self.schema_registry_config.authn_method = "http_basic"

        self.admin_user.certificate = tls_manager.create_cert(
            socket.gethostname(),
            common_name=self.admin_user.username,
            name="test_admin_client",
        )
        self.security.tls_provider = PandaProxyTLSProvider(tls_manager)
        self.schema_registry_config.client_key = self.admin_user.certificate.key
        self.schema_registry_config.client_crt = self.admin_user.certificate.crt
        self.redpanda.set_security_settings(self.security)
        self.redpanda.set_schema_registry_settings(self.schema_registry_config)
        self.redpanda.start()

        # The CA-signed cert is reused by restricted users for the TLS transport; the
        # authenticated principal comes from each user's SASL/basic-auth credentials.
        self._cert = self.admin_user.certificate

        # Superuser client: creates topics, users, ACLs, and registers schemas.
        self._su = RpkTool(
            self.redpanda,
            username=self.admin_user.username,
            password=self.admin_user.password,
            sasl_mechanism=self.admin_user.algorithm,
            tls_cert=self._cert,
        )
        self._su.sasl_create_user(
            self.admin_user.username,
            self.admin_user.password,
            self.admin_user.algorithm,
        )
        self._admin = Admin(self.redpanda)
        self._wait_user(self.admin_user.username)
        wait_until(
            self._schema_topic_created, timeout_sec=60, backoff_sec=3, retry_on_exc=True
        )

    def _schema_topic_created(self):
        self._su.list_schemas()
        return "_schemas" in self._su.list_topics()

    def _wait_user(self, username):
        def propagated():
            return all(
                username in self._admin.list_users(node=n) for n in self.redpanda.nodes
            )

        wait_until(propagated, timeout_sec=60, backoff_sec=3)

    def _make_user(self, name):
        """Create a restricted SCRAM user (no ACLs) and return an rpk client for it."""
        self._su.sasl_create_user(name, self.password, self.mechanism)
        self._wait_user(name)
        return RpkTool(
            self.redpanda,
            username=name,
            password=self.password,
            sasl_mechanism=self.mechanism,
            tls_cert=self._cert,
        )

    def _grant(self, name, operations, topic):
        # sasl_allow_principal (unlike allow_principal) carries the client's SASL creds + TLS
        # settings, which this harness's authorized+mTLS Kafka listener requires.
        self._su.sasl_allow_principal(f"User:{name}", operations, "topic", topic)

    def _eventually(self, fn, timeout_sec=30):
        # ACL grants propagate through the controller asynchronously, so an operation issued
        # right after its grant can still be denied. Retry until the now-authorized op succeeds.
        # (Denied produce attempts write nothing, so the first success still lands at offset 0.)
        box = {}

        def attempt():
            box["v"] = fn()
            return True

        wait_until(attempt, timeout_sec=timeout_sec, backoff_sec=1, retry_on_exc=True)
        return box["v"]

    def _eventually_raises(self, fn, predicate, timeout_sec=30):
        # Negative-path analogue of _eventually. The DESCRIBE_CONFIGS denial checks run right
        # after granting WRITE/READ, but those grants (which imply DESCRIBE) propagate
        # asynchronously; until they take effect the op raises a DIFFERENT transient error
        # (UNKNOWN_TOPIC_OR_PARTITION, or a produce/consume authz failure) rather than the
        # DescribeConfigs denial we want to assert. Retry until fn() raises an RpkException
        # matching predicate. A clean success is a hard failure (the op should stay rejected --
        # DESCRIBE_CONFIGS is never granted during this phase, so it can never succeed).
        def attempt():
            try:
                fn()
            except RpkException as e:
                return predicate(e)  # intended error -> done; transient -> keep waiting
            raise AssertionError(
                "operation unexpectedly succeeded; expected it to be rejected"
            )

        wait_until(attempt, timeout_sec=timeout_sec, backoff_sec=1)

    def create_schema(self, subject, schema, context=None):
        with tempfile.NamedTemporaryFile(suffix=".avro") as tf:
            tf.write(bytes(schema, "UTF-8"))
            tf.seek(0)
            out = self._su.create_schema(subject, tf.name, context=context)
            assert out["subject"] == subject
            return out["id"]  # context-local id

    @staticmethod
    def _denied(e):
        # produce/consume rejected by the broker's authorizer. Redpanda may report the outright
        # authz failure, or mask an unauthorized topic as nonexistent (UNKNOWN_TOPIC_OR_PARTITION)
        # when the principal also lacks DESCRIBE; accept either as "denied".
        s = str(e).upper()
        return bool(e.returncode) and (
            "AUTHORIZATION" in s or "UNKNOWN_TOPIC_OR_PARTITION" in s
        )

    @staticmethod
    def _context_lookup_denied(e):
        # The config-derived context path (DescribeConfigs) failed; rpk wraps it with this
        # message in both produce.go and consume.go. Distinguishes the DESCRIBE_CONFIGS gate
        # from a plain produce/consume authz failure.
        return bool(e.returncode) and "unable to determine schema context" in str(e)

    @cluster(num_nodes=3)
    def test_produce_consume_minimal_permissions(self):
        # Plain produce/consume (no schema registry): WRITE to produce, READ to consume.
        topic = "perm-plain"
        self._su.create_topic(topic)
        producer = self._make_user("plain-producer")
        consumer = self._make_user("plain-consumer")

        # Denied with no ACL.
        with expect_exception(RpkException, self._denied):
            producer.produce(topic, key="k", msg="v")

        # WRITE on the topic is the minimal grant to produce.
        self._grant("plain-producer", ["WRITE"], topic)
        self._eventually(lambda: producer.produce(topic, key="k", msg="v"))

        # Consume denied without READ.
        with expect_exception(RpkException, self._denied):
            consumer.consume(topic, offset="0:1")

        # READ on the topic is the minimal grant to consume (direct partition consume, no
        # group, so no READ-on-GROUP ACL is needed).
        self._grant("plain-consumer", ["READ"], topic)
        out = json.loads(
            self._eventually(lambda: consumer.consume(topic, offset="0:1"))
        )
        assert out["value"] == "v", out

    @cluster(num_nodes=3)
    def test_schema_id_explicit_context_minimal_permissions(self):
        # produce/consume --schema-id with an explicit --schema-context: the context is taken
        # from the flag, so rpk does NOT read the topic config -> no DescribeConfigs, and the
        # minimal Kafka grant (WRITE/READ) is sufficient. (Schema Registry authz is off, so the
        # restricted user can fetch the schema by id.)
        self.create_schema(
            "filler-value", AVRO, context=".team"
        )  # bump the .team id counter
        ctx_id = self.create_schema("perm-ctx-value", AVRO, context=".team")

        topic = "perm-ctx"
        self._su.create_topic(topic)
        producer = self._make_user("ctx-producer")
        consumer = self._make_user("ctx-consumer")
        self._grant("ctx-producer", ["WRITE"], topic)
        self._grant("ctx-consumer", ["READ"], topic)

        msg = '{"f1":"hi"}'
        # Explicit context: succeeds with only WRITE (no DESCRIBE_CONFIGS).
        self._eventually(
            lambda: producer.produce(
                topic, key="k", msg=msg, schema_id=ctx_id, schema_context=".team"
            )
        )
        # Explicit context on decode: succeeds with only READ.
        out = self._eventually(
            lambda: consumer.consume(
                topic, offset="0:1", use_schema_registry="value", schema_context=".team"
            )
        )
        assert json.loads(json.loads(out)["value"]) == json.loads(msg), out

    @cluster(num_nodes=3)
    def test_schema_id_topic_context_requires_describe_configs(self):
        # produce/consume --schema-id WITHOUT --schema-context: rpk derives the context from the
        # topic's redpanda.schema.registry.context via DescribeConfigs, which needs the distinct
        # DESCRIBE_CONFIGS ACL (NOT implied by WRITE/READ). Confirm it fails with only WRITE/READ,
        # then succeeds once DESCRIBE_CONFIGS is also granted.
        self.create_schema("filler2-value", AVRO, context=".team")
        ctx_id = self.create_schema("perm-cfg-value", AVRO, context=".team")

        topic = "perm-cfg"
        self._su.create_topic(
            topic, config={"redpanda.schema.registry.context": ".team"}
        )
        producer = self._make_user("cfg-producer")
        consumer = self._make_user("cfg-consumer")
        # Grant the "obvious" produce/consume ACLs, but NOT DESCRIBE_CONFIGS.
        self._grant("cfg-producer", ["WRITE"], topic)
        self._grant("cfg-consumer", ["READ"], topic)

        msg = '{"f1":"hi"}'

        # Produce: the topic-config lookup is denied without DESCRIBE_CONFIGS. (Retry through
        # WRITE-grant propagation: until WRITE/DESCRIBE lands, DescribeConfigs reports the topic
        # as unknown -- which produce tolerates -- and the failure surfaces elsewhere.)
        self._eventually_raises(
            lambda: producer.produce(topic, key="k", msg=msg, schema_id=ctx_id),
            self._context_lookup_denied,
        )

        # Grant DESCRIBE_CONFIGS on the topic -> the lookup resolves .team and produce succeeds.
        self._grant("cfg-producer", ["DESCRIBE_CONFIGS"], topic)
        self._eventually(
            lambda: producer.produce(topic, key="k", msg=msg, schema_id=ctx_id)
        )

        # Consume: same gate on the decode side. (Retry through READ-grant propagation: until
        # READ/DESCRIBE lands, the up-front topic-existence check fails first with a different
        # error.)
        self._eventually_raises(
            lambda: consumer.consume(topic, offset="0:1", use_schema_registry="value"),
            self._context_lookup_denied,
        )

        self._grant("cfg-consumer", ["DESCRIBE_CONFIGS"], topic)
        out = self._eventually(
            lambda: consumer.consume(topic, offset="0:1", use_schema_registry="value")
        )
        assert json.loads(json.loads(out)["value"]) == json.loads(msg), out
