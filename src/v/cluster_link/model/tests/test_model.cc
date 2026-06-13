/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/model/types.h"
#include "serde/rw/rw.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace cluster_link::model::tests {
namespace {
template<typename To, typename From>
To serde_to(const From& from) {
    auto b = serde::to_iobuf(from);
    return serde::from_iobuf<To>(std::move(b));
}

struct schema_registry_sync_config_v0
  : serde::envelope<
      schema_registry_sync_config_v0,
      serde::version<0>,
      serde::compat_version<0>> {
    using shadow_schema_registry_mode_t = serde::variant<
      schema_registry_sync_config::shadow_entire_schema_registry>;

    std::optional<shadow_schema_registry_mode_t>
      sync_schema_registry_topic_mode;

    auto serde_fields() { return std::tie(sync_schema_registry_topic_mode); }
};
} // namespace

TEST(test_model, test_no_leak_private_data) {
    scram_credentials creds{
      .username = "user", .password = "pass", .mechanism = "SCRAM-SHA-256"};

    auto creds_str = fmt::format("{}", creds);
    // verify password does not get printed
    EXPECT_TRUE(creds_str.contains("password: ****"));

    connection_config config_files{
      .bootstrap_servers = {net::unresolved_address{"localhost", 9092}},
      .authn_config = creds,
      .cert = tls_file_path{"cert.pem"},
      .key = tls_file_path{"key.pem"},
      .ca = tls_file_path{"ca.pem"},
      .client_id = "client-id"};

    auto fmt = fmt::format("{}", config_files);
    // verify password does not get printed
    EXPECT_TRUE(fmt.contains("password: ****"));

    connection_config config_values{
      .bootstrap_servers = {net::unresolved_address{"localhost", 9092}},
      .authn_config = creds,
      .cert = tls_value{"cert.pem"},
      .key = tls_value{"key.pem"},
      .ca = tls_value{"ca.pem"},
      .client_id = "client-id"};
    auto values_fmt = fmt::format("{}", config_values);
    // verify password does not get printed
    EXPECT_TRUE(fmt.contains("password: ****"));
    // verify key is not printed
    EXPECT_TRUE(values_fmt.contains("key: {value: ****}"));
}

TEST(test_model, schema_registry_sync_config_reads_v0_topic_mode) {
    schema_registry_sync_config_v0 legacy;
    legacy.sync_schema_registry_topic_mode
      = schema_registry_sync_config::shadow_entire_schema_registry{};

    auto cfg = serde_to<schema_registry_sync_config>(legacy);

    EXPECT_TRUE(cfg.is_topic_mode());
    EXPECT_EQ(cfg.api_mode(), nullptr);
}

TEST(test_model, schema_registry_sync_config_round_trips_api_mode) {
    schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "https://schema-registry.example.com";
    api.auth_config = schema_registry_sync_config::basic_auth{
      .username = "sr-api-key",
      .password = "sr-api-secret",
      .password_last_updated = ::model::timestamp{1759193250080}};
    api.filter.contexts = {".", ".prod"};
    api.filter.subjects = {"orders-value", ":.prod:payments-value"};
    api.destination = schema_registry_sync_config::identity_context_mapping{};
    api.feature_policy
      = schema_registry_sync_config::unsupported_feature_policy::remove;

    schema_registry_sync_config cfg;
    cfg.sync_mode = api.copy();

    auto roundtrip = serde_to<schema_registry_sync_config>(cfg);

    EXPECT_FALSE(roundtrip.is_topic_mode());
    const auto* roundtrip_api_ptr = roundtrip.api_mode();
    ASSERT_NE(roundtrip_api_ptr, nullptr);
    const auto& roundtrip_api = *roundtrip_api_ptr;
    EXPECT_EQ(roundtrip_api.source_url, api.source_url);
    ASSERT_THAT(
      roundtrip_api.auth_config,
      testing::Optional(
        testing::VariantWith<schema_registry_sync_config::basic_auth>(
          testing::AllOf(
            testing::Field(
              &schema_registry_sync_config::basic_auth::username,
              ss::sstring{"sr-api-key"}),
            testing::Field(
              &schema_registry_sync_config::basic_auth::password,
              ss::sstring{"sr-api-secret"}),
            testing::Field(
              &schema_registry_sync_config::basic_auth::password_last_updated,
              ::model::timestamp{1759193250080})))));
    EXPECT_EQ(roundtrip_api.filter.contexts, api.filter.contexts);
    EXPECT_EQ(roundtrip_api.filter.subjects, api.filter.subjects);
    ASSERT_TRUE(roundtrip_api.destination.has_value());
    EXPECT_TRUE(
      std::holds_alternative<
        schema_registry_sync_config::identity_context_mapping>(
        *roundtrip_api.destination));
    EXPECT_EQ(
      roundtrip_api.feature_policy,
      schema_registry_sync_config::unsupported_feature_policy::remove);
}

TEST(test_model, schema_registry_sync_config_round_trips_topic_mode) {
    schema_registry_sync_config cfg;
    cfg.sync_mode
      = schema_registry_sync_config::shadow_entire_schema_registry{};

    auto roundtrip = serde_to<schema_registry_sync_config>(cfg);

    EXPECT_TRUE(roundtrip.is_topic_mode());
    EXPECT_EQ(roundtrip.api_mode(), nullptr);
}

// Mid-upgrade safety: a freshly-upgraded node writes the existing topic-mode
// field with the v1 schema, and a not-yet-upgraded node must still recover the
// field it understands while skipping any trailing fields added in v1.
TEST(test_model, schema_registry_sync_config_legacy_reads_v1_topic_mode) {
    schema_registry_sync_config cfg;
    cfg.sync_mode
      = schema_registry_sync_config::shadow_entire_schema_registry{};

    auto legacy = serde_to<schema_registry_sync_config_v0>(cfg);

    ASSERT_TRUE(legacy.sync_schema_registry_topic_mode.has_value());
    EXPECT_TRUE(
      std::holds_alternative<
        schema_registry_sync_config::shadow_entire_schema_registry>(
        *legacy.sync_schema_registry_topic_mode));
}

// Mid-upgrade safety: a not-yet-upgraded node decoding an API-mode record (a
// v1-only field) finds no topic-mode field and degrades to "no Schema Registry
// sync" rather than failing to decode. API mode is feature-gated until the
// cluster is fully upgraded, so this state should not arise in practice, but
// the wire format must still tolerate it.
TEST(test_model, schema_registry_sync_config_legacy_skips_v1_api_mode) {
    schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "https://schema-registry.example.com";

    schema_registry_sync_config cfg;
    cfg.sync_mode = std::move(api);

    auto legacy = serde_to<schema_registry_sync_config_v0>(cfg);

    EXPECT_FALSE(legacy.sync_schema_registry_topic_mode.has_value());
}
} // namespace cluster_link::model::tests
