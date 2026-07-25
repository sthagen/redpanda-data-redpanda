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

#include "cluster_link/model/filter_utils.h"
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

// The shape of link_configuration before role sync (v26.2) appended
// role_sync_cfg: the four fields that preceded it, at the original envelope
// version. Models a node on the prior release decoding a link configuration
// written by an upgraded node. Reuses the current nested config types (as
// schema_registry_sync_config_v0 does) -- only the absence of the trailing
// role_sync_cfg field is modeled here.
struct link_configuration_v0
  : serde::envelope<
      link_configuration_v0,
      serde::version<0>,
      serde::compat_version<0>> {
    topic_metadata_mirroring_config topic_metadata_mirroring_cfg;
    consumer_groups_mirroring_config consumer_groups_mirroring_cfg;
    security_settings_sync_config security_settings_sync_cfg;
    schema_registry_sync_config schema_registry_sync_cfg;

    auto serde_fields() {
        return std::tie(
          topic_metadata_mirroring_cfg,
          consumer_groups_mirroring_cfg,
          security_settings_sync_cfg,
          schema_registry_sync_cfg);
    }
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
    api.is_enabled = enabled_t::no;

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
    EXPECT_EQ(roundtrip_api.is_enabled, enabled_t::no);
}

TEST(test_model, schema_registry_sync_config_round_trips_topic_mode) {
    schema_registry_sync_config cfg;
    cfg.sync_mode
      = schema_registry_sync_config::shadow_entire_schema_registry{};

    auto roundtrip = serde_to<schema_registry_sync_config>(cfg);

    EXPECT_TRUE(roundtrip.is_topic_mode());
    EXPECT_EQ(roundtrip.api_mode(), nullptr);
}

TEST(test_model, schema_registry_sync_config_is_enabled_copy_and_round_trip) {
    schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "https://schema-registry.example.com";
    api.is_enabled = enabled_t::no;

    schema_registry_sync_config cfg;
    cfg.sync_mode = std::move(api);

    EXPECT_EQ(cfg.copy().api_mode()->is_enabled, enabled_t::no);
    EXPECT_EQ(
      serde_to<schema_registry_sync_config>(cfg).api_mode()->is_enabled,
      enabled_t::no);

    cfg.api_mode()->is_enabled = enabled_t::yes;
    EXPECT_EQ(cfg.copy().api_mode()->is_enabled, enabled_t::yes);
    EXPECT_EQ(
      serde_to<schema_registry_sync_config>(cfg).api_mode()->is_enabled,
      enabled_t::yes);
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

TEST(test_model, role_sync_config_serde_round_trip) {
    link_configuration cfg;
    cfg.role_sync_cfg.is_enabled = enabled_t::no;
    cfg.role_sync_cfg.task_interval = std::chrono::seconds{45};
    cfg.role_sync_cfg.role_name_filters.push_back(
      resource_name_filter_pattern{
        .pattern_type = filter_pattern_type::prefix,
        .filter = filter_type::include,
        .pattern = "analytics-"});

    auto buf = serde::to_iobuf(cfg.copy());
    auto decoded = serde::from_iobuf<link_configuration>(std::move(buf));

    EXPECT_EQ(decoded, cfg);
    ASSERT_EQ(decoded.role_sync_cfg.role_name_filters.size(), 1);
    EXPECT_EQ(decoded.role_sync_cfg.is_enabled, enabled_t::no);
    EXPECT_EQ(
      decoded.role_sync_cfg.get_task_interval(), std::chrono::seconds{45});
}

// Downgrade safety for the unfinalized-upgrade feature. Role sync (v26.2)
// appends role_sync_cfg to link_configuration as a trailing field, bumping the
// envelope to version<1> while leaving compat_version at 0. Because shadow
// links exist since v25.3, a link created or edited on the upgraded binary
// while the upgrade is still unfinalized persists a v1 link_configuration in
// the controller log; if that upgrade is rolled back, a node on the prior
// release must still decode the record -- recovering every field it knows and
// skipping the trailing role_sync_cfg. If this breaks (compat_version bumped,
// or a field inserted ahead of role_sync_cfg) an unfinalized downgrade could no
// longer replay the controller log.
TEST(test_model, link_configuration_legacy_reads_v1_role_sync) {
    link_configuration cfg;
    // A field that predates role_sync_cfg: the legacy reader must recover it.
    cfg.topic_metadata_mirroring_cfg.is_enabled = enabled_t::no;
    cfg.topic_metadata_mirroring_cfg.task_interval = std::chrono::seconds{45};
    // The trailing v1-only field, populated so the test proves it is skipped
    // rather than corrupting the decode of the fields ahead of it.
    cfg.role_sync_cfg.is_enabled = enabled_t::no;
    cfg.role_sync_cfg.role_name_filters.push_back(
      resource_name_filter_pattern{
        .pattern_type = filter_pattern_type::prefix,
        .filter = filter_type::include,
        .pattern = "analytics-"});

    auto legacy = serde_to<link_configuration_v0>(cfg);

    EXPECT_EQ(legacy.topic_metadata_mirroring_cfg.is_enabled, enabled_t::no);
    EXPECT_EQ(
      legacy.topic_metadata_mirroring_cfg.get_task_interval(),
      std::chrono::seconds{45});
}

// The complementary upgrade-read direction: a link_configuration written by the
// prior release (no role_sync_cfg field) is decoded by the current binary,
// which must default role_sync_cfg rather than fail. An upgraded node reads
// pre-upgrade link records this way on every restart.
TEST(test_model, link_configuration_reads_v0_defaults_role_sync) {
    link_configuration_v0 legacy;
    legacy.topic_metadata_mirroring_cfg.is_enabled = enabled_t::no;

    auto cfg = serde_to<link_configuration>(legacy);

    EXPECT_EQ(cfg.topic_metadata_mirroring_cfg.is_enabled, enabled_t::no);
    // role_sync_cfg falls back to its defaults: no filters, so the migrator is
    // a no-op until one is configured.
    EXPECT_TRUE(cfg.role_sync_cfg.role_name_filters.empty());
    EXPECT_EQ(
      cfg.role_sync_cfg.get_task_interval(),
      role_sync_config::task_interval_default);
}

TEST(test_model, select_role_include_exclude_prefix) {
    using namespace cluster_link::model;
    chunked_vector<resource_name_filter_pattern> patterns;
    patterns.push_back(
      resource_name_filter_pattern{
        .pattern_type = filter_pattern_type::prefix,
        .filter = filter_type::include,
        .pattern = "analytics-"});
    patterns.push_back(
      resource_name_filter_pattern{
        .pattern_type = filter_pattern_type::literal,
        .filter = filter_type::exclude,
        .pattern = "analytics-secret"});

    EXPECT_TRUE(select_role("analytics-reader", patterns));
    EXPECT_FALSE(select_role("analytics-secret", patterns)); // excluded
    EXPECT_FALSE(select_role("ops-admin", patterns));        // no include match
}
} // namespace cluster_link::model::tests
