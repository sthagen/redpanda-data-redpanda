/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vassert.h"
#include "cluster/cluster_link/frontend.h"
#include "cluster/cluster_link/table.h"
#include "cluster/cluster_link/tests/utils.h"
#include "cluster_link/model/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "test_utils/test.h"
#include "utils/unresolved_address.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

namespace cluster::cluster_link {

namespace clm = ::cluster_link::model;

// Exercises the write-protection decision at the level of
// cluster_link::frontend, driven directly off a sharded<table> populated with
// link metadata. The decision logic (frontend -> api_mode_shadows_context ->
// filter_selects_source_context) runs against real metadata without booting a
// node: the frontend is constructed with only the table dependency, since the
// write-blocking queries touch nothing else.
class shadow_link_write_blocking_test : public seastar_test {
public:
    ss::future<> SetUpAsync() override {
        co_await _table.start();
        _frontend = std::make_unique<frontend>(
          model::node_id{1},
          /*leaders=*/nullptr,
          &_table.local(),
          /*controller=*/nullptr,
          /*connections=*/nullptr,
          /*features=*/nullptr,
          /*as=*/nullptr);
    }

    ss::future<> TearDownAsync() override {
        _frontend.reset();
        co_await _table.stop();
    }

    static clm::metadata
    make_base_metadata(clm::name_t name = clm::name_t{"sr-shadow"}) {
        return clm::metadata{
          .name = std::move(name),
          .uuid = clm::uuid_t{::uuid_t::create()},
          .connection = clm::connection_config{
            .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    }

    // Builds base metadata carrying a _schemas mirror topic in the given
    // status, to exercise the mirror-topic branch of topic-mode shadowing
    // (where the failover status decides whether the local topic is writable).
    static clm::metadata
    make_schemas_mirror_link(clm::mirror_topic_status status) {
        auto md = make_base_metadata();
        testing::set_link_mirror_topics(
          md,
          model::schema_registry_internal_tp.topic,
          status,
          model::schema_registry_internal_tp.topic);
        return md;
    }

    // Applies an upsert command straight to the table, bypassing the
    // controller/raft path. Re-upserting an existing link name overwrites it.
    ss::future<> install_link(clm::metadata md) {
        auto err = co_await _table.local().apply_update(
          testing::create_upsert_command(
            model::offset{++_latest_id}, std::move(md)));
        vassert(!err, "Failed to install link: {}", err.message());
    }

    // Installs a Schema Registry shadow link with the given sync mode.
    ss::future<> install_shadow_link(auto sync_mode) {
        auto md = make_base_metadata();
        md.configuration.schema_registry_sync_cfg.sync_mode = std::move(
          sync_mode);
        co_await install_link(std::move(md));
    }

    frontend& fe() { return *_frontend; }

    bool client_write_blocked(std::string_view ctx) {
        return _frontend->schema_registry_client_writes_disabled(ctx);
    }

    bool sync_write_blocked() {
        return _frontend->schema_registry_local_topic_writes_disabled();
    }

    ss::sharded<table> _table;
    std::unique_ptr<frontend> _frontend;
    int64_t _latest_id{0};
};

// Identity mapping: a destination context is owned when the filter selects the
// same source context, whether named directly or implied by a selected subject.
TEST_F_CORO(
  shadow_link_write_blocking_test, api_shadow_blocks_only_owned_context) {
    // Baseline: with no shadow link, nothing is blocked.
    EXPECT_FALSE(fe().schema_registry_shadowing_active());
    EXPECT_FALSE(client_write_blocked(".prod"));

    clm::schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "http://schema-registry.example.com:8081";
    api.filter.contexts = {".prod"};
    // Subjects also select their context: a qualified subject (".audit"), a
    // context-only qualified subject (":.payments" -> ".payments"), and an
    // unqualified subject (the default context ".").
    api.filter.subjects = {":.audit:events", ":.payments", "orders-value"};
    api.destination
      = clm::schema_registry_sync_config::identity_context_mapping{};
    co_await install_shadow_link(std::move(api));

    EXPECT_TRUE(fe().schema_registry_shadowing_active());

    // Owned via the filter, directly or through a selected subject's context.
    EXPECT_TRUE(client_write_blocked(".prod"));
    EXPECT_TRUE(client_write_blocked(".audit"));
    EXPECT_TRUE(client_write_blocked(".payments"));
    EXPECT_TRUE(client_write_blocked("."));
    // Not selected by anything in the filter.
    EXPECT_FALSE(client_write_blocked(".staging"));

    // Sync (import) writes are never blocked under API-mode shadowing, even for
    // an owned context: that is how the mirroring populates the destination.
    EXPECT_FALSE(sync_write_blocked());
}

// A paused API-mode link relinquishes ownership of its contexts: a context it
// would otherwise block becomes writable again, while a context it never owned
// is writable either way. Re-upserting the same link name flips only `paused`,
// so this exercises the pause branch of link_disables_client_writes in
// isolation.
TEST_F_CORO(shadow_link_write_blocking_test, api_shadow_paused_lifts_block) {
    auto make_link = [](bool paused) {
        auto md = make_base_metadata();
        clm::schema_registry_sync_config::shadow_schema_registry_api api;
        api.source_url = "http://schema-registry.example.com:8081";
        api.filter.contexts = {".prod"};
        api.destination
          = clm::schema_registry_sync_config::identity_context_mapping{};
        api.is_enabled = clm::enabled_t{!paused};
        md.configuration.schema_registry_sync_cfg.sync_mode = std::move(api);
        return md;
    };

    // Not paused: the owned context is blocked, an unowned one is not.
    co_await install_link(make_link(/*paused=*/false));
    EXPECT_TRUE(client_write_blocked(".prod"));
    EXPECT_FALSE(client_write_blocked(".staging"));

    // Paused: the previously owned context is writable again; the unowned one
    // is unaffected.
    co_await install_link(make_link(/*paused=*/true));
    EXPECT_FALSE(client_write_blocked(".prod"));
    EXPECT_FALSE(client_write_blocked(".staging"));
    // Sync writes are never blocked under API mode, paused or not.
    EXPECT_FALSE(sync_write_blocked());
}

// Pausing one link must not unblock a context another (non-paused) link still
// owns: paused only removes that one link's contribution to the any_of across
// links, so a context owned by any live link stays blocked.
TEST_F_CORO(
  shadow_link_write_blocking_test, paused_link_does_not_unblock_other_owner) {
    auto make_api_link =
      [](clm::name_t name, ss::sstring owned_context, bool paused) {
          auto md = make_base_metadata(std::move(name));
          clm::schema_registry_sync_config::shadow_schema_registry_api api;
          api.source_url = "http://schema-registry.example.com:8081";
          api.filter.contexts = {std::move(owned_context)};
          api.destination
            = clm::schema_registry_sync_config::identity_context_mapping{};
          api.is_enabled = clm::enabled_t{!paused};
          md.configuration.schema_registry_sync_cfg.sync_mode = std::move(api);
          return md;
      };

    // link-a (paused) and link-b (active) both own ".prod".
    co_await install_link(
      make_api_link(clm::name_t{"link-a"}, ".prod", /*paused=*/true));
    co_await install_link(
      make_api_link(clm::name_t{"link-b"}, ".prod", /*paused=*/false));

    // ".prod" stays blocked: link-b still owns it even though link-a is paused.
    EXPECT_TRUE(client_write_blocked(".prod"));
    // A context neither link owns is writable.
    EXPECT_FALSE(client_write_blocked(".staging"));
}

// Exact mapping: a destination context is owned when a selected source context
// maps to it. Blocking follows the destination name, not the source name.
TEST_F_CORO(
  shadow_link_write_blocking_test, api_shadow_blocks_remapped_destination) {
    clm::schema_registry_sync_config::exact_context_mapping exact;
    exact.mappings.emplace(".prod", ".prod-mirror");

    clm::schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "http://schema-registry.example.com:8081";
    api.filter.contexts = {".prod"};
    api.destination = std::move(exact);
    co_await install_shadow_link(std::move(api));

    // The mapped destination ".prod-mirror" is owned.
    EXPECT_TRUE(client_write_blocked(".prod-mirror"));
    // The source-named ".prod" is not a local destination, so it is writable.
    EXPECT_FALSE(client_write_blocked(".prod"));
    EXPECT_FALSE(client_write_blocked(".staging"));
}

// Exact mapping: a mapping whose source context is not selected by the filter
// is inert -- nothing is mirrored into its destination, so that destination
// stays writable. Ownership requires both the destination name to match and the
// source to be selected by the filter.
TEST_F_CORO(
  shadow_link_write_blocking_test, api_shadow_ignores_unfiltered_mapping) {
    clm::schema_registry_sync_config::exact_context_mapping exact;
    exact.mappings.emplace(".prod", ".prod-mirror");
    // ".staging" maps to ".staging-mirror" but is not selected by the filter.
    exact.mappings.emplace(".staging", ".staging-mirror");

    clm::schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "http://schema-registry.example.com:8081";
    api.filter.contexts = {".prod"};
    api.destination = std::move(exact);
    co_await install_shadow_link(std::move(api));

    // The filter-selected source ".prod" makes its destination owned.
    EXPECT_TRUE(client_write_blocked(".prod-mirror"));
    // ".staging-mirror" is also a mapping destination, but its source
    // ".staging" is not in the filter, so the mapping is inert and the
    // destination stays writable.
    EXPECT_FALSE(client_write_blocked(".staging-mirror"));
}

// Topic-mode shadowing owns the entire local _schemas topic, so it blocks every
// context regardless of any source filter, and also blocks the sync importer
// and internal topic creation.
TEST_F_CORO(
  shadow_link_write_blocking_test, topic_shadow_blocks_every_context) {
    co_await install_shadow_link(
      clm::schema_registry_sync_config::shadow_entire_schema_registry{});

    EXPECT_TRUE(fe().schema_registry_shadowing_active());

    // Every context is blocked for client writes, owned or not.
    EXPECT_TRUE(client_write_blocked(".prod"));
    EXPECT_TRUE(client_write_blocked(".staging"));
    EXPECT_TRUE(client_write_blocked("."));

    // Unlike API mode, the sync importer and internal _schemas topic creation
    // are blocked too, because topic-mode owns the topic itself.
    EXPECT_TRUE(sync_write_blocked());
    EXPECT_TRUE(fe().schema_registry_local_topic_writes_disabled());
}

// An empty source filter mirrors the whole source registry, so API-mode
// shadowing owns (and blocks client writes to) every context.
TEST_F_CORO(
  shadow_link_write_blocking_test, api_shadow_empty_filter_blocks_all) {
    clm::schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "http://schema-registry.example.com:8081";
    // No contexts and no subjects -> the whole source registry is mirrored.
    api.destination
      = clm::schema_registry_sync_config::identity_context_mapping{};
    co_await install_shadow_link(std::move(api));

    EXPECT_TRUE(client_write_blocked("."));
    EXPECT_TRUE(client_write_blocked(".prod"));
    EXPECT_TRUE(client_write_blocked(".staging"));

    // Still API mode, so the sync importer and internal _schemas topic creation
    // are not blocked, even though every context is owned for client writes.
    EXPECT_FALSE(sync_write_blocked());
    EXPECT_FALSE(fe().schema_registry_local_topic_writes_disabled());
}

// No destination set falls back to identity mapping: a destination context is
// owned exactly when the filter selects the same source context. Exercises the
// `!destination` branch, which is distinct from an explicit identity mapping.
TEST_F_CORO(
  shadow_link_write_blocking_test, api_shadow_unset_destination_is_identity) {
    clm::schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "http://schema-registry.example.com:8081";
    api.filter.contexts = {".prod"};
    // destination left unset (std::nullopt).
    co_await install_shadow_link(std::move(api));

    EXPECT_TRUE(client_write_blocked(".prod"));
    EXPECT_FALSE(client_write_blocked(".staging"));
}

// An empty filter selects every source context, but an exact mapping is
// exhaustive: only the destinations it names are mirrored, so ownership is
// limited to those destinations even when the filter selects all sources.
TEST_F_CORO(
  shadow_link_write_blocking_test,
  api_shadow_empty_filter_blocks_only_mapped_destinations) {
    clm::schema_registry_sync_config::exact_context_mapping exact;
    exact.mappings.emplace(".prod", ".prod-mirror");

    clm::schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "http://schema-registry.example.com:8081";
    // Empty filter (no contexts, no subjects).
    api.destination = std::move(exact);
    co_await install_shadow_link(std::move(api));

    // Only the mapped destination is owned; the unmapped source name and
    // unrelated contexts stay writable.
    EXPECT_TRUE(client_write_blocked(".prod-mirror"));
    EXPECT_FALSE(client_write_blocked(".prod"));
    EXPECT_FALSE(client_write_blocked(".anything"));
}

// Exact mapping combined with a subject filter: a subject selects its source
// context, and ownership then follows that source context's mapped destination.
TEST_F_CORO(
  shadow_link_write_blocking_test,
  api_shadow_exact_mapping_with_subject_filter) {
    clm::schema_registry_sync_config::exact_context_mapping exact;
    exact.mappings.emplace(".payments", ".payments-mirror");

    clm::schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "http://schema-registry.example.com:8081";
    // A context-only qualified subject (":.payments") selects the ".payments"
    // source context.
    api.filter.subjects = {":.payments"};
    api.destination = std::move(exact);
    co_await install_shadow_link(std::move(api));

    // ".payments" is selected via the subject and maps to ".payments-mirror".
    EXPECT_TRUE(client_write_blocked(".payments-mirror"));
    // The source-named context and unrelated contexts stay writable.
    EXPECT_FALSE(client_write_blocked(".payments"));
    EXPECT_FALSE(client_write_blocked(".staging"));
}

// Two source contexts map to the same destination; the destination is owned as
// long as at least one of those sources is selected by the filter.
TEST_F_CORO(
  shadow_link_write_blocking_test,
  api_shadow_shared_destination_owned_if_any_source_selected) {
    clm::schema_registry_sync_config::exact_context_mapping exact;
    exact.mappings.emplace(".prod", ".shared");
    exact.mappings.emplace(".staging", ".shared");

    clm::schema_registry_sync_config::shadow_schema_registry_api api;
    api.source_url = "http://schema-registry.example.com:8081";
    // Only ".prod" is selected; ".staging" is not.
    api.filter.contexts = {".prod"};
    api.destination = std::move(exact);
    co_await install_shadow_link(std::move(api));

    // ".shared" is owned because the selected ".prod" maps to it, even though
    // the other source (".staging") is not selected.
    EXPECT_TRUE(client_write_blocked(".shared"));
}

// Topic-mode shadowing via a real _schemas mirror topic: while the mirror has
// not yet failed over, it owns the local topic, so every client write and the
// sync importer are blocked. Exercises the mirror-topic branch of
// link_shadows_schema_registry_topic and the non-mutable arm of
// is_topic_mutable across every pre-failover status.
TEST_F_CORO(
  shadow_link_write_blocking_test, topic_mirror_blocks_writes_before_failover) {
    for (auto status : {
           clm::mirror_topic_status::active,
           clm::mirror_topic_status::failed,
           clm::mirror_topic_status::paused,
           clm::mirror_topic_status::failing_over,
           clm::mirror_topic_status::promoting,
         }) {
        co_await install_link(make_schemas_mirror_link(status));
        auto s = static_cast<int>(status);
        EXPECT_TRUE(fe().schema_registry_shadowing_active()) << "status=" << s;
        // Topic-mode owns the whole _schemas topic: every context is blocked.
        EXPECT_TRUE(client_write_blocked(".prod")) << "status=" << s;
        EXPECT_TRUE(client_write_blocked(".")) << "status=" << s;
        // The sync importer and internal topic creation are blocked too.
        EXPECT_TRUE(sync_write_blocked()) << "status=" << s;
    }
}

// Relaxation after failover: once the _schemas mirror has failed over or been
// promoted, the local topic is mutable again, so shadowing is no longer active
// and neither client nor sync writes are blocked. Exercises the mutable arm of
// is_topic_mutable.
TEST_F_CORO(
  shadow_link_write_blocking_test, topic_mirror_relaxes_writes_after_failover) {
    for (auto status : {
           clm::mirror_topic_status::failed_over,
           clm::mirror_topic_status::promoted,
         }) {
        co_await install_link(make_schemas_mirror_link(status));
        auto s = static_cast<int>(status);
        // The link still exists (cluster linking is active), so this exercises
        // the decision logic rather than the no-link early-out.
        EXPECT_FALSE(fe().schema_registry_shadowing_active()) << "status=" << s;
        EXPECT_FALSE(client_write_blocked(".prod")) << "status=" << s;
        EXPECT_FALSE(sync_write_blocked()) << "status=" << s;
    }
}

// Aggregation across links: write blocking is the OR over every link, so two
// API-mode links owning different contexts each block their own, while a
// context owned by neither stays writable.
TEST_F_CORO(
  shadow_link_write_blocking_test, multiple_links_each_block_their_context) {
    auto make_api_link = [](clm::name_t name, ss::sstring owned_context) {
        auto md = make_base_metadata(std::move(name));
        clm::schema_registry_sync_config::shadow_schema_registry_api api;
        api.source_url = "http://schema-registry.example.com:8081";
        api.filter.contexts = {std::move(owned_context)};
        api.destination
          = clm::schema_registry_sync_config::identity_context_mapping{};
        md.configuration.schema_registry_sync_cfg.sync_mode = std::move(api);
        return md;
    };

    co_await install_link(make_api_link(clm::name_t{"link-a"}, ".prod"));
    co_await install_link(make_api_link(clm::name_t{"link-b"}, ".staging"));

    // Each link blocks its own owned context; the any_of across links covers
    // both.
    EXPECT_TRUE(client_write_blocked(".prod"));
    EXPECT_TRUE(client_write_blocked(".staging"));
    // A context owned by neither link stays writable.
    EXPECT_FALSE(client_write_blocked(".audit"));
}

} // namespace cluster::cluster_link
