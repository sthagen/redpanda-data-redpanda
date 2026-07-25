/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "http/client.h"
#include "pandaproxy/schema_registry/rest_client/client.h"
#include "pandaproxy/schema_registry/rest_client/error.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"
#include "test_utils/boost_fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>

#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string_view>
#include <variant>

namespace rc = pandaproxy::schema_registry::rest_client;
namespace pps = pandaproxy::schema_registry;
namespace bh = boost::beast::http;
using namespace std::chrono_literals;

namespace {

// A backward-compatible Avro record evolution: v2 adds a field with a default,
// so it registers under the default (backward) compatibility after v1.
constexpr std::string_view schema_v1
  = R"({"schema": "{\"type\": \"record\", \"name\": \"r1\", \"fields\": [{\"name\": \"f1\", \"type\": \"string\"}]}", "schemaType": "AVRO"})";
constexpr std::string_view schema_v2
  = R"({"schema": "{\"type\": \"record\", \"name\": \"r1\", \"fields\": [{\"name\": \"f1\", \"type\": \"string\"}, {\"name\": \"f2\", \"type\": \"string\", \"default\": \"\"}]}", "schemaType": "AVRO"})";
// Same shape as schema_v1 but carries metadata.properties. Redpanda's SR models
// only metadata.properties (not e.g. tags), so the read path parses it back and
// reports nothing as unknown.
constexpr std::string_view schema_with_metadata
  = R"({"schema": "{\"type\": \"record\", \"name\": \"r1\", \"fields\": [{\"name\": \"f1\", \"type\": \"string\"}]}", "schemaType": "AVRO", "metadata": {"properties": {"owner": "team-a", "tier": "gold"}}})";

rc::client make_rest_client(uint16_t port) {
    net::base_transport::configuration cfg;
    cfg.server_addr = net::unresolved_address{"localhost", port};
    return rc::client{
      std::make_unique<http::client>(cfg),
      fmt::format("http://localhost:{}", port),
      std::nullopt,
      pps::qualified_subjects_enabled::yes};
}

// POST a schema under subject_path (the wire form, raw) via the seed client.
void register_schema(
  http::client& client, std::string_view subject_path, std::string_view body) {
    auto res = http_request(
      client,
      fmt::format("/subjects/{}/versions", subject_path),
      iobuf::from(body),
      bh::verb::post,
      serialization_format::schema_registry_v1_json,
      serialization_format::schema_registry_v1_json);
    BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::ok);
}

// Issue a soft (impermanent) DELETE against subject_path via the seed client.
void soft_delete(http::client& client, std::string_view subject_path) {
    auto res = http_request(
      client,
      fmt::format("/subjects/{}", subject_path),
      bh::verb::delete_,
      serialization_format::schema_registry_v1_json,
      serialization_format::schema_registry_v1_json);
    BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::ok);
}

// Set the registry-wide (global) mode via PUT /mode on the seed client.
// mode_mutability defaults to true, so this is accepted.
void put_global_mode(http::client& client, std::string_view mode_value) {
    auto res = http_request(
      client,
      "/mode",
      iobuf::from(fmt::format(R"({{"mode": "{}"}})", mode_value)),
      bh::verb::put,
      serialization_format::schema_registry_v1_json,
      serialization_format::schema_registry_v1_json);
    BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::ok);
}

// Set a subject's (or context's) mode via PUT /mode/{subject} on the seed
// client. subject_path is the raw wire form.
void put_subject_mode(
  http::client& client,
  std::string_view subject_path,
  std::string_view mode_value) {
    auto res = http_request(
      client,
      fmt::format("/mode/{}", subject_path),
      iobuf::from(fmt::format(R"({{"mode": "{}"}})", mode_value)),
      bh::verb::put,
      serialization_format::schema_registry_v1_json,
      serialization_format::schema_registry_v1_json);
    BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::ok);
}

// Set the registry-wide (global) compatibility via PUT /config on the seed
// client. Note the request field is "compatibility", whereas GET /config
// returns it as "compatibilityLevel".
void put_global_config(http::client& client, std::string_view compat) {
    auto res = http_request(
      client,
      "/config",
      iobuf::from(fmt::format(R"({{"compatibility": "{}"}})", compat)),
      bh::verb::put,
      serialization_format::schema_registry_v1_json,
      serialization_format::schema_registry_v1_json);
    BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::ok);
}

// Set a subject's (or context's) compatibility via PUT /config/{subject} on the
// seed client. As with PUT /config the request field is "compatibility".
void put_subject_config(
  http::client& client,
  std::string_view subject_path,
  std::string_view compat) {
    auto res = http_request(
      client,
      fmt::format("/config/{}", subject_path),
      iobuf::from(fmt::format(R"({{"compatibility": "{}"}})", compat)),
      bh::verb::put,
      serialization_format::schema_registry_v1_json,
      serialization_format::schema_registry_v1_json);
    BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::ok);
}

} // namespace

// Drives the rest_client against the in-tree Schema Registry server: seeds
// schemas over the real REST API, then exercises its read calls (list_subjects,
// list_contexts, list_subject_versions, get_schema_by_version) plus the real
// not-found (40401/40402) responses through a real http::client. This is
// the fidelity counterpart to the mock-based client_test — it proves the wire
// shape, qualified-subject %3A path encoding, and error-code classification
// against actual server responses. (References are covered by the parser unit
// tests; this test keeps to default- and context-qualified subjects.)
FIXTURE_TEST(sr_rest_client_integration, pandaproxy_test_fixture) {
    info("Seeding subjects via the real REST API");
    auto seed = make_schema_reg_client();
    register_schema(seed, "multi", schema_v1);
    register_schema(seed, "multi", schema_v2);
    register_schema(seed, "solo", schema_v1);
    register_schema(seed, "withmeta", schema_with_metadata);
    // Qualified subjects are enabled by default; this exercises the client's
    // %3A path encoding end-to-end against the real server.
    register_schema(seed, ":.myctx:ctx-sub", schema_v1);

    auto sut = make_rest_client(*schema_reg_port);
    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 100ms);

    const auto multi = pps::context_subject::unqualified("multi");
    const auto solo = pps::context_subject::unqualified("solo");
    const auto withmeta = pps::context_subject::unqualified("withmeta");
    const auto ctx_sub = pps::context_subject{
      pps::context{".myctx"}, pps::subject{"ctx-sub"}};

    info("list_subjects returns the seeded subjects");
    {
        auto res = sut.list_subjects(rtc).get();
        BOOST_REQUIRE(res.has_value());
        const auto& subs = res.value();
        auto contains = [&subs](const pps::context_subject& s) {
            return std::ranges::find(subs, s) != subs.end();
        };
        BOOST_REQUIRE(contains(multi));
        BOOST_REQUIRE(contains(solo));
        BOOST_REQUIRE(contains(ctx_sub));
    }

    info("list_contexts returns the default and the seeded named context");
    {
        auto res = sut.list_contexts(rtc).get();
        BOOST_REQUIRE(res.has_value());
        const auto& ctxs = res.value();
        auto contains = [&ctxs](const pps::context& c) {
            return std::ranges::find(ctxs, c) != ctxs.end();
        };
        // The default context is always present; registering :.myctx:ctx-sub
        // above materialized ".myctx".
        BOOST_REQUIRE(contains(pps::default_context));
        BOOST_REQUIRE(contains(pps::context{".myctx"}));
    }

    info(
      "list_contexts filters by prefix client-side (the server ignores the "
      "contextPrefix param)");
    {
        // Redpanda returns every context; the client filters, so only ".myctx"
        // — not the default "." — comes back for the ".myctx" prefix.
        auto res = sut.list_contexts(rtc, ss::sstring{".myctx"}).get();
        BOOST_REQUIRE(res.has_value());
        const auto& ctxs = res.value();
        auto contains = [&ctxs](const pps::context& c) {
            return std::ranges::find(ctxs, c) != ctxs.end();
        };
        BOOST_REQUIRE(contains(pps::context{".myctx"}));
        BOOST_REQUIRE(!contains(pps::default_context));
    }

    info("get_mode returns the default READWRITE global mode");
    {
        // No global mode has been set, so the registry reports its built-in
        // default. The real server emits {"mode":"READWRITE"}.
        auto res = sut.get_mode(rtc).get();
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(res->mode == rc::registry_mode::read_write);
        BOOST_REQUIRE_EQUAL(res->raw, "READWRITE");
    }

    info("get_config returns the default BACKWARD compatibility level");
    {
        // No global config has been set, so the registry reports its built-in
        // default. The real server emits {"compatibilityLevel":"BACKWARD"} and
        // nothing else, so unsupported is empty.
        auto res = sut.get_config(rtc).get();
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(res->level == rc::registry_compatibility_level::backward);
        BOOST_REQUIRE_EQUAL(res->raw, "BACKWARD");
        BOOST_REQUIRE(res->unsupported.empty());
    }

    info(
      "get_subject_config: an un-overridden subject is not configured, but "
      "defaultToGlobal resolves the effective config");
    {
        // "multi" was seeded without a subject-level config, so its own config
        // is the real 40408 (mapped to subject_config_not_found).
        auto own = sut.get_subject_config(multi, rtc).get();
        BOOST_REQUIRE(!own.has_value());
        BOOST_REQUIRE(
          std::holds_alternative<rc::subject_config_not_found>(own.error()));

        // With defaultToGlobal the effective config resolves down to the global
        // default (BACKWARD) rather than 40408.
        auto effective
          = sut.get_subject_config(multi, rtc, pps::default_to_global::yes)
              .get();
        BOOST_REQUIRE(effective.has_value());
        BOOST_REQUIRE(
          effective->level == rc::registry_compatibility_level::backward);
    }

    info(
      "get_subject_mode: an un-overridden subject is not configured, but "
      "defaultToGlobal resolves the effective mode");
    {
        // "multi" was seeded without a subject-level mode, so its own mode is
        // the real 40409 (mapped to subject_mode_not_found).
        auto own = sut.get_subject_mode(multi, rtc).get();
        BOOST_REQUIRE(!own.has_value());
        BOOST_REQUIRE(
          std::holds_alternative<rc::subject_mode_not_found>(own.error()));

        // With defaultToGlobal the effective mode resolves down to the global
        // default (READWRITE) rather than 40409.
        auto effective
          = sut.get_subject_mode(multi, rtc, pps::default_to_global::yes).get();
        BOOST_REQUIRE(effective.has_value());
        BOOST_REQUIRE(effective->mode == rc::registry_mode::read_write);
    }

    info("list_subject_versions returns [1, 2]");
    {
        auto res = sut.list_subject_versions(multi, rtc).get();
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE_EQUAL(res->size(), 2U);
        BOOST_REQUIRE_EQUAL((*res)[0], pps::schema_version{1});
        BOOST_REQUIRE_EQUAL((*res)[1], pps::schema_version{2});
    }

    info("get_schema_by_version returns the stored schema");
    {
        auto res
          = sut.get_schema_by_version(multi, pps::schema_version{2}, rtc).get();
        BOOST_REQUIRE(res.has_value());
        // Redpanda's SR emits only fields we model, so nothing is dropped.
        BOOST_REQUIRE(res->unsupported.empty());
        const auto& s = res.value();
        BOOST_REQUIRE_EQUAL(s.schema.sub(), multi);
        BOOST_REQUIRE_EQUAL(s.version, pps::schema_version{2});
        BOOST_REQUIRE_GE(s.id(), 1);
        BOOST_REQUIRE(s.schema.def().type() == pps::schema_type::avro);
        BOOST_REQUIRE(!s.schema.def().raw()().linearize_to_string().empty());
    }

    info("get_schema_by_version round-trips metadata.properties");
    {
        auto res
          = sut.get_schema_by_version(withmeta, pps::schema_version{1}, rtc)
              .get();
        BOOST_REQUIRE(res.has_value());
        // metadata.properties is modeled, so it parses back in full and nothing
        // is reported as unsupported.
        BOOST_REQUIRE(res->unsupported.empty());
        const auto& def = res->schema.def();
        BOOST_REQUIRE(def.meta().has_value());
        BOOST_REQUIRE(def.meta()->properties.has_value());
        const auto& props = def.meta()->properties.value();
        BOOST_REQUIRE_EQUAL(props.size(), 2U);
        BOOST_REQUIRE_EQUAL(props.at("owner"), "team-a");
        BOOST_REQUIRE_EQUAL(props.at("tier"), "gold");
    }

    info("get_schema_by_version reaches a context-qualified subject (%3A)");
    {
        auto res
          = sut.get_schema_by_version(ctx_sub, pps::schema_version{1}, rtc)
              .get();
        BOOST_REQUIRE(res.has_value());
        const auto& s = res.value();
        BOOST_REQUIRE_EQUAL(s.schema.sub(), ctx_sub);
        BOOST_REQUIRE_EQUAL(s.version, pps::schema_version{1});
    }

    info("a missing subject yields subject_not_found (real 40401)");
    {
        const auto missing = pps::context_subject::unqualified("missing");
        auto versions = sut.list_subject_versions(missing, rtc).get();
        BOOST_REQUIRE(!versions.has_value());
        BOOST_REQUIRE(
          std::holds_alternative<rc::subject_not_found>(versions.error()));

        auto schema
          = sut.get_schema_by_version(missing, pps::schema_version{1}, rtc)
              .get();
        BOOST_REQUIRE(!schema.has_value());
        BOOST_REQUIRE(
          std::holds_alternative<rc::subject_not_found>(schema.error()));
    }

    info("a missing version yields version_not_found (real 40402)");
    {
        auto res = sut
                     .get_schema_by_version(multi, pps::schema_version{99}, rtc)
                     .get();
        BOOST_REQUIRE(!res.has_value());
        BOOST_REQUIRE(
          std::holds_alternative<rc::version_not_found>(res.error()));
    }

    // Kept before the delete section, while multi/v1 and solo/v1 are both live.
    auto sv_contains = [](
                         const auto& range,
                         const pps::context_subject& s,
                         pps::schema_version v) {
        return std::ranges::any_of(range, [&](const pps::subject_version& sv) {
            return sv.sub == s && sv.version == v;
        });
    };

    info(
      "get_schema_id_subject_versions enumerates every subject sharing an id");
    {
        // "multi" v1 and "solo" v1 registered identical content (schema_v1), so
        // they share one schema id in the default context; the lookup returns
        // both pairs.
        auto v1
          = sut.get_schema_by_version(multi, pps::schema_version{1}, rtc).get();
        BOOST_REQUIRE(v1.has_value());

        auto res = sut.get_schema_id_subject_versions(v1->id, rtc).get();
        BOOST_REQUIRE(res.has_value());
        // Order is not guaranteed; check membership.
        BOOST_REQUIRE(sv_contains(res.value(), multi, pps::schema_version{1}));
        BOOST_REQUIRE(sv_contains(res.value(), solo, pps::schema_version{1}));
    }

    info(
      "get_schema_id_subject_versions yields schema_id_not_found for a missing "
      "id (real 40403)");
    {
        auto res
          = sut.get_schema_id_subject_versions(pps::schema_id{123456}, rtc)
              .get();
        BOOST_REQUIRE(!res.has_value());
        BOOST_REQUIRE(
          std::holds_alternative<rc::schema_id_not_found>(res.error()));
    }

    info("get_schema_id_subject_versions resolves an id in a named context");
    {
        // ctx-sub lives in .myctx; passing it as the subject parameter resolves
        // the id within that context (%3A path/query encoding end-to-end).
        auto cs = sut
                    .get_schema_by_version(ctx_sub, pps::schema_version{1}, rtc)
                    .get();
        BOOST_REQUIRE(cs.has_value());

        auto res
          = sut.get_schema_id_subject_versions(cs->id, rtc, ctx_sub).get();
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(
          sv_contains(res.value(), ctx_sub, pps::schema_version{1}));
    }

    info("deleted=true surfaces soft-deleted versions and subjects");
    {
        // Soft-delete version 1 of "multi" (v2 remains, so the subject stays
        // live) and the whole single-version "solo" subject.
        soft_delete(seed, "multi/versions/1");
        soft_delete(seed, "solo");

        auto contains = [](const auto& range, const pps::context_subject& s) {
            return std::ranges::find(range, s) != range.end();
        };

        info(
          "list_subject_versions hides v1 by default, shows it with deleted");
        {
            auto live = sut.list_subject_versions(multi, rtc).get();
            BOOST_REQUIRE(live.has_value());
            BOOST_REQUIRE_EQUAL(live->size(), 1U);
            BOOST_REQUIRE_EQUAL((*live)[0], pps::schema_version{2});

            auto all
              = sut.list_subject_versions(multi, rtc, pps::include_deleted::yes)
                  .get();
            BOOST_REQUIRE(all.has_value());
            BOOST_REQUIRE_EQUAL(all->size(), 2U);
            BOOST_REQUIRE_EQUAL((*all)[0], pps::schema_version{1});
            BOOST_REQUIRE_EQUAL((*all)[1], pps::schema_version{2});
        }

        info(
          "get_schema_by_version reaches a soft-deleted version with deleted");
        {
            auto missing
              = sut.get_schema_by_version(multi, pps::schema_version{1}, rtc)
                  .get();
            BOOST_REQUIRE(!missing.has_value());
            BOOST_REQUIRE(
              std::holds_alternative<rc::version_not_found>(missing.error()));

            auto found = sut
                           .get_schema_by_version(
                             multi,
                             pps::schema_version{1},
                             rtc,
                             pps::include_deleted::yes)
                           .get();
            BOOST_REQUIRE(found.has_value());
            BOOST_REQUIRE_EQUAL(found->version, pps::schema_version{1});
            // Only the per-version response carries an explicit deleted flag;
            // confirm it round-trips as the source-reported flag.
            BOOST_REQUIRE(found->deleted == pps::is_deleted::yes);
        }

        info("list_subjects hides a fully-deleted subject without deleted");
        {
            auto live = sut.list_subjects(rtc).get();
            BOOST_REQUIRE(live.has_value());
            BOOST_REQUIRE(!contains(live.value(), solo));
            BOOST_REQUIRE(contains(live.value(), multi));

            auto all = sut.list_subjects(rtc, pps::include_deleted::yes).get();
            BOOST_REQUIRE(all.has_value());
            BOOST_REQUIRE(contains(all.value(), solo));
        }
    }

    // Kept after the delete section but before the subject mode is set to
    // READONLY below: a subject in read-only mode rejects config writes too.
    info(
      "get_subject_config reflects a subject config set via "
      "PUT /config/<subject>");
    {
        // A subject-level override, distinct from the global compatibility.
        put_subject_config(seed, "multi", "NONE");

        // Without defaultToGlobal we read the subject's own override back.
        auto own = sut.get_subject_config(multi, rtc).get();
        BOOST_REQUIRE(own.has_value());
        BOOST_REQUIRE(own->level == rc::registry_compatibility_level::none);
        BOOST_REQUIRE_EQUAL(own->raw, "NONE");

        // The effective config agrees: a subject override wins over the global.
        auto effective
          = sut.get_subject_config(multi, rtc, pps::default_to_global::yes)
              .get();
        BOOST_REQUIRE(effective.has_value());
        BOOST_REQUIRE(
          effective->level == rc::registry_compatibility_level::none);
    }

    // Kept after the delete section: setting "multi" READONLY would block the
    // soft-deletes above.
    info(
      "get_subject_mode reflects a subject mode set via PUT /mode/<subject>");
    {
        put_subject_mode(seed, "multi", "READONLY");

        // Without defaultToGlobal we read the subject's own override back.
        auto own = sut.get_subject_mode(multi, rtc).get();
        BOOST_REQUIRE(own.has_value());
        BOOST_REQUIRE(own->mode == rc::registry_mode::read_only);
        BOOST_REQUIRE_EQUAL(own->raw, "READONLY");

        // The effective mode agrees: a subject override wins over the global.
        auto effective
          = sut.get_subject_mode(multi, rtc, pps::default_to_global::yes).get();
        BOOST_REQUIRE(effective.has_value());
        BOOST_REQUIRE(effective->mode == rc::registry_mode::read_only);
    }

    info("get_config reflects a compatibility change made via PUT /config");
    {
        // PUT uses the "compatibility" field; GET returns "compatibilityLevel".
        put_global_config(seed, "FULL");
        auto res = sut.get_config(rtc).get();
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(res->level == rc::registry_compatibility_level::full);
        BOOST_REQUIRE_EQUAL(res->raw, "FULL");
    }

    // Kept last: switching the global mode to READONLY would reject the schema
    // registrations and soft-deletes the earlier sections rely on.
    info("get_mode reflects a mode change made via PUT /mode");
    {
        put_global_mode(seed, "READONLY");
        auto ro = sut.get_mode(rtc).get();
        BOOST_REQUIRE(ro.has_value());
        BOOST_REQUIRE(ro->mode == rc::registry_mode::read_only);
        BOOST_REQUIRE_EQUAL(ro->raw, "READONLY");

        // Reading tracks a change in the other direction too (and restores the
        // registry so it isn't left read-only).
        put_global_mode(seed, "READWRITE");
        auto rw = sut.get_mode(rtc).get();
        BOOST_REQUIRE(rw.has_value());
        BOOST_REQUIRE(rw->mode == rc::registry_mode::read_write);
    }

    sut.shutdown().get();
}
