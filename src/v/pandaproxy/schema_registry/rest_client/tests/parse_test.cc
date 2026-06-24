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
#include "pandaproxy/schema_registry/rest_client/parse.h"
#include "pandaproxy/schema_registry/types.h"
#include "ssx/sformat.h"
#include "test_utils/test.h"

#include <seastar/core/temporary_buffer.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <vector>

namespace pandaproxy::schema_registry::rest_client {

namespace {

// Build an iobuf whose bytes are split into separate fragments of at most
// `chunk_size` bytes, to exercise the streaming parser across fragment
// boundaries (including values that span fragments).
iobuf fragmented_iobuf(std::string_view s, size_t chunk_size) {
    iobuf buf;
    for (size_t i = 0; i < s.size(); i += chunk_size) {
        auto piece = s.substr(i, chunk_size);
        ss::temporary_buffer<char> frag{piece.size()};
        std::ranges::copy(piece, frag.get_write());
        buf.append(std::move(frag));
    }
    return buf;
}

// Linearize the raw schema text out of a parsed stored_schema for comparison.
ss::sstring schema_text(const stored_schema& s) {
    return s.schema.def().raw()().linearize_to_string();
}

} // namespace

TEST_CORO(parse_subjects_test, empty_array) {
    auto res = co_await parse_subjects(
      iobuf::from("[]"), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_TRUE_CORO(res.value().empty());
}

TEST_CORO(parse_subjects_test, mixed_subjects_enabled) {
    // bare, qualified, colon-in-subject, leading-':'-without-'.', and explicit
    // default context. Element order is preserved.
    auto res = co_await parse_subjects(
      iobuf::from(
        R"(["bare", ":.ctx:sub", ":.ctx:a:b:c", ":no-dot", ":.:def"])"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value();
    ASSERT_EQ_CORO(s.size(), size_t{5});
    ASSERT_EQ_CORO(s[0], context_subject(default_context, subject{"bare"}));
    ASSERT_EQ_CORO(s[1], context_subject(context{".ctx"}, subject{"sub"}));
    ASSERT_EQ_CORO(s[2], context_subject(context{".ctx"}, subject{"a:b:c"}));
    ASSERT_EQ_CORO(s[3], context_subject(default_context, subject{":no-dot"}));
    ASSERT_EQ_CORO(s[4], context_subject(default_context, subject{"def"}));
}

TEST_CORO(parse_subjects_test, qualified_disabled_is_literal) {
    // With the policy disabled, a ":.ctx:sub" element is taken verbatim as a
    // default-context subject rather than being split.
    auto res = co_await parse_subjects(
      iobuf::from(R"([":.ctx:sub", "bare"])"), qualified_subjects_enabled::no);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value();
    ASSERT_EQ_CORO(s.size(), size_t{2});
    ASSERT_EQ_CORO(
      s[0], context_subject(default_context, subject{":.ctx:sub"}));
    ASSERT_EQ_CORO(s[1], context_subject(default_context, subject{"bare"}));
}

TEST_CORO(parse_subjects_test, trailing_content_after_array_is_error) {
    // The subjects body is exactly one array; content after the closing ']' is
    // rejected, not ignored.
    for (std::string_view body :
         {R"(["a"] "more")", R"(["a"][])", R"(["a"]garbage)", R"(["a"],)"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subjects(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subjects_test, trailing_whitespace_is_ok) {
    // Whitespace after the array is fine; the parser skips it to reach EOF.
    auto res = co_await parse_subjects(
      iobuf::from("[\"a\"]  \n\t "), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value().size(), size_t{1});
    ASSERT_EQ_CORO(
      res.value()[0], context_subject(default_context, subject{"a"}));
}

TEST_CORO(parse_subjects_test, not_an_array_is_error) {
    for (std::string_view body :
         {R"({})", R"("just-a-string")", "42", "null", "true"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subjects(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subjects_test, non_string_element_is_error) {
    for (std::string_view body :
         {R"(["ok", 42])",
          R"(["ok", null])",
          R"(["ok", {}])",
          R"(["ok", ["nested"]])"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subjects(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subjects_test, malformed_or_truncated_is_error) {
    for (std::string_view body :
         {"", "[", R"(["a")", R"(["a",)", R"(["unterminated)", "not json"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subjects(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subjects_test, fragmented_input) {
    // One byte per fragment: forces subjects (and the array structure) to span
    // fragment boundaries.
    constexpr std::string_view body = R"(["bare", ":.ctx:sub", ":.ctx:a:b:c"])";
    auto res = co_await parse_subjects(
      fragmented_iobuf(body, 1), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value();
    ASSERT_EQ_CORO(s.size(), size_t{3});
    ASSERT_EQ_CORO(s[0], context_subject(default_context, subject{"bare"}));
    ASSERT_EQ_CORO(s[1], context_subject(context{".ctx"}, subject{"sub"}));
    ASSERT_EQ_CORO(s[2], context_subject(context{".ctx"}, subject{"a:b:c"}));
}

TEST_CORO(parse_subjects_test, round_trip) {
    // Build the wire form from a set of subjects, parse it back, and assert we
    // recover the originals (a lightweight stand-in until fuzzing exists).
    std::vector<context_subject> expected{
      context_subject(default_context, subject{"bare"}),
      context_subject(context{".ctx"}, subject{"sub"}),
      context_subject(context{".env"}, subject{"topic-value"}),
      context_subject(default_context, subject{"with.dots"}),
    };

    iobuf body;
    body.append("[", 1);
    for (size_t i = 0; i < expected.size(); ++i) {
        auto elem = ssx::sformat(
          R"({}"{}")", i == 0 ? "" : ",", expected[i].to_string());
        body.append(elem.data(), elem.size());
    }
    body.append("]", 1);

    auto res = co_await parse_subjects(
      std::move(body), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value();
    ASSERT_EQ_CORO(s.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        SCOPED_TRACE(i);
        ASSERT_EQ_CORO(s[i], expected[i]);
    }
}

TEST_CORO(parse_subject_versions_test, basic) {
    auto res = co_await parse_subject_versions(iobuf::from("[1, 2, 3]"));
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value();
    ASSERT_EQ_CORO(s.size(), size_t{3});
    ASSERT_EQ_CORO(s[0], schema_version{1});
    ASSERT_EQ_CORO(s[1], schema_version{2});
    ASSERT_EQ_CORO(s[2], schema_version{3});
}

TEST_CORO(parse_subject_versions_test, empty_array) {
    auto res = co_await parse_subject_versions(iobuf::from("[]"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_TRUE_CORO(res.value().empty());
}

TEST_CORO(parse_subject_versions_test, gaps_and_single) {
    // Version numbers need not be contiguous; deleted versions leave gaps.
    auto res = co_await parse_subject_versions(iobuf::from("[1, 3]"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value().size(), size_t{2});
    ASSERT_EQ_CORO(res.value()[0], schema_version{1});
    ASSERT_EQ_CORO(res.value()[1], schema_version{3});

    auto one = co_await parse_subject_versions(iobuf::from("[1]"));
    ASSERT_TRUE_CORO(one.has_value());
    ASSERT_EQ_CORO(one.value().size(), size_t{1});
    ASSERT_EQ_CORO(one.value()[0], schema_version{1});
}

TEST_CORO(parse_subject_versions_test, rejects_invalid_elements) {
    for (std::string_view body :
         {R"([-2])",         // negative (deletedAsNegative not supported)
          R"([1, -2, 3])",   // mixed signs
          R"([0])",          // zero never occurs
          R"([2147483648])", // > INT32_MAX
          R"([1.5])",        // non-integer (double)
          R"([1e2])",        // scientific notation -> double
          R"(["1"])",        // string
          R"([null])",
          R"([{}])",
          R"([[1]])"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subject_versions(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subject_versions_test, trailing_content_is_error) {
    for (std::string_view body : {R"([1] 2)", R"([1][])", R"([1]garbage)"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subject_versions(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subject_versions_test, trailing_whitespace_is_ok) {
    auto res = co_await parse_subject_versions(iobuf::from("[1]  \n\t "));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value().size(), size_t{1});
}

TEST_CORO(parse_subject_versions_test, malformed_or_truncated_is_error) {
    for (std::string_view body : {"", "[", "[1", "[1,", "not json", "{}"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subject_versions(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subject_versions_test, fragmented_input) {
    // One byte per fragment: multi-digit numbers span fragment boundaries.
    constexpr std::string_view body = "[1, 22, 333]";
    auto res = co_await parse_subject_versions(fragmented_iobuf(body, 1));
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value();
    ASSERT_EQ_CORO(s.size(), size_t{3});
    ASSERT_EQ_CORO(s[0], schema_version{1});
    ASSERT_EQ_CORO(s[1], schema_version{22});
    ASSERT_EQ_CORO(s[2], schema_version{333});
}

TEST_CORO(parse_subject_versions_test, round_trip) {
    std::vector<schema_version> expected{
      schema_version{1},
      schema_version{2},
      schema_version{5},
      schema_version{std::numeric_limits<int32_t>::max()},
    };

    iobuf body;
    body.append("[", 1);
    for (size_t i = 0; i < expected.size(); ++i) {
        auto elem = ssx::sformat("{}{}", i == 0 ? "" : ",", expected[i]());
        body.append(elem.data(), elem.size());
    }
    body.append("]", 1);

    auto res = co_await parse_subject_versions(std::move(body));
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value();
    ASSERT_EQ_CORO(s.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        SCOPED_TRACE(i);
        ASSERT_EQ_CORO(s[i], expected[i]);
    }
}

TEST_CORO(parse_subject_version_test, minimal_avro_defaults) {
    auto res = co_await parse_subject_version(
      iobuf::from(
        R"({"subject":"User","version":1,"id":100001,)"
        R"("schema":"{\"type\":\"string\"}"})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_TRUE_CORO(res.value().unknown_fields.empty());
    const auto& s = res.value().schema;
    ASSERT_EQ_CORO(
      s.schema.sub(), (context_subject{default_context, subject{"User"}}));
    ASSERT_EQ_CORO(s.version, schema_version{1});
    ASSERT_EQ_CORO(s.id, schema_id{100001});
    ASSERT_EQ_CORO(s.schema.type(), schema_type::avro); // default
    ASSERT_EQ_CORO(s.deleted, is_deleted::no);          // default
    ASSERT_TRUE_CORO(s.schema.def().refs().empty());
    ASSERT_EQ_CORO(schema_text(s), R"({"type":"string"})");
}

TEST_CORO(parse_subject_version_test, schema_types) {
    auto json = co_await parse_subject_version(
      iobuf::from(
        R"({"subject":"r","version":1,"id":2,"schemaType":"JSON",)"
        R"("schema":"{\"type\":\"object\"}"})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(json.has_value());
    ASSERT_EQ_CORO(json.value().schema.schema.type(), schema_type::json);

    // PROTOBUF: the .proto text (quotes and newlines) is preserved verbatim.
    auto proto = co_await parse_subject_version(
      iobuf::from(
        R"({"subject":"r","version":1,"id":2,"schemaType":"PROTOBUF",)"
        R"("schema":"syntax = \"proto3\";\nmessage M {}\n"})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(proto.has_value());
    const auto& p = proto.value().schema;
    ASSERT_EQ_CORO(p.schema.type(), schema_type::protobuf);
    ASSERT_EQ_CORO(schema_text(p), "syntax = \"proto3\";\nmessage M {}\n");
}

TEST_CORO(parse_subject_version_test, full_object_maps_all_fields) {
    auto res = co_await parse_subject_version(
      iobuf::from(
        R"({"subject":":.ctx:MyRecord","version":2,"id":12,)"
        R"("schemaType":"AVRO","references":[)"
        R"({"name":"com.acme.Referenced","subject":"childSubject","version":1}],)"
        R"("schema":"{\"type\":\"record\"}","deleted":true})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_TRUE_CORO(res.value().unknown_fields.empty());
    const auto& s = res.value().schema;
    ASSERT_EQ_CORO(
      s.schema.sub(), (context_subject{context{".ctx"}, subject{"MyRecord"}}));
    ASSERT_EQ_CORO(s.version, schema_version{2});
    ASSERT_EQ_CORO(s.id, schema_id{12});
    ASSERT_EQ_CORO(s.schema.type(), schema_type::avro);
    ASSERT_EQ_CORO(s.deleted, is_deleted::yes);
    ASSERT_EQ_CORO(schema_text(s), R"({"type":"record"})");
    const auto& refs = s.schema.def().refs();
    ASSERT_EQ_CORO(refs.size(), size_t{1});
    ASSERT_EQ_CORO(refs[0].name, "com.acme.Referenced");
    ASSERT_EQ_CORO(
      refs[0].sub.sub,
      (context_subject{default_context, subject{"childSubject"}}));
    ASSERT_EQ_CORO(refs[0].version, schema_version{1});
}

TEST_CORO(parse_subject_version_test, reference_subject_honors_policy) {
    constexpr std::string_view body
      = R"({"subject":"r","version":1,"id":2,"schema":"x",)"
        R"("references":[{"name":"n","subject":":.ctx:Sub","version":1}]})";

    auto on = co_await parse_subject_version(
      iobuf::from(body), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(on.has_value());
    const auto& on_ref = on.value().schema.schema.def().refs()[0];
    ASSERT_EQ_CORO(on_ref.sub.qualified, is_qualified::yes);
    ASSERT_EQ_CORO(
      on_ref.sub.sub, (context_subject{context{".ctx"}, subject{"Sub"}}));

    auto off = co_await parse_subject_version(
      iobuf::from(body), qualified_subjects_enabled::no);
    ASSERT_TRUE_CORO(off.has_value());
    const auto& off_ref = off.value().schema.schema.def().refs()[0];
    ASSERT_EQ_CORO(off_ref.sub.qualified, is_qualified::no);
    ASSERT_EQ_CORO(
      off_ref.sub.sub,
      (context_subject{default_context, subject{":.ctx:Sub"}}));
}

TEST_CORO(parse_subject_version_test, records_unmodeled_fields) {
    // guid/ts/ruleSet/schemaTags (and a future field) are not mapped into the
    // schema, but their top-level names are recorded so the caller can decide
    // whether dropping them is acceptable. A nested object/array under such a
    // field is skipped without descending into it. metadata is special: its
    // modeled `properties` is captured, while an unmodeled sub-key
    // (`sensitive`) is reported under a `metadata.` prefix.
    auto res = co_await parse_subject_version(
      iobuf::from(
        R"({"guid":"abc","ts":1715000000000,"subject":"User","version":1,)"
        R"("id":7,"schema":"x","schemaType":"AVRO",)"
        R"("ruleSet":{"domainRules":[{"name":"r","kind":"TRANSFORM"}]},)"
        R"("metadata":{"properties":{"owner":"team-a"},"sensitive":["ssn"]},)"
        R"("schemaTags":[{"tags":["PII"]}],"futureField":[1,2,3]})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value().schema;
    ASSERT_EQ_CORO(
      s.schema.sub(), (context_subject{default_context, subject{"User"}}));
    ASSERT_EQ_CORO(s.version, schema_version{1});
    ASSERT_EQ_CORO(s.id, schema_id{7});
    // metadata.properties is captured into the schema.
    ASSERT_TRUE_CORO(s.schema.def().meta().has_value());
    ASSERT_TRUE_CORO(s.schema.def().meta()->properties.has_value());
    const auto& props = *s.schema.def().meta()->properties;
    ASSERT_EQ_CORO(props.size(), size_t{1});
    ASSERT_EQ_CORO(props.at("owner"), "team-a");
    // The unmodeled keys are reported, in encounter order; modeled nested keys
    // (metadata.properties, ruleSet.domainRules, ...) are not. metadata's
    // unmodeled `sensitive` sub-key is reported under a `metadata.` prefix.
    const auto& unknown = res.value().unknown_fields;
    ASSERT_EQ_CORO(unknown.size(), size_t{6});
    ASSERT_EQ_CORO(unknown[0], "guid");
    ASSERT_EQ_CORO(unknown[1], "ts");
    ASSERT_EQ_CORO(unknown[2], "ruleSet");
    ASSERT_EQ_CORO(unknown[3], "metadata.sensitive");
    ASSERT_EQ_CORO(unknown[4], "schemaTags");
    ASSERT_EQ_CORO(unknown[5], "futureField");
}

TEST_CORO(parse_subject_version_test, metadata_properties_coercion) {
    // metadata.properties is a string map; numbers and booleans are coerced to
    // strings (matching the write path), and key order is normalized by the
    // underlying btree_map.
    auto res = co_await parse_subject_version(
      iobuf::from(
        R"({"subject":"r","version":1,"id":2,"schema":"x","metadata":)"
        R"({"properties":{"owner":"team-a","count":3,"ratio":1.5,)"
        R"("enabled":true,"hidden":false}}})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_TRUE_CORO(res.value().unknown_fields.empty());
    const auto& meta = res.value().schema.schema.def().meta();
    ASSERT_TRUE_CORO(meta.has_value());
    ASSERT_TRUE_CORO(meta->properties.has_value());
    const auto& props = *meta->properties;
    ASSERT_EQ_CORO(props.size(), size_t{5});
    ASSERT_EQ_CORO(props.at("owner"), "team-a");
    ASSERT_EQ_CORO(props.at("count"), "3");
    ASSERT_EQ_CORO(props.at("ratio"), "1.5");
    ASSERT_EQ_CORO(props.at("enabled"), "true");
    ASSERT_EQ_CORO(props.at("hidden"), "false");
}

TEST_CORO(parse_subject_version_test, metadata_present_without_properties) {
    // metadata carrying only unmodeled sub-keys still yields a present (but
    // empty) schema_metadata; each unmodeled sub-key is reported with a prefix.
    auto res = co_await parse_subject_version(
      iobuf::from(
        R"({"subject":"r","version":1,"id":2,"schema":"x","metadata":)"
        R"({"tags":{"f":["PII"]},"sensitive":["ssn"]}})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& meta = res.value().schema.schema.def().meta();
    ASSERT_TRUE_CORO(meta.has_value());
    ASSERT_FALSE_CORO(meta->properties.has_value());
    const auto& unknown = res.value().unknown_fields;
    ASSERT_EQ_CORO(unknown.size(), size_t{2});
    ASSERT_EQ_CORO(unknown[0], "metadata.tags");
    ASSERT_EQ_CORO(unknown[1], "metadata.sensitive");
}

TEST_CORO(parse_subject_version_test, metadata_empty_and_null) {
    // metadata: {} is present-but-empty; metadata: null is treated as absent.
    auto empty_obj = co_await parse_subject_version(
      iobuf::from(R"({"subject":"r","version":1,"id":2,"metadata":{}})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(empty_obj.has_value());
    ASSERT_TRUE_CORO(empty_obj.value().unknown_fields.empty());
    ASSERT_TRUE_CORO(empty_obj.value().schema.schema.def().meta().has_value());

    auto null_meta = co_await parse_subject_version(
      iobuf::from(R"({"subject":"r","version":1,"id":2,"metadata":null})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(null_meta.has_value());
    ASSERT_TRUE_CORO(null_meta.value().unknown_fields.empty());
    ASSERT_FALSE_CORO(null_meta.value().schema.schema.def().meta().has_value());

    // metadata.properties: null leaves properties absent (metadata present).
    auto null_props = co_await parse_subject_version(
      iobuf::from(
        R"({"subject":"r","version":1,"metadata":{"properties":null}})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(null_props.has_value());
    const auto& meta = null_props.value().schema.schema.def().meta();
    ASSERT_TRUE_CORO(meta.has_value());
    ASSERT_FALSE_CORO(meta->properties.has_value());
}

TEST_CORO(parse_subject_version_test, absent_fields_use_sentinels_not_error) {
    // Permissive: missing fields are NOT errors; they take defaults/sentinels.
    auto empty = co_await parse_subject_version(
      iobuf::from("{}"), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(empty.has_value());
    const auto& e = empty.value().schema;
    ASSERT_EQ_CORO(e.version, invalid_schema_version);
    ASSERT_EQ_CORO(e.id, invalid_schema_id);
    ASSERT_EQ_CORO(e.schema.sub(), invalid_subject);
    ASSERT_EQ_CORO(e.schema.type(), schema_type::avro);
    ASSERT_EQ_CORO(e.deleted, is_deleted::no);

    auto no_schema = co_await parse_subject_version(
      iobuf::from(R"({"subject":"r","version":1,"id":2})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(no_schema.has_value());
    ASSERT_EQ_CORO(no_schema.value().schema.version, schema_version{1});
}

TEST_CORO(parse_subject_version_test, id_zero_is_valid) {
    // Unlike version (always >= 1), id 0 is legal: upstream permits id 0 on
    // import, so a server may return it.
    auto res = co_await parse_subject_version(
      iobuf::from(R"({"subject":"r","version":1,"id":0,"schema":"x"})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value().schema.id, schema_id{0});
}

TEST_CORO(parse_subject_version_test, rejects_unrepresentable) {
    for (std::string_view body :
         {R"({"schemaType":"YAML","subject":"r","schema":"x"})", // unknown enum
          R"({"version":"1","subject":"r"})",    // version wrong type
          R"({"id":1.5,"subject":"r"})",         // id non-integer
          R"({"deleted":"no","subject":"r"})",   // deleted wrong type
          R"({"version":0,"subject":"r"})",      // version non-positive
          R"({"id":-1,"subject":"r"})",          // id negative
          R"({"id":2147483648,"subject":"r"})",  // > INT32_MAX
          R"({"references":[{"version":"x"}]})", // bad reference element
          R"({"references":5})",                 // references not an array
          R"({"metadata":5,"subject":"r"})",     // metadata not an object
          R"({"metadata":{"properties":5}})",    // properties not an object
          R"({"metadata":{"properties":{"k":["x"]}}})", // value not a scalar
          R"({"metadata":{"properties":{"k":null}}})",  // value null
          "[1,2,3]",                                    // not an object
          R"({"subject":"r")",                          // truncated
          R"({"subject":"r"}garbage)", // trailing content after }
          "not json"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subject_version(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subject_version_test, fragmented_input) {
    constexpr std::string_view body
      = R"({"subject":":.ctx:User","version":12,"id":34,"schemaType":"AVRO",)"
        R"("schema":"{\"type\":\"record\",\"name\":\"User\"}",)"
        R"("references":[{"name":"N","subject":"Sub","version":1}]})";
    auto res = co_await parse_subject_version(
      fragmented_iobuf(body, 1), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value().schema;
    ASSERT_EQ_CORO(
      s.schema.sub(), (context_subject{context{".ctx"}, subject{"User"}}));
    ASSERT_EQ_CORO(s.version, schema_version{12});
    ASSERT_EQ_CORO(s.id, schema_id{34});
    ASSERT_EQ_CORO(s.schema.def().refs().size(), size_t{1});
}

TEST_CORO(parse_error_body_test, full) {
    auto res = co_await parse_error_body(
      iobuf::from(R"({"error_code": 40401, "message": "Subject not found"})"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->error_code, 40401);
    ASSERT_EQ_CORO(res->message, "Subject not found");
}

TEST_CORO(parse_error_body_test, ignores_unknown_fields) {
    auto res = co_await parse_error_body(
      iobuf::from(R"({"extra": {"a": [1, 2]}, "error_code": 50001})"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->error_code, 50001);
    // message is optional; absent -> empty.
    ASSERT_EQ_CORO(res->message, "");
}

TEST_CORO(parse_error_body_test, missing_error_code_is_nullopt) {
    auto res = co_await parse_error_body(
      iobuf::from(R"({"message": "no code here"})"));
    ASSERT_FALSE_CORO(res.has_value());
}

TEST_CORO(parse_error_body_test, non_integer_error_code_is_nullopt) {
    auto res = co_await parse_error_body(
      iobuf::from(R"({"error_code": "40401"})"));
    ASSERT_FALSE_CORO(res.has_value());
}

TEST_CORO(parse_error_body_test, non_object_is_nullopt) {
    auto res = co_await parse_error_body(iobuf::from(R"([40401])"));
    ASSERT_FALSE_CORO(res.has_value());
}

TEST_CORO(parse_error_body_test, non_json_is_nullopt) {
    // Auth proxies may return an HTML or empty body instead of JSON.
    auto res = co_await parse_error_body(
      iobuf::from("<html>403 Forbidden</html>"));
    ASSERT_FALSE_CORO(res.has_value());
    auto empty = co_await parse_error_body(iobuf::from(""));
    ASSERT_FALSE_CORO(empty.has_value());
}

TEST_CORO(parse_error_body_test, trailing_content_is_nullopt) {
    // A complete object followed by garbage is not a valid JSON document.
    auto res = co_await parse_error_body(
      iobuf::from(R"({"error_code": 40401}garbage)"));
    ASSERT_FALSE_CORO(res.has_value());
    // Trailing whitespace is fine.
    auto ok = co_await parse_error_body(
      iobuf::from("{\"error_code\": 40401}  \n"));
    ASSERT_TRUE_CORO(ok.has_value());
    ASSERT_EQ_CORO(ok->error_code, 40401);
}

} // namespace pandaproxy::schema_registry::rest_client
