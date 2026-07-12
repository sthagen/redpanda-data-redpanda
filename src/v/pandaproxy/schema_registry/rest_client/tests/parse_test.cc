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

TEST_CORO(parse_contexts_test, basic) {
    // The default context (".") sorts first, followed by named, dot-prefixed
    // contexts. Each string is wrapped verbatim; order is preserved.
    auto res = co_await parse_contexts(
      iobuf::from(R"([".", ".dev", ".prod-eu"])"));
    ASSERT_TRUE_CORO(res.has_value());
    const auto& c = res.value();
    ASSERT_EQ_CORO(c.size(), size_t{3});
    ASSERT_EQ_CORO(c[0], default_context);
    ASSERT_EQ_CORO(c[1], context{".dev"});
    ASSERT_EQ_CORO(c[2], context{".prod-eu"});
}

TEST_CORO(parse_contexts_test, default_only) {
    // A brand-new/empty registry still reports the default context.
    auto res = co_await parse_contexts(iobuf::from(R"(["."])"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value().size(), size_t{1});
    ASSERT_EQ_CORO(res.value()[0], default_context);
}

TEST_CORO(parse_contexts_test, empty_array) {
    // A contextPrefix that matches nothing yields []; the parser accepts it
    // even though a live registry always has at least the default context.
    auto res = co_await parse_contexts(iobuf::from("[]"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_TRUE_CORO(res.value().empty());
}

TEST_CORO(parse_contexts_test, colon_form_taken_verbatim) {
    // Contexts are never colon-qualified in this response; a ":.ctx:"-looking
    // element is not split, it is wrapped verbatim as a context.
    auto res = co_await parse_contexts(iobuf::from(R"([":.ctx:"])"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value().size(), size_t{1});
    ASSERT_EQ_CORO(res.value()[0], context{":.ctx:"});
}

TEST_CORO(parse_contexts_test, fragmented_input) {
    // One byte per fragment: forces context names and the array structure to
    // span fragment boundaries.
    constexpr std::string_view body = R"([".", ".dev", ".prod-eu"])";
    auto res = co_await parse_contexts(fragmented_iobuf(body, 1));
    ASSERT_TRUE_CORO(res.has_value());
    const auto& c = res.value();
    ASSERT_EQ_CORO(c.size(), size_t{3});
    ASSERT_EQ_CORO(c[0], default_context);
    ASSERT_EQ_CORO(c[1], context{".dev"});
    ASSERT_EQ_CORO(c[2], context{".prod-eu"});
}

TEST_CORO(parse_contexts_test, not_an_array_is_error) {
    for (std::string_view body :
         {R"({})", R"("just-a-string")", "42", "null", "true"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_contexts(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_contexts_test, non_string_element_is_error) {
    for (std::string_view body :
         {R"([".", 42])",
          R"([".", null])",
          R"([".", {}])",
          R"([".", ["x"]])"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_contexts(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_contexts_test, trailing_content_after_array_is_error) {
    // The contexts body is exactly one array; content after the closing ']' is
    // rejected, not ignored.
    for (std::string_view body :
         {R"(["."] "more")", R"(["."][])", R"(["."]garbage)", R"(["."],)"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_contexts(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_contexts_test, trailing_whitespace_is_ok) {
    auto res = co_await parse_contexts(iobuf::from("[\".\"]  \n\t "));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value().size(), size_t{1});
    ASSERT_EQ_CORO(res.value()[0], default_context);
}

TEST_CORO(parse_contexts_test, malformed_or_truncated_is_error) {
    // Missing close bracket, dangling comma, unterminated string, non-JSON.
    for (std::string_view body :
         {"", "[", "[\".\"", "[\".\",", "[\".unterminated", "not json"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_contexts(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_mode_test, readwrite) {
    auto res = co_await parse_mode(iobuf::from(R"({"mode": "READWRITE"})"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->mode, registry_mode::read_write);
    ASSERT_EQ_CORO(res->raw, "READWRITE");
}

TEST_CORO(parse_mode_test, all_known_values_map_to_enumerators) {
    // Every value the Schema Registry REST API defines maps to its enumerator,
    // including READONLY_OVERRIDE (deprecated) and FORWARD — values Redpanda's
    // own server never emits but a Confluent-compatible one can. raw always
    // carries the verbatim wire string.
    const std::pair<std::string_view, registry_mode> cases[] = {
      {"READWRITE", registry_mode::read_write},
      {"READONLY", registry_mode::read_only},
      {"READONLY_OVERRIDE", registry_mode::read_only_override},
      {"IMPORT", registry_mode::import},
      {"FORWARD", registry_mode::forward},
    };
    for (const auto& [wire, expected] : cases) {
        SCOPED_TRACE(wire);
        auto res = co_await parse_mode(
          iobuf::from(ssx::sformat(R"({{"mode": "{}"}})", wire)));
        ASSERT_TRUE_CORO(res.has_value());
        ASSERT_EQ_CORO(res->mode, expected);
        ASSERT_EQ_CORO(res->raw, ss::sstring{wire});
    }
}

TEST_CORO(parse_mode_test, unknown_value_is_open_enum) {
    // An unrecognized mode is tolerated, not rejected: it maps to `unknown`
    // with the verbatim wire string preserved so nothing is lost.
    auto res = co_await parse_mode(iobuf::from(R"({"mode": "GALAXY_BRAIN"})"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->mode, registry_mode::unknown);
    ASSERT_EQ_CORO(res->raw, "GALAXY_BRAIN");
}

TEST_CORO(parse_mode_test, empty_value_is_unknown_not_error) {
    // Shape is strict, value is open: a present-but-empty string is not a shape
    // violation; the value simply maps to `unknown` (raw="").
    auto res = co_await parse_mode(iobuf::from(R"({"mode": ""})"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->mode, registry_mode::unknown);
    ASSERT_EQ_CORO(res->raw, "");
}

TEST_CORO(parse_mode_test, ignores_unknown_fields) {
    // Only `mode` is modeled; any other top-level field is skipped, whether it
    // precedes or follows `mode` and whatever its (possibly nested) value.
    for (std::string_view body :
         {R"({"mode": "READONLY", "extra": 123})",
          R"({"extra": {"a": [1, 2]}, "mode": "READONLY"})"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_mode(iobuf::from(body));
        ASSERT_TRUE_CORO(res.has_value());
        ASSERT_EQ_CORO(res->mode, registry_mode::read_only);
        ASSERT_EQ_CORO(res->raw, "READONLY");
    }
}

TEST_CORO(parse_mode_test, missing_mode_is_error) {
    // `mode` is the one field a successful response must carry.
    for (std::string_view body : {R"({})", R"({"other": 1})"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_mode(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_mode_test, non_object_is_error) {
    for (std::string_view body :
         {R"(["READWRITE"])", R"("READWRITE")", "42", "null", "true"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_mode(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_mode_test, non_string_mode_is_error) {
    for (std::string_view body :
         {R"({"mode": 5})",
          R"({"mode": null})",
          R"({"mode": ["READWRITE"]})",
          R"({"mode": {}})",
          R"({"mode": true})"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_mode(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_mode_test, trailing_content_after_object_is_error) {
    // The mode body is exactly one object; content after the closing '}' is
    // rejected, not ignored.
    for (std::string_view body :
         {R"({"mode": "READWRITE"} "more")",
          R"({"mode": "READWRITE"}{})",
          R"({"mode": "READWRITE"}garbage)",
          R"({"mode": "READWRITE"},)"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_mode(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_mode_test, trailing_whitespace_is_ok) {
    auto res = co_await parse_mode(
      iobuf::from("{\"mode\": \"READWRITE\"}  \n\t "));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->mode, registry_mode::read_write);
}

TEST_CORO(parse_mode_test, fragmented_input) {
    // One byte per fragment forces the object structure and the mode value to
    // span parser fragment boundaries.
    auto res = co_await parse_mode(
      fragmented_iobuf(R"({"mode": "READONLY_OVERRIDE"})", 1));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->mode, registry_mode::read_only_override);
    ASSERT_EQ_CORO(res->raw, "READONLY_OVERRIDE");
}

TEST_CORO(parse_mode_test, malformed_or_truncated_is_error) {
    // Missing close brace, missing value, unterminated string, an unclosed
    // object after a complete pair, a dangling key at EOF, and non-JSON — every
    // route ends in an error, never a partial result.
    for (std::string_view body :
         {"",
          "{",
          R"({"mode")",
          R"({"mode":)",
          R"({"mode": "READWRITE)",
          R"({"mode": "READWRITE")",
          R"({"x": 1, "mode")",
          "not json"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_mode(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_mode_test, unknown_field_missing_value_is_error) {
    // An unmodeled field whose value is absent (a truncated object) faults the
    // skip-unknown path; the exception firewall reports it as a parse_error
    // rather than letting the parser exception escape.
    auto res = co_await parse_mode(iobuf::from(R"({"other"})"));
    ASSERT_FALSE_CORO(res.has_value());
}

TEST(registry_mode_test, to_string_view_and_format) {
    // The display/log form of every mode, including the open-enum `unknown`
    // sentinel. The known arms are also hit via registry_mode_from_wire, but
    // this pins the full contract and the format_to path.
    EXPECT_EQ(to_string_view(registry_mode::read_write), "READWRITE");
    EXPECT_EQ(to_string_view(registry_mode::read_only), "READONLY");
    EXPECT_EQ(
      to_string_view(registry_mode::read_only_override), "READONLY_OVERRIDE");
    EXPECT_EQ(to_string_view(registry_mode::import), "IMPORT");
    EXPECT_EQ(to_string_view(registry_mode::forward), "FORWARD");
    EXPECT_EQ(to_string_view(registry_mode::unknown), "{unknown}");
    // The fmt formatter (via format_to) mirrors to_string_view.
    EXPECT_EQ(fmt::format("{}", registry_mode::forward), "FORWARD");
    EXPECT_EQ(fmt::format("{}", registry_mode::unknown), "{unknown}");
}

TEST_CORO(parse_config_test, compatibility_level_only) {
    // Redpanda's server emits just compatibilityLevel; nothing is recorded as
    // an unknown field.
    auto res = co_await parse_config(
      iobuf::from(R"({"compatibilityLevel": "BACKWARD"})"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->level, registry_compatibility_level::backward);
    ASSERT_EQ_CORO(res->raw, "BACKWARD");
    ASSERT_TRUE_CORO(res->unknown_fields.empty());
}

TEST_CORO(parse_config_test, all_known_levels_map_to_enumerators) {
    const std::pair<std::string_view, registry_compatibility_level> cases[] = {
      {"NONE", registry_compatibility_level::none},
      {"BACKWARD", registry_compatibility_level::backward},
      {"BACKWARD_TRANSITIVE",
       registry_compatibility_level::backward_transitive},
      {"FORWARD", registry_compatibility_level::forward},
      {"FORWARD_TRANSITIVE", registry_compatibility_level::forward_transitive},
      {"FULL", registry_compatibility_level::full},
      {"FULL_TRANSITIVE", registry_compatibility_level::full_transitive},
    };
    for (const auto& [wire, expected] : cases) {
        SCOPED_TRACE(wire);
        auto res = co_await parse_config(
          iobuf::from(ssx::sformat(R"({{"compatibilityLevel": "{}"}})", wire)));
        ASSERT_TRUE_CORO(res.has_value());
        ASSERT_EQ_CORO(res->level, expected);
        ASSERT_EQ_CORO(res->raw, ss::sstring{wire});
    }
}

TEST_CORO(parse_config_test, unknown_level_is_open_enum) {
    // An unrecognized level is tolerated, not rejected: it maps to `unknown`
    // with the verbatim wire string preserved.
    auto res = co_await parse_config(
      iobuf::from(R"({"compatibilityLevel": "SIDEWAYS"})"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->level, registry_compatibility_level::unknown);
    ASSERT_EQ_CORO(res->raw, "SIDEWAYS");
}

TEST_CORO(parse_config_test, empty_level_is_unknown_not_error) {
    // Shape is strict, value is open: a present-but-empty string maps to
    // `unknown` (raw="") rather than failing.
    auto res = co_await parse_config(
      iobuf::from(R"({"compatibilityLevel": ""})"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->level, registry_compatibility_level::unknown);
    ASSERT_EQ_CORO(res->raw, "");
}

TEST_CORO(parse_config_test, records_unmodeled_fields) {
    // A Confluent registry may return a rich object. Only compatibilityLevel is
    // modeled; every other top-level field's name is recorded (in encounter
    // order) and its value skipped, whatever its shape (scalar, object, null).
    auto res = co_await parse_config(
      iobuf::from(
        R"({"compatibilityLevel": "FULL", "normalize": true, )"
        R"("validateFields": false, )"
        R"("defaultMetadata": {"properties": {"o": "a"}}, )"
        R"("defaultRuleSet": null})"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->level, registry_compatibility_level::full);
    ASSERT_EQ_CORO(res->unknown_fields.size(), size_t{4});
    ASSERT_EQ_CORO(res->unknown_fields[0], "normalize");
    ASSERT_EQ_CORO(res->unknown_fields[1], "validateFields");
    ASSERT_EQ_CORO(res->unknown_fields[2], "defaultMetadata");
    ASSERT_EQ_CORO(res->unknown_fields[3], "defaultRuleSet");
}

TEST_CORO(parse_config_test, compatibility_level_need_not_come_first) {
    auto res = co_await parse_config(
      iobuf::from(R"({"normalize": true, "compatibilityLevel": "FORWARD"})"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->level, registry_compatibility_level::forward);
    ASSERT_EQ_CORO(res->unknown_fields.size(), size_t{1});
    ASSERT_EQ_CORO(res->unknown_fields[0], "normalize");
}

TEST_CORO(parse_config_test, missing_compatibility_level_is_error) {
    // compatibilityLevel is the one field a config response must carry.
    for (std::string_view body : {R"({})", R"({"normalize": true})"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_config(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_config_test, non_object_is_error) {
    for (std::string_view body :
         {R"(["BACKWARD"])", R"("BACKWARD")", "42", "null", "true"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_config(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_config_test, non_string_compatibility_level_is_error) {
    for (std::string_view body :
         {R"({"compatibilityLevel": 5})",
          R"({"compatibilityLevel": null})",
          R"({"compatibilityLevel": ["BACKWARD"]})",
          R"({"compatibilityLevel": {}})",
          R"({"compatibilityLevel": true})"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_config(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_config_test, trailing_content_after_object_is_error) {
    for (std::string_view body :
         {R"({"compatibilityLevel": "NONE"} "more")",
          R"({"compatibilityLevel": "NONE"}{})",
          R"({"compatibilityLevel": "NONE"}garbage)",
          R"({"compatibilityLevel": "NONE"},)"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_config(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_config_test, trailing_whitespace_is_ok) {
    auto res = co_await parse_config(
      iobuf::from("{\"compatibilityLevel\": \"FULL\"}  \n\t "));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->level, registry_compatibility_level::full);
}

TEST_CORO(parse_config_test, fragmented_input) {
    // One byte per fragment forces the object structure, the level value, and
    // an unmodeled field to span parser fragment boundaries.
    auto res = co_await parse_config(fragmented_iobuf(
      R"({"compatibilityLevel": "BACKWARD_TRANSITIVE", "normalize": true})",
      1));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(
      res->level, registry_compatibility_level::backward_transitive);
    ASSERT_EQ_CORO(res->raw, "BACKWARD_TRANSITIVE");
    ASSERT_EQ_CORO(res->unknown_fields.size(), size_t{1});
    ASSERT_EQ_CORO(res->unknown_fields[0], "normalize");
}

TEST_CORO(parse_config_test, malformed_or_truncated_is_error) {
    for (std::string_view body :
         {"",
          "{",
          R"({"compatibilityLevel")",
          R"({"compatibilityLevel":)",
          R"({"compatibilityLevel": "BACKWARD)",
          R"({"compatibilityLevel": "BACKWARD")",
          R"({"normalize": true, "compatibilityLevel")",
          "not json"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_config(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_config_test, unknown_field_missing_value_is_error) {
    // As parse_mode: an unmodeled field with no value trips the skip path; the
    // firewall turns the parser exception into a parse_error.
    auto res = co_await parse_config(iobuf::from(R"({"other"})"));
    ASSERT_FALSE_CORO(res.has_value());
}

TEST(registry_compatibility_level_test, to_string_view_and_format) {
    // The display/log form of every level, including the open-enum `unknown`
    // sentinel. Known arms are also hit via
    // registry_compatibility_level_from_wire, but this pins the full contract
    // and the format_to path.
    using cl = registry_compatibility_level;
    EXPECT_EQ(to_string_view(cl::none), "NONE");
    EXPECT_EQ(to_string_view(cl::backward), "BACKWARD");
    EXPECT_EQ(to_string_view(cl::backward_transitive), "BACKWARD_TRANSITIVE");
    EXPECT_EQ(to_string_view(cl::forward), "FORWARD");
    EXPECT_EQ(to_string_view(cl::forward_transitive), "FORWARD_TRANSITIVE");
    EXPECT_EQ(to_string_view(cl::full), "FULL");
    EXPECT_EQ(to_string_view(cl::full_transitive), "FULL_TRANSITIVE");
    EXPECT_EQ(to_string_view(cl::unknown), "{unknown}");
    // The fmt formatter (via format_to) mirrors to_string_view.
    EXPECT_EQ(fmt::format("{}", cl::full_transitive), "FULL_TRANSITIVE");
    EXPECT_EQ(fmt::format("{}", cl::unknown), "{unknown}");
}

TEST_CORO(parse_schema_id_subject_versions_test, basic) {
    auto res = co_await parse_schema_id_subject_versions(
      iobuf::from(
        R"([{"subject": "s1", "version": 1}, {"subject": "s2", "version": 3}])"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& v = res.value();
    ASSERT_EQ_CORO(v.size(), size_t{2});
    ASSERT_EQ_CORO(v[0].sub, context_subject(default_context, subject{"s1"}));
    ASSERT_EQ_CORO(v[0].version, schema_version{1});
    ASSERT_EQ_CORO(v[1].sub, context_subject(default_context, subject{"s2"}));
    ASSERT_EQ_CORO(v[1].version, schema_version{3});
}

TEST_CORO(parse_schema_id_subject_versions_test, qualified_subject) {
    // A non-default context comes back context-qualified and is split under the
    // qualified policy, exactly like parse_subjects.
    auto res = co_await parse_schema_id_subject_versions(
      iobuf::from(R"([{"subject": ":.ctx:orders", "version": 2}])"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value().size(), size_t{1});
    ASSERT_EQ_CORO(
      res.value()[0].sub, context_subject(context{".ctx"}, subject{"orders"}));
    ASSERT_EQ_CORO(res.value()[0].version, schema_version{2});
}

TEST_CORO(
  parse_schema_id_subject_versions_test, qualified_disabled_is_literal) {
    auto res = co_await parse_schema_id_subject_versions(
      iobuf::from(R"([{"subject": ":.ctx:orders", "version": 2}])"),
      qualified_subjects_enabled::no);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(
      res.value()[0].sub,
      context_subject(default_context, subject{":.ctx:orders"}));
}

TEST_CORO(parse_schema_id_subject_versions_test, empty_array) {
    // A valid outcome: the id exists but no pair matches the filters.
    auto res = co_await parse_schema_id_subject_versions(
      iobuf::from("[]"), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_TRUE_CORO(res.value().empty());
}

TEST_CORO(parse_schema_id_subject_versions_test, unknown_keys_are_skipped) {
    // Extra keys (in any order) are tolerated; subject/version still parse.
    auto res = co_await parse_schema_id_subject_versions(
      iobuf::from(
        R"([{"guid": "g", "version": 5, "subject": "s", "ruleSet": {"x": [1]}}])"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value().size(), size_t{1});
    ASSERT_EQ_CORO(
      res.value()[0].sub, context_subject(default_context, subject{"s"}));
    ASSERT_EQ_CORO(res.value()[0].version, schema_version{5});
}

TEST_CORO(parse_schema_id_subject_versions_test, missing_field_is_error) {
    // Both subject and version must be present.
    for (std::string_view body :
         {R"([{"subject": "s"}])", R"([{"version": 1}])", R"([{}])"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_schema_id_subject_versions(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_schema_id_subject_versions_test, not_an_array_is_error) {
    for (std::string_view body :
         {R"({"subject": "s", "version": 1})", R"("s")", "42", "null"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_schema_id_subject_versions(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_schema_id_subject_versions_test, non_object_element_is_error) {
    for (std::string_view body :
         {R"(["s"])", R"([1])", R"([{"subject": "s", "version": 1}, 2])"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_schema_id_subject_versions(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(
  parse_schema_id_subject_versions_test, non_key_token_in_object_is_error) {
    // An object member that opens with a non-key token (a bare value) is
    // rejected by an explicit shape check rather than by letting value_string()
    // throw and relying on the exception firewall.
    auto res = co_await parse_schema_id_subject_versions(
      iobuf::from(R"([{true}])"), qualified_subjects_enabled::yes);
    ASSERT_FALSE_CORO(res.has_value());
}

TEST_CORO(
  parse_schema_id_subject_versions_test, unknown_key_missing_value_is_error) {
    // An unmodeled key with no value faults the skip path; the exception
    // firewall reports it as a parse_error rather than letting it escape.
    auto res = co_await parse_schema_id_subject_versions(
      iobuf::from(R"([{"x"}])"), qualified_subjects_enabled::yes);
    ASSERT_FALSE_CORO(res.has_value());
}

TEST_CORO(parse_schema_id_subject_versions_test, wrong_typed_field_is_error) {
    for (std::string_view body :
         {R"([{"subject": 5, "version": 1}])",
          R"([{"subject": "s", "version": "1"}])",
          R"([{"subject": "s", "version": 1.5}])"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_schema_id_subject_versions(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(
  parse_schema_id_subject_versions_test, out_of_range_version_is_error) {
    // version must be >= 1.
    for (std::string_view body :
         {R"([{"subject": "s", "version": 0}])",
          R"([{"subject": "s", "version": -1}])"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_schema_id_subject_versions(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_schema_id_subject_versions_test, trailing_content_is_error) {
    for (std::string_view body :
         {R"([{"subject": "s", "version": 1}] "x")",
          R"([{"subject": "s", "version": 1}]{})",
          R"([{"subject": "s", "version": 1}],)"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_schema_id_subject_versions(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_schema_id_subject_versions_test, trailing_whitespace_is_ok) {
    auto res = co_await parse_schema_id_subject_versions(
      iobuf::from("[{\"subject\": \"s\", \"version\": 1}]  \n\t "),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value().size(), size_t{1});
}

TEST_CORO(parse_schema_id_subject_versions_test, fragmented_input) {
    // One byte per fragment forces the objects, their keys, and the array
    // structure to span parser fragment boundaries.
    auto res = co_await parse_schema_id_subject_versions(
      fragmented_iobuf(
        R"([{"subject": ":.ctx:orders", "version": 2}, {"subject": "s", "version": 1}])",
        1),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& v = res.value();
    ASSERT_EQ_CORO(v.size(), size_t{2});
    ASSERT_EQ_CORO(
      v[0].sub, context_subject(context{".ctx"}, subject{"orders"}));
    ASSERT_EQ_CORO(v[0].version, schema_version{2});
    ASSERT_EQ_CORO(v[1].sub, context_subject(default_context, subject{"s"}));
    ASSERT_EQ_CORO(v[1].version, schema_version{1});
}

TEST_CORO(
  parse_schema_id_subject_versions_test, malformed_or_truncated_is_error) {
    for (std::string_view body :
         {"",
          "[",
          R"([{)",
          R"([{"subject")",
          R"([{"subject": "s", "version": 1)",
          R"([{"subject": "s", "version": 1})",
          "not json"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_schema_id_subject_versions(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
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
    ASSERT_TRUE_CORO(res.value().unsupported.empty());
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
    ASSERT_TRUE_CORO(res.value().unsupported.empty());
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

TEST_CORO(parse_subject_version_test, surfaces_unsupported_fields) {
    // ruleSet/schemaTags (and a future field) are not mapped into the schema;
    // they are surfaced in `unsupported` as JSON pointers so the caller can
    // apply its policy. A nested object/array under such a field is skipped
    // without descending into it. metadata is special: its modeled `properties`
    // is captured, while an unmodeled sub-key (`sensitive`) is reported as
    // `/metadata/sensitive`. The server-assigned `guid`/`ts` are ignorable and
    // dropped silently (not surfaced).
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
    // Unsupported fields are surfaced as JSON pointers, in encounter order.
    // guid/ts are ignorable (dropped); modeled nested keys
    // (metadata.properties, ruleSet.domainRules, ...) are not reported.
    const auto& unsupported = res.value().unsupported;
    ASSERT_EQ_CORO(unsupported.size(), size_t{4});
    ASSERT_EQ_CORO(unsupported[0].json_pointer, "/ruleSet");
    ASSERT_EQ_CORO(unsupported[0].json_type, "object");
    ASSERT_EQ_CORO(unsupported[1].json_pointer, "/metadata/sensitive");
    ASSERT_EQ_CORO(unsupported[1].json_type, "array");
    ASSERT_EQ_CORO(unsupported[2].json_pointer, "/schemaTags");
    ASSERT_EQ_CORO(unsupported[2].json_type, "array");
    ASSERT_EQ_CORO(unsupported[3].json_pointer, "/futureField");
    ASSERT_EQ_CORO(unsupported[3].json_type, "array");
}

TEST_CORO(parse_subject_version_test, ignorable_and_null_unmodeled_fields) {
    // Server-assigned guid/ts are ignorable, and a null-valued unmodeled field
    // is treated as absent; none are surfaced.
    auto res = co_await parse_subject_version(
      iobuf::from(
        R"({"subject":"r","version":1,"id":2,"schema":"x",)"
        R"("guid":"g","ts":1,"ruleSet":null})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_TRUE_CORO(res.value().unsupported.empty());
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
    ASSERT_TRUE_CORO(res.value().unsupported.empty());
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
    const auto& unsupported = res.value().unsupported;
    ASSERT_EQ_CORO(unsupported.size(), size_t{2});
    ASSERT_EQ_CORO(unsupported[0].json_pointer, "/metadata/tags");
    ASSERT_EQ_CORO(unsupported[0].json_type, "object");
    ASSERT_EQ_CORO(unsupported[1].json_pointer, "/metadata/sensitive");
    ASSERT_EQ_CORO(unsupported[1].json_type, "array");
}

TEST_CORO(parse_subject_version_test, metadata_empty_and_null) {
    // metadata: {} is present-but-empty; metadata: null is treated as absent.
    auto empty_obj = co_await parse_subject_version(
      iobuf::from(R"({"subject":"r","version":1,"id":2,"metadata":{}})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(empty_obj.has_value());
    ASSERT_TRUE_CORO(empty_obj.value().unsupported.empty());
    ASSERT_TRUE_CORO(empty_obj.value().schema.schema.def().meta().has_value());

    auto null_meta = co_await parse_subject_version(
      iobuf::from(R"({"subject":"r","version":1,"id":2,"metadata":null})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(null_meta.has_value());
    ASSERT_TRUE_CORO(null_meta.value().unsupported.empty());
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
          R"({"subject":42})",                          // subject wrong type
          R"({"schema":42,"subject":"r"})",             // schema wrong type
          R"({"schemaType":42,"subject":"r"})",         // schemaType wrong type
          R"({"references":[42]})",             // reference not an object
          R"({"references":[{"name":42}]})",    // reference name wrong type
          R"({"references":[{"subject":42}]})", // reference subject wrong type
          R"({"references":[{"version":0}]})", // reference version out of range
          R"({"references":[{true}]})",        // reference: non-key token
          R"({"references":[{)", // reference: missing key (truncated)
          R"({"metadata":{)",    // truncated metadata (parser throws, firewall
                                 // catch)
          "[1,2,3]",             // not an object
          R"({"subject":"r")",   // truncated
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

TEST_CORO(parse_subject_version_test, reference_unknown_keys_skipped) {
    // A reference may carry keys the client does not model; they are skipped
    // (never rejected, never surfaced in `unsupported`) and the modeled
    // name/subject/version are still recovered.
    auto res = co_await parse_subject_version(
      iobuf::from(
        R"({"subject":"r","version":1,"references":)"
        R"([{"extra":{"nested":true},"name":"n","subject":"s","version":2}]})"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_TRUE_CORO(res->unsupported.empty());
    const auto& refs = res->schema.schema.def().refs();
    ASSERT_EQ_CORO(refs.size(), size_t{1});
    ASSERT_EQ_CORO(refs[0].name, "n");
    ASSERT_EQ_CORO(refs[0].version, schema_version{2});
}

TEST_CORO(parse_error_body_test, non_key_token_is_nullopt) {
    // After '{', a token that is not an object key means a malformed body:
    // tolerantly nullopt, not an error.
    auto res = co_await parse_error_body(iobuf::from(R"({true})"));
    ASSERT_FALSE_CORO(res.has_value());
}

TEST_CORO(parse_error_body_test, error_code_out_of_int32_range_is_nullopt) {
    // error_code must fit int32; a value outside the range is not a usable
    // code, so it is dropped and the body (carrying no other code) yields
    // nullopt.
    auto res = co_await parse_error_body(
      iobuf::from(R"({"error_code": 9999999999})"));
    ASSERT_FALSE_CORO(res.has_value());
}

TEST_CORO(parse_error_body_test, non_string_message_ignored) {
    // A present-but-wrong-typed message is skipped like any unmodeled field;
    // the error_code is still recovered and message stays empty.
    auto res = co_await parse_error_body(
      iobuf::from(R"({"error_code": 40401, "message": 42})"));
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res->error_code, 40401);
    ASSERT_EQ_CORO(res->message, "");
}

TEST_CORO(parse_error_body_test, missing_value_is_nullopt) {
    // A key with no value makes the parser throw while skipping; the tolerant
    // firewall turns that into nullopt rather than propagating.
    for (std::string_view body : {R"({"error_code"})", R"({"unknown"})"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_error_body(iobuf::from(body));
        ASSERT_FALSE_CORO(res.has_value());
    }
}

} // namespace pandaproxy::schema_registry::rest_client
