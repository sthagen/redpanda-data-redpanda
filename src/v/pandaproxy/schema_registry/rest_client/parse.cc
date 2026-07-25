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
#include "pandaproxy/schema_registry/rest_client/parse.h"

#include "serde/json/parser.h"
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>

namespace pandaproxy::schema_registry::rest_client {

namespace {

// A present version must be a positive value representable as int32; this also
// keeps a present value from aliasing invalid_schema_version (-1).
std::optional<int32_t> checked_positive_i32(int64_t v) {
    if (v < 1 || v > std::numeric_limits<int32_t>::max()) {
        return std::nullopt;
    }
    return static_cast<int32_t>(v);
}

// A present id may be 0 (upstream permits id 0 on import), so it must be a
// non-negative value representable as int32; this keeps a present value from
// aliasing invalid_schema_id (-1).
std::optional<int32_t> checked_nonnegative_i32(int64_t v) {
    if (v < 0 || v > std::numeric_limits<int32_t>::max()) {
        return std::nullopt;
    }
    return static_cast<int32_t>(v);
}

// Server-assigned response fields Redpanda does not model but which carry no
// user content. The client opts into them via the
// `Confluent-Accept-Unknown-Properties` header (see client.cc), so they arrive
// on schema responses; this list drops them rather than surfacing them to the
// unsupported-feature policy, so a source that returns them does not spuriously
// trip it.
//
// A constexpr array scanned with ranges::contains is a deliberate choice at
// this size: it keeps the list trivially extensible (just add a literal) rather
// than a switch/case, and for N=2 a linear scan beats a hash set (no static
// init, stays constexpr). Promote to a flat_hash_set only if this list grows
// large or gains a bulk-lookup site (cf. cluster_link's
// disallowed_topic_properties, materialized into a set in its validator).
constexpr auto ignorable_fields = std::to_array<std::string_view>(
  {"guid", "ts"});

bool is_ignorable_field(std::string_view key) {
    return std::ranges::contains(ignorable_fields, key);
}

// The field's JSON type name, for unsupported-feature diagnostics; takes the
// parser's current value token.
const char* json_type_name(serde::json::token t) {
    using token = serde::json::token;
    switch (t) {
    case token::start_object:
        return "object";
    case token::start_array:
        return "array";
    case token::value_string:
        return "string";
    case token::value_int:
    case token::value_double:
        return "number";
    case token::value_true:
    case token::value_false:
        return "boolean";
    case token::value_null:
        return "null";
    // Non-value tokens never reach here (this is called only on the parser's
    // current value token). They are enumerated rather than folded into a
    // default so the switch stays exhaustive: -Wswitch (via -Werror) then flags
    // a newly-added token at compile time instead of silently returning
    // "unknown".
    case token::error:
    case token::key:
    case token::end_object:
    case token::end_array:
    case token::eof:
        return "unknown";
    }
}

} // namespace

ss::future<std::expected<chunked_vector<context_subject>, parse_error>>
parse_subjects(iobuf body, qualified_subjects_enabled qualified) {
    using token = serde::json::token;
    // Firewall exceptions from the parser: malformed input is reported via the
    // returned std::expected, not thrown.
    try {
        serde::json::parser p(std::move(body));

        if (!co_await p.next() || p.token() != token::start_array) {
            co_return std::unexpected(
              parse_error{.reason = "expected a JSON array of subjects"});
        }

        chunked_vector<context_subject> subjects;
        while (co_await p.next()) {
            switch (p.token()) {
            case token::end_array:
                // The body is exactly a JSON array of strings: reject any
                // trailing content rather than ignoring it.
                co_await p.next();
                if (p.token() != token::eof) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "trailing content after subjects array"});
                }
                co_return std::move(subjects);
            case token::value_string:
                subjects.push_back(
                  context_subject::from_string(
                    p.value_string().linearize_to_string(), qualified));
                break;
            default:
                co_return std::unexpected(
                  parse_error{
                    .reason = "expected a string element in subjects array"});
            }
        }

        // next() returned false before the closing ']' was seen.
        co_return std::unexpected(
          parse_error{.reason = "truncated or malformed JSON"});
    } catch (const std::exception& e) {
        co_return std::unexpected(
          parse_error{
            .reason = ssx::sformat("failed to parse subjects: {}", e.what())});
    }
}

ss::future<std::expected<chunked_vector<context>, parse_error>>
parse_contexts(iobuf body) {
    using token = serde::json::token;
    // Firewall exceptions from the parser: malformed input is reported via the
    // returned std::expected, not thrown.
    try {
        serde::json::parser p(std::move(body));

        if (!co_await p.next() || p.token() != token::start_array) {
            co_return std::unexpected(
              parse_error{.reason = "expected a JSON array of contexts"});
        }

        chunked_vector<context> contexts;
        while (co_await p.next()) {
            switch (p.token()) {
            case token::end_array:
                // The body is exactly a JSON array of strings: reject any
                // trailing content rather than ignoring it.
                co_await p.next();
                if (p.token() != token::eof) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "trailing content after contexts array"});
                }
                co_return std::move(contexts);
            case token::value_string:
                // Each element is a bare, dot-prefixed context name (".",
                // ".dev") and is wrapped verbatim. Unlike a subject, a context
                // has no ":.ctx:" qualified form to decode here.
                contexts.push_back(
                  context{p.value_string().linearize_to_string()});
                break;
            default:
                co_return std::unexpected(
                  parse_error{
                    .reason = "expected a string element in contexts array"});
            }
        }

        // next() returned false before the closing ']' was seen.
        co_return std::unexpected(
          parse_error{.reason = "truncated or malformed JSON"});
    } catch (const std::exception& e) {
        co_return std::unexpected(
          parse_error{
            .reason = ssx::sformat("failed to parse contexts: {}", e.what())});
    }
}

ss::future<std::expected<mode_info, parse_error>> parse_mode(iobuf body) {
    using token = serde::json::token;
    // Firewall exceptions from the parser: malformed input is reported via the
    // returned std::expected, not thrown.
    try {
        serde::json::parser p(std::move(body));

        if (!co_await p.next() || p.token() != token::start_object) {
            co_return std::unexpected(
              parse_error{.reason = "expected a JSON object"});
        }

        std::optional<mode_info> result;
        while (co_await p.next()) {
            if (p.token() == token::end_object) {
                // The body is exactly one JSON object: reject any trailing
                // content rather than ignoring it.
                co_await p.next();
                if (p.token() != token::eof) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "trailing content after mode object"});
                }
                if (!result.has_value()) {
                    // `mode` is the one field a successful response must carry.
                    co_return std::unexpected(
                      parse_error{.reason = "missing mode field"});
                }
                co_return std::move(*result);
            }
            if (p.token() != token::key) {
                co_return std::unexpected(
                  parse_error{.reason = "expected an object key"});
            }
            auto key = p.value_string().linearize_to_string();
            if (!co_await p.next()) {
                co_return std::unexpected(
                  parse_error{.reason = "truncated JSON after key"});
            }
            if (key == "mode") {
                if (p.token() != token::value_string) {
                    co_return std::unexpected(
                      parse_error{.reason = "mode must be a string"});
                }
                // Shape is strict but the value is open: map the recognized
                // wire strings and keep the verbatim value, so an unrecognized
                // (open-enum) mode is preserved rather than rejected.
                auto raw = p.value_string().linearize_to_string();
                result = mode_info{
                  .mode = registry_mode_from_wire(raw), .raw = std::move(raw)};
            } else {
                // Ignore any other field: the server omits null/empty fields
                // and a client must not assume any field beyond `mode`.
                co_await p.skip_value();
            }
        }

        // next() returned false before the closing '}'.
        co_return std::unexpected(
          parse_error{.reason = "truncated or malformed JSON"});
    } catch (const std::exception& e) {
        co_return std::unexpected(
          parse_error{
            .reason = ssx::sformat("failed to parse mode: {}", e.what())});
    }
}

ss::future<std::expected<config_info, parse_error>> parse_config(iobuf body) {
    using token = serde::json::token;
    // Firewall exceptions from the parser: malformed input is reported via the
    // returned std::expected, not thrown.
    try {
        serde::json::parser p(std::move(body));

        if (!co_await p.next() || p.token() != token::start_object) {
            co_return std::unexpected(
              parse_error{.reason = "expected a JSON object"});
        }

        std::optional<registry_compatibility_level> level;
        ss::sstring raw;
        chunked_vector<unsupported_feature> unsupported;
        while (co_await p.next()) {
            if (p.token() == token::end_object) {
                // The body is exactly one JSON object: reject any trailing
                // content rather than ignoring it.
                co_await p.next();
                if (p.token() != token::eof) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "trailing content after config object"});
                }
                // An absent compatibilityLevel is not an error: a subject
                // config may carry only governance fields, which must still
                // reach the unsupported-feature policy.
                co_return config_info{
                  .level = level,
                  .raw = std::move(raw),
                  .unsupported = std::move(unsupported)};
            }
            if (p.token() != token::key) {
                co_return std::unexpected(
                  parse_error{.reason = "expected an object key"});
            }
            auto key = p.value_string().linearize_to_string();
            if (!co_await p.next()) {
                co_return std::unexpected(
                  parse_error{.reason = "truncated JSON after key"});
            }
            if (key == "compatibilityLevel") {
                if (p.token() == token::value_null) {
                    // Null means absent, as for every other field; duplicate
                    // keys are last-wins, so clear any earlier value.
                    level.reset();
                    raw = {};
                    co_await p.skip_value();
                    continue;
                }
                if (p.token() != token::value_string) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "compatibilityLevel must be a string"});
                }
                // Shape is strict but the value is open: map the recognized
                // wire strings and keep the verbatim value, so an unrecognized
                // (open-enum) level is preserved rather than rejected.
                raw = p.value_string().linearize_to_string();
                level = registry_compatibility_level_from_wire(raw);
            } else {
                // Any other non-null top-level field is unmodeled: recorded as
                // a JSON pointer + type for the policy; null means absent.
                if (p.token() != token::value_null) {
                    unsupported.push_back(
                      unsupported_feature{
                        .json_pointer = ssx::sformat("/{}", key),
                        .json_type = json_type_name(p.token())});
                }
                co_await p.skip_value();
            }
        }

        // next() returned false before the closing '}'.
        co_return std::unexpected(
          parse_error{.reason = "truncated or malformed JSON"});
    } catch (const std::exception& e) {
        co_return std::unexpected(
          parse_error{
            .reason = ssx::sformat("failed to parse config: {}", e.what())});
    }
}

ss::future<std::expected<chunked_vector<schema_version>, parse_error>>
parse_subject_versions(iobuf body) {
    using token = serde::json::token;
    // Firewall exceptions from the parser: malformed input is reported via the
    // returned std::expected, not thrown.
    try {
        serde::json::parser p(std::move(body));

        if (!co_await p.next() || p.token() != token::start_array) {
            co_return std::unexpected(
              parse_error{.reason = "expected a JSON array of versions"});
        }

        chunked_vector<schema_version> versions;
        while (co_await p.next()) {
            switch (p.token()) {
            case token::end_array:
                // The body is exactly a JSON array of integers: reject any
                // trailing content rather than ignoring it.
                co_await p.next();
                if (p.token() != token::eof) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "trailing content after versions array"});
                }
                co_return std::move(versions);
            case token::value_int: {
                auto raw = p.value_int();
                if (raw < 0) {
                    // Only the deletedAsNegative mode produces negatives, which
                    // this client does not request; modeling soft-deleted
                    // versions is future work.
                    co_return std::unexpected(
                      parse_error{
                        .reason = "negative version number; deletedAsNegative "
                                  "mode is not supported"});
                }
                auto v = checked_positive_i32(raw);
                if (!v) {
                    co_return std::unexpected(
                      parse_error{.reason = "version number out of range"});
                }
                versions.push_back(schema_version{*v});
                break;
            }
            default:
                co_return std::unexpected(
                  parse_error{
                    .reason = "expected an integer element in versions array"});
            }
        }

        // next() returned false before the closing ']' was seen.
        co_return std::unexpected(
          parse_error{.reason = "truncated or malformed JSON"});
    } catch (const std::exception& e) {
        co_return std::unexpected(
          parse_error{
            .reason = ssx::sformat("failed to parse versions: {}", e.what())});
    }
}

ss::future<std::expected<chunked_vector<subject_version>, parse_error>>
parse_schema_id_subject_versions(
  iobuf body, qualified_subjects_enabled qualified) {
    using token = serde::json::token;
    // Firewall exceptions from the parser: malformed input is reported via the
    // returned std::expected, not thrown.
    try {
        serde::json::parser p(std::move(body));

        if (!co_await p.next() || p.token() != token::start_array) {
            co_return std::unexpected(
              parse_error{
                .reason = "expected a JSON array of subject-version objects"});
        }

        chunked_vector<subject_version> result;
        while (co_await p.next()) {
            if (p.token() == token::end_array) {
                // The body is exactly a JSON array: reject any trailing content
                // rather than ignoring it.
                co_await p.next();
                if (p.token() != token::eof) {
                    co_return std::unexpected(
                      parse_error{
                        .reason
                        = "trailing content after subject-versions array"});
                }
                co_return std::move(result);
            }
            if (p.token() != token::start_object) {
                co_return std::unexpected(
                  parse_error{.reason = "expected a subject-version object"});
            }
            // Each element is a {subject, version} object. Unknown keys are
            // tolerated and skipped, but both modeled fields must be present.
            std::optional<context_subject> sub;
            std::optional<schema_version> version;
            while (co_await p.next() && p.token() != token::end_object) {
                // Shape is strict: reject a non-key token explicitly rather
                // than letting value_string() throw and rely on the catch
                // below.
                if (p.token() != token::key) {
                    co_return std::unexpected(
                      parse_error{.reason = "expected an object key"});
                }
                auto key = p.value_string().linearize_to_string();
                if (!co_await p.next()) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "truncated JSON in subject-version object"});
                }
                if (key == "subject") {
                    if (p.token() != token::value_string) {
                        co_return std::unexpected(
                          parse_error{.reason = "subject must be a string"});
                    }
                    sub = context_subject::from_string(
                      p.value_string().linearize_to_string(), qualified);
                } else if (key == "version") {
                    if (p.token() != token::value_int) {
                        co_return std::unexpected(
                          parse_error{.reason = "version must be an integer"});
                    }
                    auto v = checked_positive_i32(p.value_int());
                    if (!v) {
                        co_return std::unexpected(
                          parse_error{.reason = "version number out of range"});
                    }
                    version = schema_version{*v};
                } else {
                    co_await p.skip_value();
                }
            }
            if (!sub.has_value() || !version.has_value()) {
                co_return std::unexpected(
                  parse_error{
                    .reason
                    = "subject-version object missing subject or version"});
            }
            result.emplace_back(std::move(*sub), *version);
        }

        // next() returned false before the closing ']' was seen.
        co_return std::unexpected(
          parse_error{.reason = "truncated or malformed JSON"});
    } catch (const std::exception& e) {
        co_return std::unexpected(
          parse_error{
            .reason = ssx::sformat(
              "failed to parse schema-id subject-versions: {}", e.what())});
    }
}

namespace {

// Parse a JSON array of {name, subject, version} reference objects. Entered
// with the current token at the array start; leaves the parser at the end_array
// token. Lenient: unknown keys within a reference are skipped, absent fields
// take defaults; only wrong-typed values are rejected.
ss::future<std::expected<schema_definition::references, parse_error>>
parse_references(serde::json::parser& p, qualified_subjects_enabled qualified) {
    using token = serde::json::token;
    schema_definition::references refs;
    while (co_await p.next()) {
        if (p.token() == token::end_array) {
            co_return refs;
        }
        if (p.token() != token::start_object) {
            co_return std::unexpected(
              parse_error{.reason = "schema reference must be an object"});
        }
        ss::sstring name;
        std::optional<context_subject_reference> sub;
        std::optional<schema_version> version;
        while (co_await p.next() && p.token() != token::end_object) {
            // Shape is strict: reject a non-key token explicitly rather than
            // letting value_string() throw and rely on the catch in the caller.
            if (p.token() != token::key) {
                co_return std::unexpected(
                  parse_error{.reason = "expected an object key"});
            }
            auto key = p.value_string().linearize_to_string();
            if (!co_await p.next()) {
                co_return std::unexpected(
                  parse_error{.reason = "truncated JSON in schema reference"});
            }
            if (key == "name") {
                if (p.token() != token::value_string) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "schema reference name must be a string"});
                }
                name = p.value_string().linearize_to_string();
            } else if (key == "subject") {
                if (p.token() != token::value_string) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "schema reference subject must be a string"});
                }
                sub = context_subject_reference::from_string(
                  p.value_string().linearize_to_string(), qualified);
            } else if (key == "version") {
                if (p.token() != token::value_int) {
                    co_return std::unexpected(
                      parse_error{
                        .reason
                        = "schema reference version must be an integer"});
                }
                auto v = checked_positive_i32(p.value_int());
                if (!v) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "schema reference version out of range"});
                }
                version = schema_version{*v};
            } else {
                co_await p.skip_value();
            }
        }
        refs.push_back(
          schema_reference{
            .name = std::move(name),
            .sub = sub.value_or(
              context_subject_reference{invalid_subject, is_qualified::no}),
            .version = version.value_or(invalid_schema_version)});
    }
    co_return std::unexpected(
      parse_error{.reason = "truncated or malformed references array"});
}

// The result of parsing a metadata object: the modeled portion plus any
// unsupported sub-fields, each already reported as a `/metadata/<key>` pointer.
struct parsed_metadata {
    schema_metadata metadata;
    chunked_vector<unsupported_feature> unsupported;
};

// Parse a metadata object of the form {"properties": {<str>: <str>}, ...}.
// Entered with the current token at the object start; leaves the parser at the
// end_object token. Only `properties` is modeled; its values are stored as
// strings, with numbers and booleans coerced to strings to match the write
// path. Any other non-null key (e.g. `tags`, `sensitive`) is reported in
// parsed_metadata::unsupported as a `/metadata/<key>` pointer.
ss::future<std::expected<parsed_metadata, parse_error>>
parse_metadata(serde::json::parser& p) {
    using token = serde::json::token;
    parsed_metadata result;
    while (co_await p.next()) {
        if (p.token() == token::end_object) {
            co_return result;
        }
        auto key = p.value_string().linearize_to_string();
        if (!co_await p.next()) {
            co_return std::unexpected(
              parse_error{.reason = "truncated JSON in schema metadata"});
        }
        if (key != "properties") {
            // A null value means the sub-field is absent; anything else is an
            // unsupported feature, surfaced as a `/metadata/<key>` pointer.
            if (p.token() != token::value_null) {
                result.unsupported.push_back(
                  unsupported_feature{
                    .json_pointer = ssx::sformat("/metadata/{}", key),
                    .json_type = json_type_name(p.token())});
            }
            co_await p.skip_value();
            continue;
        }
        if (p.token() == token::value_null) {
            continue;
        }
        if (p.token() != token::start_object) {
            co_return std::unexpected(
              parse_error{
                .reason = "schema metadata properties must be an object"});
        }
        auto& props = result.metadata.properties.emplace();
        while (co_await p.next()) {
            if (p.token() == token::end_object) {
                break;
            }
            auto prop_key = p.value_string().linearize_to_string();
            if (!co_await p.next()) {
                co_return std::unexpected(
                  parse_error{
                    .reason = "truncated JSON in schema metadata properties"});
            }
            switch (p.token()) {
            case token::value_string:
                props.insert_or_assign(
                  std::move(prop_key), p.value_string().linearize_to_string());
                break;
            case token::value_int:
                props.insert_or_assign(
                  std::move(prop_key), ssx::sformat("{}", p.value_int()));
                break;
            case token::value_double:
                props.insert_or_assign(
                  std::move(prop_key), ssx::sformat("{}", p.value_double()));
                break;
            case token::value_true:
                props.insert_or_assign(std::move(prop_key), "true");
                break;
            case token::value_false:
                props.insert_or_assign(std::move(prop_key), "false");
                break;
            default:
                co_return std::unexpected(
                  parse_error{
                    .reason = "schema metadata property value must be a "
                              "string, number, or boolean"});
            }
        }
    }
    co_return std::unexpected(
      parse_error{.reason = "truncated or malformed schema metadata object"});
}

} // namespace

ss::future<std::expected<source_schema_read, parse_error>>
parse_subject_version(iobuf body, qualified_subjects_enabled qualified) {
    using token = serde::json::token;
    // Firewall exceptions from the parser: malformed input is reported via the
    // returned std::expected, not thrown.
    try {
        serde::json::parser p(std::move(body));

        if (!co_await p.next() || p.token() != token::start_object) {
            co_return std::unexpected(
              parse_error{.reason = "expected a JSON object"});
        }

        std::optional<context_subject> subject;
        std::optional<schema_version> version;
        std::optional<schema_id> id;
        std::optional<iobuf> schema;
        schema_type type{schema_type::avro};
        schema_definition::references refs;
        std::optional<is_deleted> deleted;
        std::optional<schema_metadata> metadata;
        chunked_vector<unsupported_feature> unsupported;

        while (co_await p.next()) {
            if (p.token() == token::end_object) {
                // The body is exactly one JSON object: reject any trailing
                // content rather than ignoring it.
                co_await p.next();
                if (p.token() != token::eof) {
                    co_return std::unexpected(
                      parse_error{
                        .reason = "trailing content after schema object"});
                }
                // Absent fields fall back to defaults/sentinels; completeness
                // is a higher-layer concern. Unmodeled fields were recorded in
                // `unsupported` above for the caller to act on. `deleted` stays
                // nullopt when the body omitted the flag, so a caller can tell
                // an absent flag from an explicit false; into_stored() resolves
                // it to a concrete value with the caller's fallback.
                co_return source_schema_read{
                  .schema = subject_schema{
                    subject.value_or(invalid_subject),
                    schema_definition{
                      schema_definition::raw_string{
                        std::move(schema).value_or(iobuf{})},
                      type,
                      std::move(refs),
                      std::move(metadata)}},
                  .version = version.value_or(invalid_schema_version),
                  .id = id.value_or(invalid_schema_id),
                  .deleted = deleted,
                  .unsupported = std::move(unsupported)};
            }
            if (p.token() != token::key) {
                co_return std::unexpected(
                  parse_error{.reason = "expected an object key"});
            }
            auto key = p.value_string().linearize_to_string();
            if (!co_await p.next()) {
                co_return std::unexpected(
                  parse_error{.reason = "truncated JSON after key"});
            }
            if (key == "subject") {
                if (p.token() != token::value_string) {
                    co_return std::unexpected(
                      parse_error{.reason = "subject must be a string"});
                }
                subject = context_subject::from_string(
                  p.value_string().linearize_to_string(), qualified);
            } else if (key == "version") {
                if (p.token() != token::value_int) {
                    co_return std::unexpected(
                      parse_error{.reason = "version must be an integer"});
                }
                auto v = checked_positive_i32(p.value_int());
                if (!v) {
                    co_return std::unexpected(
                      parse_error{.reason = "version out of range"});
                }
                version = schema_version{*v};
            } else if (key == "id") {
                if (p.token() != token::value_int) {
                    co_return std::unexpected(
                      parse_error{.reason = "id must be an integer"});
                }
                auto v = checked_nonnegative_i32(p.value_int());
                if (!v) {
                    co_return std::unexpected(
                      parse_error{.reason = "id out of range"});
                }
                id = schema_id{*v};
            } else if (key == "schema") {
                if (p.token() != token::value_string) {
                    co_return std::unexpected(
                      parse_error{.reason = "schema must be a string"});
                }
                schema = p.value_string();
            } else if (key == "schemaType") {
                if (p.token() != token::value_string) {
                    co_return std::unexpected(
                      parse_error{.reason = "schemaType must be a string"});
                }
                auto st = from_string_view<schema_type>(
                  p.value_string().linearize_to_string());
                if (!st) {
                    co_return std::unexpected(
                      parse_error{.reason = "unknown schemaType"});
                }
                type = *st;
            } else if (key == "deleted") {
                if (p.token() == token::value_true) {
                    deleted = is_deleted::yes;
                } else if (p.token() == token::value_false) {
                    deleted = is_deleted::no;
                } else {
                    co_return std::unexpected(
                      parse_error{.reason = "deleted must be a boolean"});
                }
            } else if (key == "references") {
                if (p.token() != token::start_array) {
                    co_return std::unexpected(
                      parse_error{.reason = "references must be an array"});
                }
                auto r = co_await parse_references(p, qualified);
                if (!r) {
                    co_return std::unexpected(std::move(r.error()));
                }
                refs = std::move(*r);
            } else if (key == "metadata") {
                // Partially modeled: parse_metadata captures `properties` and
                // returns any other sub-key (e.g. `tags`) as a
                // `/metadata/<key>` unsupported feature. A null metadata is
                // treated as absent; any other non-object is unrepresentable.
                if (p.token() == token::start_object) {
                    auto m = co_await parse_metadata(p);
                    if (!m) {
                        co_return std::unexpected(std::move(m.error()));
                    }
                    metadata = std::move(m->metadata);
                    for (auto& f : m->unsupported) {
                        unsupported.push_back(std::move(f));
                    }
                } else if (p.token() != token::value_null) {
                    co_return std::unexpected(
                      parse_error{.reason = "metadata must be an object"});
                }
            } else {
                // Unmodeled field. Server-assigned fields that carry no user
                // content (`guid`, `ts`) are dropped silently; a null value
                // means the field is absent; anything else is an unsupported
                // feature, surfaced as a `/<key>` pointer for the migration
                // policy to act on.
                if (
                  !is_ignorable_field(key) && p.token() != token::value_null) {
                    unsupported.push_back(
                      unsupported_feature{
                        .json_pointer = ssx::sformat("/{}", key),
                        .json_type = json_type_name(p.token())});
                }
                co_await p.skip_value();
            }
        }

        // next() returned false before the closing '}'.
        co_return std::unexpected(
          parse_error{.reason = "truncated or malformed JSON"});
    } catch (const std::exception& e) {
        co_return std::unexpected(
          parse_error{
            .reason = ssx::sformat("failed to parse schema: {}", e.what())});
    }
}

ss::future<std::optional<error_body>> parse_error_body(iobuf body) {
    using token = serde::json::token;
    // Tolerant: any structural problem yields nullopt rather than an error;
    // the caller then falls back to the HTTP status.
    try {
        serde::json::parser p(std::move(body));

        if (!co_await p.next() || p.token() != token::start_object) {
            co_return std::nullopt;
        }

        std::optional<int32_t> code;
        ss::sstring message;
        while (co_await p.next()) {
            if (p.token() == token::end_object) {
                // Require the object to be the entire body: a complete object
                // followed by trailing content, or one carrying no integer
                // error_code, yields nullopt (the caller falls back to the
                // HTTP status).
                co_await p.next();
                if (p.token() != token::eof || !code.has_value()) {
                    co_return std::nullopt;
                }
                co_return error_body{
                  .error_code = *code, .message = std::move(message)};
            }
            if (p.token() != token::key) {
                co_return std::nullopt;
            }
            auto key = p.value_string().linearize_to_string();
            if (!co_await p.next()) {
                co_return std::nullopt;
            }
            if (key == "error_code" && p.token() == token::value_int) {
                auto v = p.value_int();
                if (
                  v >= std::numeric_limits<int32_t>::min()
                  && v <= std::numeric_limits<int32_t>::max()) {
                    code = static_cast<int32_t>(v);
                }
            } else if (key == "message" && p.token() == token::value_string) {
                message = p.value_string().linearize_to_string();
            } else {
                co_await p.skip_value();
            }
        }

        // next() returned false before the closing '}'.
        co_return std::nullopt;
    } catch (const std::exception&) {
        co_return std::nullopt;
    }
}

} // namespace pandaproxy::schema_registry::rest_client
