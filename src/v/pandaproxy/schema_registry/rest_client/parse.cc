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

#include <cstdint>
#include <limits>
#include <optional>
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

// The result of parsing a metadata object: the modeled portion plus the names
// of any sub-keys we don't model.
struct parsed_metadata {
    schema_metadata metadata;
    // Unmodeled keys found directly inside `metadata` (e.g. `tags`,
    // `sensitive`), unqualified; the caller qualifies and propagates them.
    chunked_vector<ss::sstring> unknown_fields;
};

// Parse a metadata object of the form {"properties": {<str>: <str>}, ...}.
// Entered with the current token at the object start; leaves the parser at the
// end_object token. Only `properties` is modeled; its values are stored as
// strings, with numbers and booleans coerced to strings to match the write
// path. Any other key (e.g. `tags`, `sensitive`) is returned, unqualified, in
// parsed_metadata::unknown_fields for the caller to record.
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
            result.unknown_fields.push_back(std::move(key));
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

ss::future<std::expected<parsed_schema, parse_error>>
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
        is_deleted deleted{false};
        std::optional<schema_metadata> metadata;
        chunked_vector<ss::sstring> unknown_fields;

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
                // unknown_fields above for the caller to act on.
                co_return parsed_schema{
                  .schema = stored_schema{
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
                    .deleted = deleted},
                  .unknown_fields = std::move(unknown_fields)};
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
                // returns any other sub-key (e.g. `tags`), which we qualify
                // with a `metadata.` prefix into unknown_fields. A null
                // metadata is treated as absent; any other non-object is
                // unrepresentable.
                if (p.token() == token::start_object) {
                    auto m = co_await parse_metadata(p);
                    if (!m) {
                        co_return std::unexpected(std::move(m.error()));
                    }
                    metadata = std::move(m->metadata);
                    for (const auto& sub : m->unknown_fields) {
                        unknown_fields.push_back(
                          ssx::sformat("metadata.{}", sub));
                    }
                } else if (p.token() != token::value_null) {
                    co_return std::unexpected(
                      parse_error{.reason = "metadata must be an object"});
                }
            } else {
                // Unknown / not-yet-modeled field (guid, ts, ruleSet,
                // schemaTags, ...): skip its value, but record the top-level
                // key so the caller can decide whether dropping it is
                // acceptable.
                unknown_fields.push_back(std::move(key));
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
