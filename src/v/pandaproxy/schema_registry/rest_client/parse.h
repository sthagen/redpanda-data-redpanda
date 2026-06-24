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
#pragma once

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <cstdint>
#include <expected>
#include <optional>

namespace pandaproxy::schema_registry::rest_client {

/// Describes why a schema registry response body could not be parsed into the
/// expected native type.
struct parse_error {
    ss::sstring reason;
};

/// Parse the body of a `GET /subjects` response into a list of subjects.
///
/// The response is a JSON array of subject-name strings (see the Schema
/// Registry REST API); each element is decoded with
/// context_subject::from_string. \p qualified selects whether a
/// ":.context:subject" element is interpreted as context-qualified; the caller
/// supplies this policy so that parsing a remote registry's response does not
/// depend on this node's cluster config.
///
/// The body must be exactly a JSON array of strings: a non-array, a non-string
/// element, or any trailing content after the array yields a parse_error rather
/// than a partial or lenient result. (Tolerance toward unmodeled fields is
/// reserved for richer Schema Registry responses that carry optional fields;
/// the subjects listing has a single fixed shape.) The function does not throw:
/// malformed input is reported via the returned std::expected.
ss::future<std::expected<chunked_vector<context_subject>, parse_error>>
parse_subjects(iobuf body, qualified_subjects_enabled qualified);

/// Parse the body of a `GET /subjects/{subject}/versions` response into a list
/// of versions.
///
/// The body must be exactly a JSON array of integers, each a version number in
/// [1, INT32_MAX]; a non-array, a non-integer or out-of-range element, or any
/// trailing content after the array yields a parse_error (same strict, fixed
/// shape as parse_subjects).
///
/// Negative values are rejected. The Schema Registry `deletedAsNegative` mode
/// encodes soft-deleted versions as negative numbers, but this client does not
/// request that mode; modeling per-version deletion state is future work to add
/// only if a client feature needs it. The function does not throw: malformed
/// input is reported via the returned std::expected.
ss::future<std::expected<chunked_vector<schema_version>, parse_error>>
parse_subject_versions(iobuf body);

/// The outcome of parsing a get-schema-by-version response: the schema, plus
/// the names of any top-level response fields the parser did not model.
///
/// parse_subject_version is deliberately lenient — it never rejects a response
/// merely for carrying fields it doesn't model; it skips them and records their
/// names here. This lets a caller that needs fidelity (e.g. schema migration)
/// apply its own policy — reject, warn, or ignore — while a caller that doesn't
/// care simply disregards the list. Recorded names are top-level keys, with one
/// exception: `metadata` is only partially modeled (just `metadata.properties`
/// is captured), so an unmodeled key directly under it is reported with a
/// `metadata.` prefix (e.g. `metadata.tags`). An unmodeled key nested inside
/// any other modeled field (e.g. within a reference) is skipped without being
/// reported. It is therefore a best-effort signal that content was dropped, not
/// a proof of a lossless round-trip.
struct parsed_schema {
    stored_schema schema;
    chunked_vector<ss::sstring> unknown_fields;
};

/// Parse the body of a `GET /subjects/{subject}/versions/{version}` response
/// into a parsed_schema (the schema plus the names of any unmodeled top-level
/// fields).
///
/// This is a faithful, lenient deserialization (the lowest layer): unknown or
/// not-yet-modeled fields (`guid`, `ts`, `ruleSet`, `schemaTags`, ...) are
/// skipped — their names are collected in parsed_schema::unknown_fields for the
/// caller to act on. `metadata` is partially modeled: `metadata.properties` is
/// captured into the schema's metadata (values are stringified, matching the
/// write path), while any other key under `metadata` (e.g. `metadata.tags`) is
/// reported in unknown_fields under a `metadata.` prefix. Absent fields take
/// their default/sentinel (absent `schemaType` -> AVRO, `deleted` -> false,
/// `references` -> empty, `metadata` -> absent, and absent
/// `subject`/`version`/`id`/`schema` -> the invalid sentinels). It does NOT
/// enforce completeness or reject for unmodeled fields — whether an incomplete
/// or lossy response is acceptable (a strict mode) is a higher-layer concern.
/// It rejects only inputs it cannot represent: a non-object body, malformed
/// JSON, a present modeled field with a wrong-typed or out-of-range value, or
/// an unknown `schemaType`.
///
/// \p qualified is the caller-supplied policy for interpreting
/// context-qualified subject strings (the response `subject` and each
/// reference's `subject`). The function does not throw.
ss::future<std::expected<parsed_schema, parse_error>>
parse_subject_version(iobuf body, qualified_subjects_enabled qualified);

/// The structured error body Schema Registry returns on failures:
/// `{"error_code": <int>, "message": "<text>"}`. `error_code` is finer-grained
/// than the HTTP status (e.g. a 404 may carry 40401 subject-not-found vs 40402
/// version-not-found).
struct error_body {
    int32_t error_code{0};
    ss::sstring message;
};

/// Tolerantly parse a Schema Registry error response body. Returns nullopt when
/// the body is empty, not a JSON object, not valid JSON, or carries no integer
/// `error_code` — an auth proxy in front of the registry may return an HTML or
/// empty body, in which case the caller falls back to the HTTP status. Unknown
/// fields are ignored. The function does not throw.
ss::future<std::optional<error_body>> parse_error_body(iobuf body);

} // namespace pandaproxy::schema_registry::rest_client
