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
#include "pandaproxy/schema_registry/rest_client/config.h"
#include "pandaproxy/schema_registry/rest_client/mode.h"
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

/// Parse the body of a `GET /contexts` response into a list of contexts.
///
/// The response is a JSON array of context-name strings (see the Schema
/// Registry REST API). Each element is a bare, dot-prefixed context name: the
/// default context is exactly ".", and a named context is "." + name (e.g.
/// ".dev"). These are NOT the ":.name:" colon-qualified forms used by
/// context-qualified subjects, so — unlike parse_subjects — there is no
/// qualified/unqualified policy: each string is wrapped verbatim into a
/// `context`.
///
/// The body must be exactly a JSON array of strings: a non-array, a non-string
/// element, or any trailing content after the array yields a parse_error (same
/// strict, fixed shape as parse_subjects). The function does not throw:
/// malformed input is reported via the returned std::expected.
ss::future<std::expected<chunked_vector<context>, parse_error>>
parse_contexts(iobuf body);

/// Parse the body of a `GET /mode` response into a mode_info.
///
/// The body is a JSON object with a single modeled field, `mode`, a string
/// (e.g. `{"mode": "READWRITE"}`). Parsing splits shape from value: the shape
/// is strict — a non-object body, a missing `mode`, a non-string `mode`, or any
/// trailing content after the object yields a parse_error — whereas the `mode`
/// value is an open enum, so an unrecognized (or empty) string is not rejected
/// but mapped to registry_mode::unknown with the original preserved in
/// mode_info::raw (see mode.h). Any other top-level field is ignored: the
/// server omits null/empty fields, and a client must not assume any field
/// beyond `mode`. The function does not throw: malformed input is reported via
/// the returned std::expected.
ss::future<std::expected<mode_info, parse_error>> parse_mode(iobuf body);

/// Parse the body of a `GET /config` response into a config_info.
///
/// The body is a JSON object. Only `compatibilityLevel` (a string) is modeled,
/// as an open enum (see config.h) with the verbatim wire string kept in
/// config_info::raw. Every other top-level field is unmodeled: its name is
/// recorded in config_info::unknown_fields and its value skipped, so a caller
/// can tell config content was dropped without this client modeling the rich
/// object. As with parse_mode the shape is strict — a non-object body, a
/// missing or non-string `compatibilityLevel`, or trailing content after the
/// object yields a parse_error — while the compatibilityLevel value is open: an
/// unrecognized string maps to registry_compatibility_level::unknown rather
/// than being rejected. The function does not throw: malformed input is
/// reported via the returned std::expected.
ss::future<std::expected<config_info, parse_error>> parse_config(iobuf body);

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

/// Parse the body of a `GET /schemas/ids/{id}/versions` response into a list of
/// (subject, version) pairs.
///
/// The body must be a JSON array of objects, each with a `subject` string and a
/// `version` integer in [1, INT32_MAX] (e.g. `[{"subject":"s","version":1}]`).
/// Each subject is decoded with context_subject::from_string under \p qualified
/// (a non-default context comes back context-qualified, e.g. ":.ctx:s"), just
/// like parse_subjects. Unknown keys within an object are tolerated and
/// skipped, but both `subject` and `version` must be present; a non-array, a
/// non-object element, a wrong-typed or out-of-range field, a missing field, or
/// trailing content after the array yields a parse_error. The result order
/// follows the wire order, which the Schema Registry does not guarantee for
/// this endpoint. The function does not throw: malformed input is reported via
/// the returned std::expected.
ss::future<std::expected<chunked_vector<subject_version>, parse_error>>
parse_schema_id_subject_versions(
  iobuf body, qualified_subjects_enabled qualified);

/// Parse the body of a `GET /subjects/{subject}/versions/{version}` response
/// into a source_schema_read: the schema projected into Redpanda's supported
/// model, plus a sidecar list of unsupported fields it could not store.
///
/// This is a faithful, lenient deserialization (the lowest layer): it never
/// rejects a response merely for carrying fields it doesn't model. Server-
/// assigned fields that carry no user content (`guid`, `ts`) are dropped
/// silently. Any other unmodeled field is surfaced in
/// source_schema_read::unsupported as a JSON pointer (e.g. `/ruleSet`), for a
/// caller that needs fidelity (e.g. schema migration) to apply its policy —
/// fail, remove, or ignore. `metadata` is partially modeled:
/// `metadata.properties` is captured into the schema's metadata (values are
/// stringified, matching the write path), while any other key under `metadata`
/// (e.g. `metadata.tags`) is reported as `/metadata/<key>`. A field whose value
/// is null is treated as absent (not reported). An unmodeled key nested inside
/// another modeled field (e.g. within a reference) is skipped without being
/// reported — so `unsupported` is a best-effort signal that content was
/// dropped, not a proof of a lossless round-trip.
///
/// Absent fields take their default/sentinel (absent `schemaType` -> AVRO,
/// `deleted` -> false, `references` -> empty, `metadata` -> absent, and absent
/// `subject`/`version`/`id`/`schema` -> the invalid sentinels). It rejects only
/// inputs it cannot represent: a non-object body, malformed JSON, a present
/// modeled field with a wrong-typed or out-of-range value, or an unknown
/// `schemaType`.
///
/// \p qualified is the caller-supplied policy for interpreting
/// context-qualified subject strings (the response `subject` and each
/// reference's `subject`). The function does not throw.
ss::future<std::expected<source_schema_read, parse_error>>
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
