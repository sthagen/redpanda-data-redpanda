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
#include "http/client.h"
#include "pandaproxy/schema_registry/rest_client/credentials.h"
#include "pandaproxy/schema_registry/rest_client/error.h"
#include "pandaproxy/schema_registry/rest_client/parse.h"
#include "pandaproxy/schema_registry/rest_client/retry_policy.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>

#include <expected>
#include <memory>
#include <optional>

namespace http {
class request_builder;
} // namespace http

namespace pandaproxy::schema_registry::rest_client {

/// A reusable client for the Schema Registry REST API, usable against both
/// Redpanda's own Schema Registry and third-party Confluent-compatible
/// registries. The HTTP transport is injected (http::abstract_client) so it can
/// be mocked in tests. A client is lightweight; create one per registry
/// endpoint. shutdown() must be called before destruction.
class client {
public:
    /// \param http_client the HTTP transport to issue requests on
    /// \param endpoint the registry's scheme+host (e.g. "https://sr:8081")
    /// \param credentials optional HTTP Basic auth credentials; absent means no
    ///        Authorization header is sent
    /// \param qualified policy for interpreting context-qualified subject
    ///        strings in responses (see context_subject::from_string); supplied
    ///        by the caller so parsing does not depend on this node's config
    /// \param retry_policy classifies failures as retriable/permanent; defaults
    ///        to default_retry_policy when null. The retry budget/backoff comes
    ///        from the per-call retry_chain_node, not this policy.
    client(
      std::unique_ptr<http::abstract_client> http_client,
      ss::sstring endpoint,
      std::optional<basic_auth_credentials> credentials = std::nullopt,
      qualified_subjects_enabled qualified = qualified_subjects_enabled::no,
      std::unique_ptr<retry_policy> retry_policy = nullptr);

    client(const client&) = delete;
    client& operator=(const client&) = delete;
    client(client&&) = delete;
    client& operator=(client&&) = delete;
    ~client() = default;

    /// GET /subjects — list all subjects across all contexts. With \p deleted
    /// set to yes, soft-deleted subjects are included in the listing.
    ss::future<std::expected<chunked_vector<context_subject>, domain_error>>
    list_subjects(
      retry_chain_node& rtc, include_deleted deleted = include_deleted::no);

    /// GET /subjects/{subject}/versions — list the version numbers registered
    /// under \p subject. With \p deleted set to yes, soft-deleted versions are
    /// included. A missing subject yields subject_not_found.
    ss::future<std::expected<chunked_vector<schema_version>, domain_error>>
    list_subject_versions(
      const context_subject& subject,
      retry_chain_node& rtc,
      include_deleted deleted = include_deleted::no);

    /// GET /subjects/{subject}/versions/{version} — fetch one version of a
    /// subject's schema. With \p deleted set to yes, a soft-deleted version can
    /// be fetched. A missing subject yields subject_not_found; a missing
    /// version (of an existing subject) yields version_not_found.
    ///
    /// The result wraps the schema together with the names of any response
    /// fields the client did not model (parsed_schema::unknown_fields); a
    /// caller that needs fidelity can inspect them and apply its own strictness
    /// policy.
    ss::future<std::expected<parsed_schema, domain_error>>
    get_schema_by_version(
      const context_subject& subject,
      schema_version version,
      retry_chain_node& rtc,
      include_deleted deleted = include_deleted::no);

    /// Stops the transport and drains in-flight requests. Must be called before
    /// destroying the client.
    ss::future<> shutdown();

private:
    std::expected<ss::gate::holder, domain_error> maybe_gate();
    void maybe_add_basic_auth(http::request_builder& request);

    // Builds and issues the request against _endpoint, retrying according to
    // the retry policy and the supplied retry_chain_node, and returns the
    // collected response body. A permanent http_status_error is enriched with
    // the parsed error_code before being returned.
    ss::future<std::expected<iobuf, domain_error>> perform_request(
      retry_chain_node& parent_rtc,
      http::request_builder builder,
      std::optional<iobuf> payload = std::nullopt);

    ss::gate _gate;
    std::unique_ptr<http::abstract_client> _http_client;
    ss::sstring _endpoint;
    std::optional<basic_auth_credentials> _credentials;
    qualified_subjects_enabled _qualified;
    std::unique_ptr<retry_policy> _retry_policy;
};

} // namespace pandaproxy::schema_registry::rest_client
