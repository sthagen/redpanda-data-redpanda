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

#include "base/external_fmt.h"
#include "base/format_to.h"
#include "base/seastarx.h"
#include "http/request_builder.h"
#include "pandaproxy/schema_registry/rest_client/parse.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <boost/beast/http/status.hpp>
#include <fmt/ranges.h>

#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace pandaproxy::schema_registry::rest_client {

struct http_status_error {
    boost::beast::http::status status;
    ss::sstring body;
    // The Schema Registry `error_code` from the response body, when one could
    // be parsed. It is finer-grained than the HTTP status: e.g. a 404 may carry
    // 40401 (subject not found) or 40402 (version not found). Left empty when
    // there is no parseable body.
    std::optional<int32_t> error_code;
    // The human-readable `message` from the Schema Registry error body, when
    // one was present (typically a fixed string templated on the
    // subject/context/version/id). Captured so the available detail is
    // discoverable from the interface; note the raw `body` above is what
    // currently gets logged. Empty when there is no parseable body or no
    // `message` field.
    std::optional<ss::sstring> message;

    fmt::iterator format_to(fmt::iterator it) const {
        it = fmt::format_to(it, "{}", status);
        if (error_code.has_value()) {
            it = fmt::format_to(it, " (error_code={})", *error_code);
        }
        if (!body.empty()) {
            it = fmt::format_to(it, ": {}", body);
        }
        return it;
    }
};

// An error seen during an http call, represented either by a status code with
// response body, or a string in case of an exception.
using http_call_error = std::variant<http_status_error, ss::sstring>;

enum class error_kind {
    permanent_failure,
    aborted,
    retriable_http_status,
    network_error,
    timeout,
};

constexpr std::string_view to_string_view(error_kind r) {
    using enum error_kind;
    switch (r) {
    case permanent_failure:
        return "permanent_failure";
    case aborted:
        return "aborted";
    case retriable_http_status:
        return "retriable_http_status";
    case network_error:
        return "network_error";
    case timeout:
        return "timeout";
    }
}

inline fmt::iterator format_to(error_kind r, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(r));
}

constexpr bool is_retriable(error_kind r) {
    switch (r) {
    case error_kind::retriable_http_status:
    case error_kind::network_error:
    case error_kind::timeout:
        return true;
    case error_kind::permanent_failure:
    case error_kind::aborted:
        return false;
    }
}

struct retries_exhausted {
    std::vector<error_kind> reasons;
    std::optional<http_call_error> last_error;
};

// Error returned when the underlying subsystems are being shut down.
using aborted_error = named_type<ss::sstring, struct aborted_tag>;

// The requested subject does not exist (HTTP 404 / error_code 40401).
struct subject_not_found {
    context_subject subject;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "subject not found: {}", subject);
    }
};

// The requested version of an existing subject does not exist
// (HTTP 404 / error_code 40402).
struct version_not_found {
    context_subject subject;
    schema_version version;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it, "version not found: subject={} version={}", subject, version());
    }
};

// Represents the sum of all error types which can be encountered during
// rest-client operations. The JSON-decode arm reuses parse_error from parse.h.
using domain_error = std::variant<
  http::url_build_error,
  parse_error,
  http_call_error,
  retries_exhausted,
  subject_not_found,
  version_not_found,
  aborted_error>;

} // namespace pandaproxy::schema_registry::rest_client

template<>
struct fmt::formatter<pandaproxy::schema_registry::rest_client::http_call_error>
  : fmt::formatter<std::string_view> {
    auto format(
      const pandaproxy::schema_registry::rest_client::http_call_error& err,
      fmt::format_context& ctx) const -> decltype(ctx.out()) {
        return std::visit(
          [&ctx](const auto& value) {
              return fmt::format_to(ctx.out(), "http_call_error: {}", value);
          },
          err);
    }
};

template<>
struct fmt::formatter<pandaproxy::schema_registry::rest_client::domain_error>
  : fmt::formatter<std::string_view> {
    auto format(
      const pandaproxy::schema_registry::rest_client::domain_error& err,
      fmt::format_context& ctx) const -> decltype(ctx.out()) {
        namespace rc = pandaproxy::schema_registry::rest_client;
        return ss::visit(
          err,
          [&](const http::url_build_error& value) {
              return fmt::format_to(ctx.out(), "url_build_error: {}", value);
          },
          [&](const rc::parse_error& value) {
              return fmt::format_to(ctx.out(), "parse_error: {}", value.reason);
          },
          [&](const rc::http_call_error& value) {
              return fmt::format_to(ctx.out(), "{}", value);
          },
          [&](const rc::retries_exhausted& value) {
              auto it = fmt::format_to(
                ctx.out(),
                "retries_exhausted:[reasons=[{}]",
                fmt::join(value.reasons, ", "));
              if (value.last_error.has_value()) {
                  it = fmt::format_to(it, ", last_error={}", *value.last_error);
              }
              return fmt::format_to(it, "]");
          },
          [&](const rc::subject_not_found& value) {
              return fmt::format_to(ctx.out(), "{}", value);
          },
          [&](const rc::version_not_found& value) {
              return fmt::format_to(ctx.out(), "{}", value);
          },
          [&](const rc::aborted_error& value) {
              return fmt::format_to(ctx.out(), "aborted_error: {}", value);
          });
    }
};
