/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "http/request_builder.h"
#include "utils/named_type.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <boost/beast/http/status.hpp>

#include <ada.h>

namespace iceberg::rest_client {

struct http_status_error {
    boost::beast::http::status status;
    ss::sstring body;

    fmt::iterator format_to(fmt::iterator it) const {
        it = fmt::format_to(it, "{}", status);
        if (!body.empty()) {
            it = fmt::format_to(it, ": {}", body);
        }
        return it;
    }
};

// An error seen during an http call, represented either by a status code with
// response body, or a string in case of an exception.
// TODO - use exception_ptr instead of string
using http_call_error = std::variant<http_status_error, ss::sstring>;

using parse_error_msg = named_type<ss::sstring, struct parse_error_msg_t>;

struct json_parse_error {
    ss::sstring context;
    parse_error_msg error;
};

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

// Represents the sum of all error types which can be encountered during
// rest-client operations.
using domain_error = std::variant<
  http::url_build_error,
  json_parse_error,
  http_call_error,
  retries_exhausted,
  aborted_error>;

// The core result type used by all operations in the iceberg/rest-client which
// can fail. Allows chaining of operations together and short-circuiting when an
// earlier operation in the chain has failed.
template<typename T>
using expected = tl::expected<T, domain_error>;

} // namespace iceberg::rest_client
template<>
struct fmt::formatter<iceberg::rest_client::domain_error>
  : fmt::formatter<std::string_view> {
    auto format(
      const iceberg::rest_client::domain_error&, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<iceberg::rest_client::error_kind>
  : fmt::formatter<std::string_view> {
    auto
    format(iceberg::rest_client::error_kind r, fmt::format_context& ctx) const
      -> decltype(ctx.out()) {
        return fmt::formatter<std::string_view>::format(
          iceberg::rest_client::to_string_view(r), ctx);
    }
};

template<>
struct fmt::formatter<iceberg::rest_client::http_call_error>
  : fmt::formatter<std::string_view> {
    auto format(
      const iceberg::rest_client::http_call_error&,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};
