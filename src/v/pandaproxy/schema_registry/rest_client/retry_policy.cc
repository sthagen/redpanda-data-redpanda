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

#include "pandaproxy/schema_registry/rest_client/retry_policy.h"

#include "bytes/iobuf.h"
#include "net/connection.h"
#include "pandaproxy/schema_registry/rest_client/logger.h"

#include <exception>

namespace {

using pandaproxy::schema_registry::rest_client::error_kind;
using pandaproxy::schema_registry::rest_client::request_error;

request_error aborted(std::string_view msg) {
    return {.kind = error_kind::aborted, .err = ss::sstring{msg}};
}

request_error make_permanent_failure(std::string_view msg) {
    return {.kind = error_kind::permanent_failure, .err = ss::sstring{msg}};
}

request_error retriable(error_kind kind, std::string_view msg) {
    return {.kind = kind, .err = ss::sstring{msg}};
}

using enum boost::beast::http::status;
constexpr auto retriable_statuses = std::to_array(
  {internal_server_error,
   bad_gateway,
   service_unavailable,
   gateway_timeout,
   request_timeout,
   too_many_requests});

bool is_abort_or_gate_close_exception(const std::exception_ptr& ex) {
    try {
        std::rethrow_exception(ex);
    } catch (const ss::abort_requested_exception&) {
        return true;
    } catch (const ss::gate_closed_exception&) {
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

namespace pandaproxy::schema_registry::rest_client {

retry_policy::result_t default_retry_policy::should_retry(
  ss::future<http::downloaded_response> response_f) const {
    vassert(response_f.available(), "future is not resolved");
    if (!response_f.failed()) {
        // future resolved successfully, check the status code
        return should_retry(response_f.get());
    } else {
        // future failed, check the exception kind
        return std::unexpected(should_retry(response_f.get_exception()));
    }
}

retry_policy::result_t
default_retry_policy::should_retry(http::downloaded_response response) const {
    const auto status = response.status;
    // the successful status class contains all status codes starting with 2
    if (
      boost::beast::http::to_status_class(status)
      == boost::beast::http::status_class::successful) {
        return response;
    }

    auto kind = std::ranges::find(retriable_statuses, status)
                    != retriable_statuses.end()
                  ? error_kind::retriable_http_status
                  : error_kind::permanent_failure;
    // The error surfaced to callers truncates the body (below) to keep error
    // messages bounded. Log the full body at trace level so it can be recovered
    // when 400 bytes is not enough to diagnose a failure. Capped at the
    // linearize limit since linearize_to_string() throws above it; share()
    // clamps to the available size, so this is safe for any body size.
    if (srclog.is_enabled(ss::log_level::trace)) {
        vlog(
          srclog.trace,
          "schema registry error response (status={}): {}",
          status,
          response.body.share(0, iobuf::max_linearize_size)
            .linearize_to_string());
    }
    constexpr size_t max_body_size = 400;
    auto body = response.body.share(0, max_body_size).linearize_to_string();
    return std::unexpected(
      request_error{
        .kind = kind,
        .err = http_status_error{.status = status, .body = std::move(body)}});
}

request_error default_retry_policy::should_retry(std::exception_ptr ex) const {
    try {
        std::rethrow_exception(ex);
    } catch (const std::system_error& err) {
        if (net::is_reconnect_error(err)) {
            return retriable(error_kind::network_error, err.what());
        }
        return make_permanent_failure(err.what());
    } catch (const ss::timed_out_error& err) {
        return retriable(error_kind::timeout, err.what());
    } catch (const boost::system::system_error& err) {
        if (
          err.code() != boost::beast::http::error::end_of_stream
          && err.code() != boost::beast::http::error::partial_message) {
            return make_permanent_failure(err.what());
        }
        return retriable(error_kind::network_error, err.what());
    } catch (const ss::gate_closed_exception&) {
        return aborted(fmt::format("{}", std::current_exception()));
    } catch (const ss::abort_requested_exception&) {
        return aborted(fmt::format("{}", std::current_exception()));
    } catch (const ss::nested_exception& nested) {
        if (
          is_abort_or_gate_close_exception(nested.inner)
          || is_abort_or_gate_close_exception(nested.outer)) {
            return aborted(fmt::format("{}", std::current_exception()));
        };
        return make_permanent_failure(
          fmt::format(
            "{} [outer: {}, inner: {}]",
            nested.what(),
            nested.outer,
            nested.inner));
    } catch (...) {
        return make_permanent_failure(
          fmt::format("{}", std::current_exception()));
    }
}

} // namespace pandaproxy::schema_registry::rest_client
