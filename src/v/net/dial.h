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
#include "base/vlog.h"
#include "net/types.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/net/api.hh>
#include <seastar/util/log.hh>

#include <concepts>
#include <exception>
#include <optional>
#include <vector>

namespace net::detail {

/// Establishes a TCP connection to \p address, aborting the attempt
/// (and shutting down the socket) once \p timeout passes. Failures are
/// reported with the address included in the error message.
ss::future<ss::connected_socket> dial_single(
  const ss::socket_address& address,
  clock_type::time_point timeout,
  seastar::logger* log);

/// Builds the exception reported when the overall dial deadline passes
/// before \p address could be attempted.
std::exception_ptr dial_deadline_exceeded(const ss::socket_address& address);

} // namespace net::detail

namespace net {

/// Decides how much time a single connection attempt gets when dialing
/// a list of addresses. \p addresses_remaining includes the address
/// about to be attempted. Returns nullopt when no attempt should be
/// made because \p deadline has already passed.
template<typename T>
concept DialPolicy = requires(
  const T& policy,
  clock_type::time_point now,
  clock_type::time_point deadline,
  size_t addresses_remaining) {
    {
        policy.attempt_deadline(now, deadline, addresses_remaining)
    } -> std::same_as<std::optional<clock_type::time_point>>;
};

/// Gives every attempt the same fixed timeout. Favors fallback latency: a slow
/// address is abandoned quickly in favor of the next one (or the caller's
/// retry), instead of waiting out the full deadline on it. An attempt started
/// with any time left before the overall deadline gets the full fixed timeout,
/// even if that overruns the deadline.
struct fixed_timeout_dial_policy {
    clock_type::duration attempt_timeout;

    std::optional<clock_type::time_point> attempt_deadline(
      clock_type::time_point now,
      clock_type::time_point deadline,
      size_t /*addresses_remaining*/) const {
        if (now >= deadline) {
            return std::nullopt;
        }
        return now + attempt_timeout;
    }
};

/// Dials each address in sequence, returning the first successful
/// connection. \p policy bounds each attempt; if all attempts fail the
/// error from the first address is propagated as the most relevant one.
/// \p check_abort is invoked before each attempt and may throw to
/// abort the dial: the exception propagates without trying further
/// addresses, but does not interrupt an attempt already in flight.
template<DialPolicy Policy, std::invocable CheckAbort>
ss::future<ss::connected_socket> dial_serially(
  std::vector<ss::socket_address> addresses,
  clock_type::time_point deadline,
  Policy policy,
  seastar::logger* log,
  CheckAbort check_abort) {
    std::exception_ptr first_err;
    for (size_t i = 0; i < addresses.size(); ++i) {
        check_abort();
        const auto& address = addresses[i];
        auto attempt_deadline = policy.attempt_deadline(
          clock_type::now(), deadline, addresses.size() - i);
        if (!attempt_deadline.has_value()) {
            if (!first_err) {
                first_err = detail::dial_deadline_exceeded(address);
            }
            break;
        }
        vlog(
          log->trace,
          "Connecting to {} ({}/{})",
          address,
          i + 1,
          addresses.size());
        auto f = co_await ss::coroutine::as_future(
          detail::dial_single(address, *attempt_deadline, log));
        if (!f.failed()) {
            co_return f.get();
        }
        auto err = f.get_exception();
        vlog(log->trace, "Connection to {} failed: {}", address, err);
        if (!first_err) {
            first_err = std::move(err);
        }
    }
    if (!first_err) {
        // This should be unreachable because the caller must provide at
        // least one address, but just in case...
        first_err = std::make_exception_ptr(
          std::runtime_error("no addresses to dial"));
    }
    co_return ss::coroutine::exception(std::move(first_err));
}

} // namespace net
