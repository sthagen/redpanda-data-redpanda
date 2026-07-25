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
#include "http/client.h"
#include "utils/token_bucket.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include <memory>
#include <optional>

namespace pandaproxy::schema_registry::rest_client {

/// An http::abstract_client that bounds the rate of requests sent through it
/// and honors server throttle signals. Compose it over a pooled_client so a
/// request acquires a token before it competes for a connection.
///
/// Two independent mechanisms:
/// - When a rate is configured, a token bucket caps dispatched requests per
///   second. Every attempt costs one token, so a caller's retries are charged
///   like first attempts. update_rate() retunes, enables, or disables the cap
///   live.
/// - A 429 (or 503) response carrying a Retry-After header pauses dispatch of
///   ALL requests until the indicated deadline, clamped to max_retry_after,
///   whether or not a rate is configured: the server signaled client-wide
///   overload, so every fiber backs off, not just the one that saw the
///   response. Only the delta-seconds form is recognized; the HTTP-date form
///   is ignored.
///
/// shutdown_and_stop() must be called exactly once before destruction. It
/// rejects new requests, wakes fibers waiting on the pause or on a token,
/// stops the wrapped transport, and drains.
class rate_limited_client final : public http::abstract_client {
public:
    /// Upper bound on an accepted Retry-After, so a broken or hostile server
    /// cannot park the client indefinitely.
    static constexpr std::chrono::seconds max_retry_after{60};

    /// \param inner the transport to dispatch on (typically a pooled_client)
    /// \param requests_per_sec proactive rate cap; nullopt disables it
    rate_limited_client(
      std::unique_ptr<http::abstract_client> inner,
      std::optional<size_t> requests_per_sec);

    ss::future<http::downloaded_response> request_and_collect_response(
      boost::beast::http::request_header<>&& request,
      std::optional<iobuf> payload = std::nullopt,
      ss::lowres_clock::duration timeout = http::default_connect_timeout) final;

    ss::future<> shutdown_and_stop() final;

    /// Retune the request-rate cap; nullopt disables proactive limiting.
    /// Fibers already waiting for a token are re-paced at the new rate (a
    /// disable releases them promptly).
    void update_rate(std::optional<size_t> requests_per_sec);

    /// Time left in a server-imposed Retry-After pause; zero when dispatch is
    /// not paused.
    ss::lowres_clock::duration pause_remaining() const;

private:
    // Extend the dispatch pause when the response carries a throttle signal.
    void observe_throttle_signal(const http::downloaded_response& response);

    // Resolves once dispatch is not paused, sleeping out extensions armed
    // while waiting.
    ss::future<> wait_out_pause();

    std::unique_ptr<http::abstract_client> _inner;
    // The configured cap. The bucket is engaged lazily on first enable and
    // never destroyed (waiters may be parked on it); when the cap is disabled
    // the request path skips the bucket and _bucket is re-rated so parked
    // waiters drain promptly.
    std::optional<size_t> _rate;
    std::optional<token_bucket<>> _bucket;
    ss::lowres_clock::time_point _paused_until{};
    // Wakes pause sleepers and token waiters on shutdown.
    ss::abort_source _as;
    ss::gate _gate;
};

} // namespace pandaproxy::schema_registry::rest_client
