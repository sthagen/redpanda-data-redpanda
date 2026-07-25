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
#include "pandaproxy/schema_registry/rest_client/rate_limited_client.h"

#include "base/external_fmt.h"
#include "base/vlog.h"
#include "pandaproxy/schema_registry/rest_client/logger.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>

#include <charconv>
#include <utility>

namespace pandaproxy::schema_registry::rest_client {

namespace {

// Bucket units charged per request. token_bucket grants tokens in ~50ms
// ticks with integer math (rate * 45ms / 1000ms per tick), so a bucket run
// directly in requests/sec starves forever below ~23/s: each tick rounds to
// zero tokens and the accrual window resets. Scaling by 1000 keeps every
// configured rate >= 1/s accruing fractional requests per tick.
constexpr size_t tokens_per_request = 1000;

// Stands in for "no cap" while waiters may be parked on the bucket: the
// bucket cannot be destroyed under them, so a disable re-rates it high enough
// to be no practical constraint and the request path stops consulting it.
constexpr size_t effectively_unlimited_rps = 1'000'000'000;

// The bucket rate for a requests/sec cap; capacity follows the rate, i.e. up
// to one second of burst.
constexpr size_t bucket_rate(size_t requests_per_sec) {
    return requests_per_sec * tokens_per_request;
}

// Parse the delta-seconds form of Retry-After, clamped to max_retry_after.
// The clamp happens before the duration is constructed: chrono::seconds has a
// signed rep, so an unclamped value above int64 max would wrap negative and
// silently disable the pause instead of bounding it. The HTTP-date form (and
// any other unparseable value) yields nullopt and is ignored.
std::optional<std::chrono::seconds>
parse_retry_after(const boost::beast::http::fields& headers) {
    auto it = headers.find(boost::beast::http::field::retry_after);
    if (it == headers.end()) {
        return std::nullopt;
    }
    auto value = it->value();
    uint64_t seconds{};
    auto [ptr, ec] = std::from_chars(
      value.data(), value.data() + value.size(), seconds);
    if (ec != std::errc{} || ptr != value.data() + value.size()) {
        return std::nullopt;
    }
    constexpr auto max_seconds = static_cast<uint64_t>(
      rate_limited_client::max_retry_after.count());
    return std::chrono::seconds{
      static_cast<std::chrono::seconds::rep>(std::min(seconds, max_seconds))};
}

} // namespace

rate_limited_client::rate_limited_client(
  std::unique_ptr<http::abstract_client> inner,
  std::optional<size_t> requests_per_sec)
  : _inner(std::move(inner)) {
    update_rate(requests_per_sec);
}

void rate_limited_client::update_rate(std::optional<size_t> requests_per_sec) {
    if (requests_per_sec == _rate) {
        return;
    }
    if (requests_per_sec.has_value()) {
        if (_bucket.has_value()) {
            _bucket->update_rate(bucket_rate(*requests_per_sec));
        } else {
            _bucket.emplace(
              bucket_rate(*requests_per_sec),
              "schema_registry/rest_client/rate");
        }
    } else if (_bucket.has_value()) {
        _bucket->update_rate(bucket_rate(effectively_unlimited_rps));
    }
    _rate = requests_per_sec;
}

ss::lowres_clock::duration rate_limited_client::pause_remaining() const {
    auto now = ss::lowres_clock::now();
    return _paused_until > now ? _paused_until - now
                               : ss::lowres_clock::duration::zero();
}

void rate_limited_client::observe_throttle_signal(
  const http::downloaded_response& response) {
    using enum boost::beast::http::status;
    if (
      response.status != too_many_requests
      && response.status != service_unavailable) {
        return;
    }
    auto retry_after = parse_retry_after(response.headers);
    if (!retry_after.has_value()) {
        return;
    }
    auto deadline = ss::lowres_clock::now() + *retry_after;
    if (deadline > _paused_until) {
        _paused_until = deadline;
        vlog(
          srclog.info,
          "source signaled {} with Retry-After; pausing requests for {}s",
          response.status,
          retry_after->count());
    }
}

ss::future<> rate_limited_client::wait_out_pause() {
    // Loop: a concurrent throttled response may extend the pause while this
    // fiber sleeps.
    while (true) {
        auto now = ss::lowres_clock::now();
        if (now >= _paused_until) {
            co_return;
        }
        co_await ss::sleep_abortable<ss::lowres_clock>(
          _paused_until - now, _as);
    }
}

ss::future<http::downloaded_response>
rate_limited_client::request_and_collect_response(
  boost::beast::http::request_header<>&& request,
  std::optional<iobuf> payload,
  ss::lowres_clock::duration timeout) {
    // Move the request into this frame before any suspension so it does not
    // depend on the caller's object while paused or waiting for a token.
    auto owned_request = std::move(request);
    auto holder = _gate.hold();
    // Wait out a server-imposed pause first, then pay for a token, so a
    // backlog released from a pause is still paced by the bucket.
    co_await wait_out_pause();
    if (_rate.has_value()) {
        // value(): a configured rate implies an engaged bucket (update_rate
        // maintains this); throw rather than dereference if that invariant is
        // ever broken.
        co_await _bucket.value().throttle(tokens_per_request, _as);
        // A throttled response may have armed the pause while this fiber
        // waited for its token; dispatching now would leak through the pause,
        // so sleep it out too. The token stays spent: the request still
        // dispatches exactly once.
        co_await wait_out_pause();
    }
    auto response = co_await ss::coroutine::as_future(
      _inner->request_and_collect_response(
        std::move(owned_request), std::move(payload), timeout));
    if (response.failed()) {
        co_return co_await std::move(response);
    }
    auto value = std::move(response).get();
    observe_throttle_signal(value);
    co_return std::move(value);
}

ss::future<> rate_limited_client::shutdown_and_stop() {
    auto drained = _gate.close();
    // Wakes pause sleepers (sleep_aborted) and token waiters
    // (semaphore_aborted); the bucket itself needs no shutdown since every
    // wait goes through _as.
    _as.request_abort();
    co_await _inner->shutdown_and_stop();
    co_await std::move(drained);
}

} // namespace pandaproxy::schema_registry::rest_client
