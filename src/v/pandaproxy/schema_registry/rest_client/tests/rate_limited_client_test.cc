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

#include "bytes/iobuf.h"
#include "http/client.h"
#include "pandaproxy/schema_registry/rest_client/rate_limited_client.h"
#include "test_utils/async.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace rc = pandaproxy::schema_registry::rest_client;
namespace bh = boost::beast::http;
using namespace std::chrono_literals;

namespace {

// Records dispatch times and answers every request with a canned response.
class fake_transport final : public http::abstract_client {
public:
    ss::future<http::downloaded_response> request_and_collect_response(
      bh::request_header<>&& request,
      std::optional<iobuf>,
      ss::lowres_clock::duration) final {
        dispatch_times.push_back(ss::lowres_clock::now());
        targets.emplace_back(request.target().begin(), request.target().end());
        if (park) {
            parked.emplace_back();
            return parked.back().get_future();
        }
        auto response = http::downloaded_response{
          .status = _status, .body = iobuf{}, .headers = _headers};
        // One-shot: after replying with the canned throttle signal once,
        // subsequent requests succeed, mirroring a server whose window
        // reopened.
        if (_one_shot) {
            _status = bh::status::ok;
            _headers = bh::fields{};
            _one_shot = false;
        }
        return ss::make_ready_future<http::downloaded_response>(
          std::move(response));
    }

    ss::future<> shutdown_and_stop() final {
        stopped = true;
        return ss::make_ready_future<>();
    }

    void respond_once_with(
      bh::status status, std::optional<std::string> retry_after) {
        _status = status;
        _headers = bh::fields{};
        if (retry_after.has_value()) {
            _headers.set(bh::field::retry_after, *retry_after);
        }
        _one_shot = true;
    }

    // Completes the oldest parked request with the given response.
    void
    release_one(bh::status status, std::optional<std::string> retry_after) {
        ASSERT_FALSE(parked.empty());
        bh::fields headers;
        if (retry_after.has_value()) {
            headers.set(bh::field::retry_after, *retry_after);
        }
        auto request = std::move(parked.front());
        parked.pop_front();
        request.set_value(
          http::downloaded_response{
            .status = status, .body = iobuf{}, .headers = std::move(headers)});
    }

    std::vector<ss::lowres_clock::time_point> dispatch_times;
    std::vector<std::string> targets;
    bool stopped{false};
    // When true, requests park until release_one().
    bool park{false};
    std::deque<ss::promise<http::downloaded_response>> parked;

private:
    bh::status _status{bh::status::ok};
    bh::fields _headers;
    bool _one_shot{false};
};

struct limiter_fixture {
    explicit limiter_fixture(std::optional<size_t> rate) {
        auto owned = std::make_unique<fake_transport>();
        transport = owned.get();
        limiter = std::make_unique<rc::rate_limited_client>(
          std::move(owned), rate);
    }

    ss::future<http::downloaded_response> issue(std::string_view target) {
        bh::request_header<> header;
        header.method(bh::verb::get);
        header.target(target);
        co_return co_await limiter->request_and_collect_response(
          std::move(header));
    }

    fake_transport* transport;
    std::unique_ptr<rc::rate_limited_client> limiter;
};

} // namespace

TEST(rate_limited_client, unlimited_mode_passes_through_immediately) {
    limiter_fixture fx(std::nullopt);
    auto start = ss::lowres_clock::now();
    for (int i = 0; i < 20; ++i) {
        auto res = fx.issue("/r").get();
        EXPECT_EQ(res.status, bh::status::ok);
    }
    EXPECT_EQ(fx.transport->dispatch_times.size(), 20);
    // No pacing: a generous bound that a single throttled request would blow.
    EXPECT_LT(ss::lowres_clock::now() - start, 5s);
    EXPECT_EQ(fx.limiter->pause_remaining(), 0ms);
    fx.limiter->shutdown_and_stop().get();
    EXPECT_TRUE(fx.transport->stopped);
}

TEST(rate_limited_client, paces_requests_at_configured_rate) {
    // rate 5/s with burst capacity 5: the sixth request must wait for a
    // refresh (~200ms of accrual at this rate).
    limiter_fixture fx(5);
    auto start = ss::lowres_clock::now();
    for (int i = 0; i < 6; ++i) {
        fx.issue("/r").get();
    }
    auto elapsed = ss::lowres_clock::now() - start;
    EXPECT_EQ(fx.transport->dispatch_times.size(), 6);
    EXPECT_GE(elapsed, 100ms) << "sixth request was not paced";
    fx.limiter->shutdown_and_stop().get();
}

TEST(rate_limited_client, retry_after_pauses_subsequent_dispatch) {
    limiter_fixture fx(std::nullopt);
    fx.transport->respond_once_with(bh::status::too_many_requests, "1");

    // The throttled response is returned to the caller (retrying is the
    // caller's job), and the pause is now armed.
    auto throttled = fx.issue("/a").get();
    EXPECT_EQ(throttled.status, bh::status::too_many_requests);
    EXPECT_GT(fx.limiter->pause_remaining(), 500ms);

    auto start = ss::lowres_clock::now();
    auto res = fx.issue("/b").get();
    EXPECT_EQ(res.status, bh::status::ok);
    EXPECT_GE(ss::lowres_clock::now() - start, 900ms)
      << "dispatch was not paused for Retry-After";
    fx.limiter->shutdown_and_stop().get();
}

TEST(rate_limited_client, retry_after_is_clamped) {
    // A day, and a value above chrono::seconds' signed rep (uint64_t max):
    // both clamp to max_retry_after rather than overflowing or being dropped.
    for (auto huge : {"86400", "18446744073709551615"}) {
        limiter_fixture fx(std::nullopt);
        fx.transport->respond_once_with(bh::status::too_many_requests, huge);
        fx.issue("/a").get();
        EXPECT_GT(fx.limiter->pause_remaining(), 30s) << "value=" << huge;
        EXPECT_LE(
          fx.limiter->pause_remaining(),
          rc::rate_limited_client::max_retry_after)
          << "value=" << huge;
        fx.limiter->shutdown_and_stop().get();
    }
}

TEST(rate_limited_client, service_unavailable_with_retry_after_pauses) {
    limiter_fixture fx(std::nullopt);
    fx.transport->respond_once_with(bh::status::service_unavailable, "30");
    fx.issue("/a").get();
    EXPECT_GT(fx.limiter->pause_remaining(), 20s);
    fx.limiter->shutdown_and_stop().get();
}

TEST(rate_limited_client, throttle_signal_without_or_bad_header_is_ignored) {
    {
        // 429 with no Retry-After: reactive backoff is the caller's retry
        // policy; no client-wide pause.
        limiter_fixture fx(std::nullopt);
        fx.transport->respond_once_with(
          bh::status::too_many_requests, std::nullopt);
        fx.issue("/a").get();
        EXPECT_EQ(fx.limiter->pause_remaining(), 0ms);
        fx.limiter->shutdown_and_stop().get();
    }
    {
        // HTTP-date form is not parsed in v1.
        limiter_fixture fx(std::nullopt);
        fx.transport->respond_once_with(
          bh::status::too_many_requests, "Fri, 31 Dec 2027 23:59:59 GMT");
        fx.issue("/a").get();
        EXPECT_EQ(fx.limiter->pause_remaining(), 0ms);
        fx.limiter->shutdown_and_stop().get();
    }
    {
        // A value from_chars cannot represent at all (> uint64_t max) is
        // ignored rather than undefined.
        limiter_fixture fx(std::nullopt);
        fx.transport->respond_once_with(
          bh::status::too_many_requests, "99999999999999999999");
        fx.issue("/a").get();
        EXPECT_EQ(fx.limiter->pause_remaining(), 0ms);
        fx.limiter->shutdown_and_stop().get();
    }
    {
        // A plain error status must not pause dispatch.
        limiter_fixture fx(std::nullopt);
        fx.transport->respond_once_with(bh::status::internal_server_error, "5");
        fx.issue("/a").get();
        EXPECT_EQ(fx.limiter->pause_remaining(), 0ms);
        fx.limiter->shutdown_and_stop().get();
    }
}

TEST(rate_limited_client, update_rate_disable_releases_waiters) {
    // rate 1/s, capacity 1: the second request parks on the bucket.
    limiter_fixture fx(1);
    fx.issue("/a").get();
    auto waiting = fx.issue("/b");
    ss::yield().get();
    EXPECT_EQ(fx.transport->dispatch_times.size(), 1);

    fx.limiter->update_rate(std::nullopt);
    // Released promptly: well before the ~1s a token would have taken.
    auto start = ss::lowres_clock::now();
    waiting.get();
    EXPECT_LT(ss::lowres_clock::now() - start, 500ms);
    EXPECT_EQ(fx.transport->dispatch_times.size(), 2);

    // Disabled mode dispatches immediately.
    fx.issue("/c").get();
    EXPECT_EQ(fx.transport->dispatch_times.size(), 3);
    fx.limiter->shutdown_and_stop().get();
}

TEST(rate_limited_client, update_rate_enables_limiting_live) {
    limiter_fixture fx(std::nullopt);
    fx.issue("/a").get();
    fx.limiter->update_rate(5);
    auto start = ss::lowres_clock::now();
    // Burst capacity 5, so the sixth paced request waits.
    for (int i = 0; i < 6; ++i) {
        fx.issue("/r").get();
    }
    EXPECT_GE(ss::lowres_clock::now() - start, 100ms);
    fx.limiter->shutdown_and_stop().get();
}

TEST(rate_limited_client, pause_armed_while_waiting_for_token_is_honored) {
    // rate 1/s: the first request drains the burst capacity, so the second
    // parks in the token wait for ~1s.
    limiter_fixture fx(1);
    fx.transport->park = true;
    auto first = fx.issue("/a");
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&] { return fx.transport->parked.size() == 1; });
    auto second = fx.issue("/b");
    ss::yield().get();
    EXPECT_EQ(fx.transport->dispatch_times.size(), 1);

    // Answer the first request with a throttle signal while the second is
    // still waiting for its token: the pause armed here must gate the second
    // request's dispatch even though it already passed the pause check.
    auto armed_at = ss::lowres_clock::now();
    fx.transport->release_one(bh::status::too_many_requests, "3");
    EXPECT_EQ(first.get().status, bh::status::too_many_requests);
    EXPECT_GT(fx.limiter->pause_remaining(), 1s);

    // The second request gets its token after ~1s but must sleep out the
    // pause (~3s) before dispatching.
    RPTEST_REQUIRE_EVENTUALLY(
      10s, [&] { return fx.transport->parked.size() == 1; });
    EXPECT_GE(ss::lowres_clock::now() - armed_at, 2s)
      << "dispatch leaked through a pause armed during the token wait";
    fx.transport->release_one(bh::status::ok, std::nullopt);
    EXPECT_EQ(second.get().status, bh::status::ok);
    fx.limiter->shutdown_and_stop().get();
}

TEST(rate_limited_client, shutdown_wakes_paused_and_waiting_requests) {
    limiter_fixture fx(1);
    fx.transport->respond_once_with(bh::status::too_many_requests, "60");
    fx.issue("/a").get();
    EXPECT_GT(fx.limiter->pause_remaining(), 30s);

    // Parked sleeping out the pause.
    auto paused = fx.issue("/b");
    ss::yield().get();
    EXPECT_EQ(fx.transport->dispatch_times.size(), 1);

    fx.limiter->shutdown_and_stop().get();
    EXPECT_THROW(paused.get(), ss::sleep_aborted);
    EXPECT_TRUE(fx.transport->stopped);
    EXPECT_THROW(fx.issue("/c").get(), ss::gate_closed_exception);
}
