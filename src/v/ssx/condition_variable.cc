/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "ssx/condition_variable.h"

#include <memory>

namespace ssx {

bool setup_waiter(
  condition_variable::waiter& w,
  ss::abort_source& as,
  condition_variable::abort_subscription_holder& h) {
    auto sub = as.subscribe([&w, &as] noexcept {
        w.set_exception(as.abort_requested_exception_ptr());
    });
    if (!sub) {
        w.set_exception(as.abort_requested_exception_ptr());
        return false;
    }
    h.sub = std::move(*sub);
    return true;
}

bool setup_waiter(
  condition_variable::waiter& w,
  ss::manual_clock::time_point tp,
  ss::timer<ss::manual_clock>& t) {
    // A bug in seastar, if a manual clock is past now, the timer doesn't
    // fire. This is fine with lowres, since it is checked every tick of the
    // reactor.
    if (tp <= ss::manual_clock::now()) {
        w.set_exception(
          std::make_exception_ptr(ss::condition_variable_timed_out{}));
        return false;
    }
    t.set_callback([&w] {
        w.set_exception(
          std::make_exception_ptr(ss::condition_variable_timed_out{}));
    });
    t.arm(tp);
    return true;
}

void setup_waiter(
  condition_variable::waiter& w,
  ss::lowres_clock::time_point tp,
  ss::timer<ss::lowres_clock>& t) {
    t.set_callback([&w] {
        w.set_exception(
          std::make_exception_ptr(ss::condition_variable_timed_out{}));
    });
    t.arm(tp);
}

void condition_variable::waiter::signal() noexcept {
    set_value();
    delete this;
}

void condition_variable::waiter::set_exception(
  const std::exception_ptr& ex) noexcept {
    promise<>::set_exception(ex);
    delete this;
}

condition_variable::~condition_variable() { broken(); }

void condition_variable::add_waiter(waiter& w) noexcept {
    if (_ex) {
        w.set_exception(_ex);
        return;
    }
    _waiters.push_back(w);
}

bool condition_variable::wakeup_first() noexcept {
    if (_waiters.empty()) {
        return false;
    }
    auto& w = _waiters.front();
    _waiters.pop_front();
    if (_ex) {
        w.set_exception(_ex);
    } else {
        w.signal();
    }
    return true;
}

bool condition_variable::check_and_consume_signal() noexcept {
    return std::exchange(_signalled, false);
}

void condition_variable::signal() noexcept {
    if (!wakeup_first()) {
        _signalled = true;
    }
}

void condition_variable::broadcast() noexcept {
    auto tmp(std::move(_waiters));
    while (!tmp.empty()) {
        auto& w = tmp.front();
        tmp.pop_front();
        if (_ex) {
            w.set_exception(_ex);
        } else {
            w.signal();
        }
    }
}

void condition_variable::broken() noexcept {
    broken(std::make_exception_ptr(ss::broken_condition_variable()));
}

void condition_variable::broken(const std::exception_ptr& ep) noexcept {
    _ex = ep;
    broadcast();
}

ss::future<> condition_variable::wait(ss::abort_source& as) noexcept {
    if (check_and_consume_signal()) {
        return ss::make_ready_future();
    }
    struct waiter_t
      : public waiter
      , public abort_subscription_holder {};
    auto* w = std::make_unique<waiter_t>().release();
    auto f = w->get_future();
    if (!setup_waiter(*w, as, *w)) {
        return f;
    }
    add_waiter(*w);
    return f;
}

ss::future<>
condition_variable::wait(ss::lowres_clock::time_point timeout) noexcept {
    if (check_and_consume_signal()) {
        return ss::make_ready_future();
    }
    struct waiter_t
      : public waiter
      , public ss::timer<ss::lowres_clock> {};
    auto* w = std::make_unique<waiter_t>().release();
    auto f = w->get_future();
    setup_waiter(*w, timeout, *w);
    add_waiter(*w);
    return f;
}

ss::future<>
condition_variable::wait(ss::manual_clock::time_point timeout) noexcept {
    if (check_and_consume_signal()) {
        return ss::make_ready_future();
    }
    struct waiter_t
      : public waiter
      , public ss::timer<ss::manual_clock> {};
    auto* w = std::make_unique<waiter_t>().release();
    auto f = w->get_future();
    if (!setup_waiter(*w, timeout, *w)) {
        return f;
    }
    add_waiter(*w);
    return f;
}

ss::future<> condition_variable::wait(
  ss::lowres_clock::time_point timeout, ss::abort_source& as) noexcept {
    if (check_and_consume_signal()) {
        return ss::make_ready_future();
    }
    struct waiter_t
      : public waiter
      , public ss::timer<ss::lowres_clock>
      , abort_subscription_holder {};
    auto* w = std::make_unique<waiter_t>().release();
    auto f = w->get_future();
    if (!setup_waiter(*w, as, *w)) {
        return f;
    }
    setup_waiter(*w, timeout, *w);
    add_waiter(*w);
    return f;
}

ss::future<> condition_variable::wait(
  ss::manual_clock::time_point timeout, ss::abort_source& as) noexcept {
    if (check_and_consume_signal()) {
        return ss::make_ready_future();
    }
    struct waiter_t
      : public waiter
      , public ss::timer<ss::manual_clock>
      , abort_subscription_holder {};
    auto* w = std::make_unique<waiter_t>().release();
    auto f = w->get_future();
    if (!setup_waiter(*w, as, *w)) {
        return f;
    }
    if (!setup_waiter(*w, timeout, *w)) {
        return f;
    }
    add_waiter(*w);
    return f;
}

} // namespace ssx
