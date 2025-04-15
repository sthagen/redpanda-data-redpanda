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

#pragma once

#include <seastar/core/lowres_clock.hh>

#include <functional>
#include <optional>

namespace ssx {

/// A rate-limited function that caches the result of the function call
/// for a specified duration.
template<typename Signature, typename Clock = seastar::lowres_clock>
class rate_limited_function;

template<typename Ret, bool Noexcept, typename Clock>
class rate_limited_function<Ret() noexcept(Noexcept), Clock> {
public:
    template<class Fn>
    rate_limited_function(Fn&& func, Clock::duration rate)
      : _func(std::forward<Fn>(func))
      , _result(std::nullopt)
      , _rate(rate)
      , _last_call(Clock::now()) {}

    Ret operator()() const noexcept(Noexcept) {
        auto now = Clock::now();
        if (!is_ready(now)) {
            // Recompute if there is no cached value or the cached value
            // has expired.
            _last_call = now;
            _result = _func();
        }
        return *_result;
    }

private:
    // Returns true if the cached value is ready to be used.
    bool is_ready(Clock::time_point now = Clock::now()) const {
        return _result.has_value() && now <= (_last_call + _rate);
    }

    std::function<Ret() noexcept(Noexcept)> _func;
    mutable std::optional<Ret> _result;
    Clock::duration _rate;
    mutable Clock::time_point _last_call;
};

} // namespace ssx
