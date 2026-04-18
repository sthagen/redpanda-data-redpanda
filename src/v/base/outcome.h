/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <system_error> // bring in std::error_code et al

// use the standard ones instead
#include <boost/outcome/basic_result.hpp>
#include <boost/outcome/std_outcome.hpp>
#include <boost/outcome/std_result.hpp>

// include utils
#include <boost/outcome/iostream_support.hpp>
#include <boost/outcome/try.hpp>
#include <boost/outcome/utils.hpp>
#include <fmt/format.h>

namespace outcome = boost::outcome_v2;

template<class S>
using failure_type = outcome::failure_type<S>;

template<
  class R,
  class S = std::error_code,
  class NoValuePolicy = outcome::policy::default_policy<R, S, void>>
using result = outcome::basic_result<R, S, NoValuePolicy>;

template<class R, class S = std::error_code>
using unchecked = outcome::std_result<R, S, outcome::policy::all_narrow>;

template<class R, class S = std::error_code>
using checked
  = outcome::result<R, S, outcome::policy::throw_bad_result_access<S, void>>;

template<class T>
constexpr bool is_result_v = outcome::is_basic_result_v<T>;

template<typename T, typename E, typename P>
struct fmt::formatter<boost::outcome_v2::basic_result<T, E, P>> {
    constexpr auto parse(fmt::format_parse_context& ctx) const {
        return ctx.begin();
    }
    template<typename Ctx>
    auto
    format(const boost::outcome_v2::basic_result<T, E, P>& r, Ctx& ctx) const {
        if (r.has_value()) {
            return fmt::format_to(ctx.out(), "{}", r.value());
        }
        return fmt::format_to(ctx.out(), "{}", r.error());
    }
};
