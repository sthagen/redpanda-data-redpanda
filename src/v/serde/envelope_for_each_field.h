// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "serde/envelope.h"

#include <tuple>
#include <utility>

namespace serde {

template<typename T>
constexpr inline auto envelope_to_tuple(T&& t) {
    return t.serde_fields();
}

template<typename T>
constexpr inline auto envelope_to_tuple(const T& t) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    return std::apply(
      [](auto&... args) { return std::tie(std::as_const(args)...); },
      const_cast<T&>(t).serde_fields());
}

template<typename Fn>
concept check_for_more_fn = requires(Fn&& fn, int& f) {
    { fn(f) } -> std::convertible_to<bool>;
};

template<typename T, typename Fn>
requires is_envelope<std::decay_t<T>>
inline auto envelope_for_each_field(T&& t, Fn&& fn) {
    std::apply(
      [&](auto&&... args) { (fn(std::forward_like<T>(args)), ...); },
      envelope_to_tuple(t));
}

template<is_envelope T, check_for_more_fn Fn>
inline auto envelope_for_each_field(T& t, Fn&& fn) {
    std::apply(
      [&](auto&&... args) { (void)(fn(args) && ...); }, envelope_to_tuple(t));
}

} // namespace serde
