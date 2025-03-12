/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include <algorithm>
#include <string_view>

/**
 * Compile time string owning string literal.
 * Inspired by
 * https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2016/p0259r0.pdf
 */

template<size_t N>
struct fixed_string;

// 0-size arrays are not allowed in c++, implement separately

template<>
struct fixed_string<0> {
    // NOLINTNEXTLINE(hicpp-explicit-conversions,hicpp-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
    constexpr fixed_string(const char (&)[1]) noexcept {}

    // NOLINTNEXTLINE(hicpp-explicit-conversions)
    constexpr operator std::string_view() const { return {}; }

private:
    constexpr fixed_string() = default;
};

template<size_t N>
struct fixed_string {
    static_assert(N != 0);

    // NOLINTNEXTLINE(hicpp-explicit-conversions,hicpp-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays,cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    constexpr fixed_string(const char (&str)[N + 1]) noexcept {
        std::copy_n(str, N, value);
    }

    // NOLINTNEXTLINE(hicpp-explicit-conversions)
    constexpr operator std::string_view() const { return {value, N}; }

    // NOLINTNEXTLINE(hicpp-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
    char value[N];

private:
    constexpr fixed_string() = default;

public:
    template<size_t A, size_t B>
    friend constexpr auto
    operator+(const fixed_string<A>& a, const fixed_string<B>& b) noexcept;
};

template<size_t A, size_t B>
constexpr auto
operator+(const fixed_string<A>& a, const fixed_string<B>& b) noexcept {
    fixed_string<A + B> ret;
    if constexpr (A != 0) {
        std::copy_n(a.value, A, ret.value);
    }
    if constexpr (B != 0) {
        std::copy_n(b.value, B, ret.value + A);
    }
    return ret;
}

/**
 * Helper for deducing N (size of the string based on the size of reference to
 * an array of chars)
 * N - 1 is used to remove the terminating argument
 */
template<size_t N>
// NOLINTNEXTLINE(hicpp-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
fixed_string(const char (&)[N]) -> fixed_string<N - 1>;
