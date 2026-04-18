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

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/std.h>

namespace fmt {

/// Formatter for std::unique_ptr<T> — formats the pointee or "null".
template<typename T, typename D>
requires is_formattable<T>::value
struct formatter<std::unique_ptr<T, D>> {
    constexpr auto parse(format_parse_context& ctx) const {
        return ctx.begin();
    }
    template<typename Ctx>
    auto format(const std::unique_ptr<T, D>& p, Ctx& ctx) const {
        if (p) {
            return fmt::format_to(ctx.out(), "{}", *p);
        }
        return fmt::format_to(ctx.out(), "null");
    }
};

using iterator = format_context::iterator;

template<typename T>
concept HasFormatToMethod = requires(const T& obj, iterator out) {
    { obj.format_to(out) } -> std::same_as<iterator>;
};

/// Concept for types with a free function `format_to(const T&, iterator)`.
/// Used for enums and other types that cannot have member functions.
/// The free function must be ADL-findable (i.e. in the same namespace as T).
template<typename T>
concept HasFormatToFreeFunction = !HasFormatToMethod<T>
                                  && requires(const T& obj, iterator out) {
                                         {
                                             format_to(obj, out)
                                         } -> std::same_as<iterator>;
                                     };

/**
 * A formatter that supports any type that implements a `format_to` method.
 *
 * For example:
 *
 * ```
 * struct foo {
 *   std::string a;
 *   int32_t b;
 *
 *   fmt::iterator format_to(fmt::iterator it) const {
 *     return fmt::format_to(it, "{{a: {}, b: {}}}", a, b)
 *   }
 * };
 * ```
 */
template<HasFormatToMethod T>
struct formatter<T> {
    /**
     * When using `format_to`, there is no support for custom modifiers like
     * `{:d}` or `{:.2f}`. So only accept `{}`
     */
    constexpr fmt::format_parse_context::iterator
    parse(fmt::format_parse_context& ctx) const {
        auto it = ctx.begin();
        if (it != ctx.end() && *it != '}') {
            throw fmt::format_error("invalid format specifier for this type");
        }
        return it;
    }

    /**
     * Formats the object using its own `format_to` method.
     *
     * This function is called by the {fmt} library to perform the actual
     * formatting. It delegates the work entirely to the object's `format_to`
     * method.
     */
    iterator format(const T& obj, format_context& ctx) const {
        return obj.format_to(ctx.out());
    }
};

/**
 * A formatter for types that provide a free function
 * `format_to(const T&, fmt::iterator) -> fmt::iterator`, found via ADL.
 *
 * This is the counterpart of HasFormatToMethod for types that cannot have
 * member functions (e.g. enums).
 */
template<HasFormatToFreeFunction T>
struct formatter<T> {
    constexpr fmt::format_parse_context::iterator
    parse(fmt::format_parse_context& ctx) const {
        auto it = ctx.begin();
        if (it != ctx.end() && *it != '}') {
            throw fmt::format_error("invalid format specifier for this type");
        }
        return it;
    }

    iterator format(const T& obj, format_context& ctx) const {
        return format_to(obj, ctx.out());
    }
};

} // namespace fmt

/// Checked wrapper around fmt::streamed().
///
/// fmt::streamed(x) formats x via operator<<.  But types that satisfy
/// HasFormatToMethod / HasFormatToFreeFunction have a blanket operator<<
/// (below) that delegates back to fmt, creating an infinite recursion:
///
///   format_to() -> streamed(x) -> operator<< -> fmt::print -> format_to()
///
/// If a type is formattable through format_to, just use fmt::format("{}", x)
/// directly — there is no need for streamed().  This wrapper enforces that
/// invariant at compile time.
template<typename T>
auto fmt_streamed(const T& val) {
    using raw = std::remove_cvref_t<T>;
    static_assert(
      !fmt::HasFormatToMethod<raw> && !fmt::HasFormatToFreeFunction<raw>,
      "Do not use fmt::streamed() / fmt_streamed() on types with format_to — "
      "use fmt::format(\"{}\", val) instead.  streamed() triggers operator<< "
      "which calls back into fmt, causing infinite recursion.");
    return fmt::streamed(val);
}

namespace std {
// For both googletest and for other external libraries that may use
// `operator<<` to print stuff, give a blanket implementation that delegates to
// the `format_to` method or free function.
//
// We have to put this in the std namespace for overload resolution rules to be
// able to find it for arbitrary T.
//
// WARNING: this creates a potential infinite-recursion footgun with
// fmt::streamed().  Never pass a type that has format_to through
// fmt::streamed() — use fmt_streamed() (above) which static_asserts against
// it, or just use fmt::format("{}", val) directly.
template<typename T>
requires fmt::HasFormatToMethod<T> || fmt::HasFormatToFreeFunction<T>
// NOLINTNEXTLINE(*-dcl58-*)
ostream& operator<<(ostream& os, const T& obj) {
    fmt::print(os, "{}", obj);
    return os;
}
} // namespace std
