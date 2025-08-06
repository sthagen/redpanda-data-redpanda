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

namespace fmt {

using iterator = format_context::iterator;

template<typename T>
concept HasFormatToMethod = requires(const T& obj, iterator out) {
    { obj.format_to(out) } -> std::same_as<iterator>;
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
 *     return fmt::print(it, "{{a: {}, b: {}}}", a, b)
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

} // namespace fmt

// For both googletest and for other external libraries that may use
// `operator<<` to print stuff, give a blanket implementation that delegates to
// the `format_to` method.
template<fmt::HasFormatToMethod T>
std::ostream& operator<<(std::ostream& os, const T& obj) {
    return fmt::format_to(os, "{}", obj);
}
