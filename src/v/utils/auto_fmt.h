// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once
#include "base/format_to.h"
#include "reflection/arity.h"

/// Base class for small value-types that adds automatic formatting of all
/// fields. The code:
///
/// \code
/// template<class F, class S>
/// struct pair : auto_fmt<pair, ','> {
///    F first;
///    S second;
/// };
/// pair<int, int> value{ .first = 42, .second = 137 };
/// fmt::print("pair = ({})", value);
/// \endcode
///
/// Will print: "pair = (42,137)"
template<class Derived, char delimiter = ' '>
struct auto_fmt {
    fmt::iterator format_to(fmt::iterator it) const {
        constexpr auto a = reflection::arity<Derived>() - 1;
        static_assert(a <= 16, "Too many fields");
        auto format_fields = [&](const auto&... fields) {
            bool first = true;
            auto write = [&](const auto& f) {
                if (!first) {
                    *it++ = delimiter;
                }
                first = false;
                if constexpr (
                  fmt::is_formattable<
                    std::remove_cvref_t<decltype(f)>>::value) {
                    it = fmt::format_to(it, "{}", f);
                } else {
                    it = fmt::format_to(it, "{}", fmt::streamed(f));
                }
            };
            (write(fields), ...);
        };
        // clang-format off
        if constexpr (a == 0) {
            return it;
        } else if constexpr (a == 1) {
            auto& [f0] = static_cast<const Derived&>(*this);
            format_fields(f0);
        } else if constexpr (a == 2) {
            auto& [f0, f1] = static_cast<const Derived&>(*this);
            format_fields(f0, f1);
        } else if constexpr (a == 3) {
            auto& [f0, f1, f2] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2);
        } else if constexpr (a == 4) {
            auto& [f0, f1, f2, f3] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3);
        } else if constexpr (a == 5) {
            auto& [f0, f1, f2, f3, f4] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4);
        } else if constexpr (a == 6) {
            auto& [f0, f1, f2, f3, f4, f5] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4, f5);
        } else if constexpr (a == 7) {
            auto& [f0, f1, f2, f3, f4, f5, f6] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4, f5, f6);
        } else if constexpr (a == 8) {
            auto& [f0, f1, f2, f3, f4, f5, f6, f7] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4, f5, f6, f7);
        } else if constexpr (a == 9) {
            auto& [f0, f1, f2, f3, f4, f5, f6, f7, f8] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4, f5, f6, f7, f8);
        } else if constexpr (a == 10) {
            auto& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9);
        } else if constexpr (a == 11) {
            auto& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10);
        } else if constexpr (a == 12) {
            auto& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11);
        } else if constexpr (a == 13) {
            auto& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12);
        } else if constexpr (a == 14) {
            auto& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13);
        } else if constexpr (a == 15) {
            auto& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14);
        } else if constexpr (a == 16) {
            auto& [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15] = static_cast<const Derived&>(*this);
            format_fields(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15);
        }
        // clang-format on
        return it;
    }
};
