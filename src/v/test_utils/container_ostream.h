// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

/// Container ostream operators for test frameworks.
///
/// Boost.Test and googletest macros (BOOST_REQUIRE_EQUAL, ASSERT_TRUE() << x,
/// BOOST_TEST_MESSAGE, etc.) use operator<< to print values.  Standard
/// containers like std::vector and std::unordered_map have fmt formatters
/// (via fmt::formatter range support) but no operator<<, which makes such
/// streaming fail to compile.
///
/// Seastar historically provided overloads for exactly std::vector and
/// std::unordered_map behind the SEASTAR_DEPRECATED_OSTREAM_FORMATTERS
/// define -- this header is the local replacement for tests that still
/// rely on streaming containers, which is why the set is limited to those
/// two types rather than every standard container.
///
/// The overloads stream each element via operator<< so they work for any
/// element type that has streaming, regardless of whether it has a fmt
/// formatter.  Include this only in test translation units to avoid
/// pulling a std-namespace overload into production code.

#include <ostream>
#include <unordered_map>
#include <vector>

namespace std {

template<typename T, typename Alloc>
// NOLINTNEXTLINE(cert-dcl58-cpp): test-only operator<< overload for std types
ostream& operator<<(ostream& os, const vector<T, Alloc>& v) {
    os << "{";
    bool first = true;
    for (const auto& e : v) {
        if (!first) {
            os << ", ";
        }
        first = false;
        os << e;
    }
    return os << "}";
}

template<typename K, typename V, typename Hash, typename Eq, typename Alloc>
// NOLINTNEXTLINE(cert-dcl58-cpp): test-only operator<< overload for std types
ostream&
operator<<(ostream& os, const unordered_map<K, V, Hash, Eq, Alloc>& m) {
    os << "{";
    bool first = true;
    for (const auto& [k, v] : m) {
        if (!first) {
            os << ", ";
        }
        first = false;
        os << "{" << k << " -> " << v << "}";
    }
    return os << "}";
}

} // namespace std
