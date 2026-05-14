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
/// The overloads delegate to fmt's range formatter, so the element type
/// must be fmt-formattable (has a fmt::formatter specialization or a
/// format_to method/free function).  Include this only in test
/// translation units to avoid pulling a std-namespace overload into
/// production code.

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

#include <ostream>
#include <unordered_map>
#include <vector>

namespace std {

template<typename T, typename Alloc>
// NOLINTNEXTLINE(cert-dcl58-cpp): test-only operator<< overload for std types
ostream& operator<<(ostream& os, const vector<T, Alloc>& v) {
    fmt::print(os, "{}", v);
    return os;
}

template<typename K, typename V, typename Hash, typename Eq, typename Alloc>
// NOLINTNEXTLINE(cert-dcl58-cpp): test-only operator<< overload for std types
ostream&
operator<<(ostream& os, const unordered_map<K, V, Hash, Eq, Alloc>& m) {
    fmt::print(os, "{}", m);
    return os;
}

} // namespace std
