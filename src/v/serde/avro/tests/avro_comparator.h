/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/vassert.h"

#include <avro/GenericDatum.hh>
#include <avro/LogicalType.hh>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <expected>
#include <string>
#include <string_view>
#include <unordered_map>

namespace serde::avro::testing {

struct compare_options {
    /// How extra record fields in `actual` are handled.
    ///
    /// reject         — records must have identical field counts.
    /// allow_null     — `actual` may have additional trailing fields
    ///                  (schema evolution) provided each is null.
    enum class extra_fields_policy : uint8_t { reject, allow_null };

    /// How map entries are matched.
    ///
    /// positional     — entries are compared index-by-index; duplicate keys
    ///                  are permitted (the original Avro map semantics).
    /// by_key_unique  — entries are matched by key regardless of order;
    ///                  duplicate keys in either side cause a test failure.
    enum class map_matching_policy : uint8_t { positional, by_key_unique };

    extra_fields_policy extra_fields = extra_fields_policy::reject;
    map_matching_policy map_matching = map_matching_policy::by_key_unique;
};

namespace detail {

/// Build a key->index map for an Avro map's entries, returning an error
/// if duplicate keys are found.
inline std::
  expected<std::unordered_map<std::string, size_t>, ::testing::AssertionResult>
  build_map_index(
    const std::vector<std::pair<std::string, ::avro::GenericDatum>>& map,
    std::string_view path,
    std::string_view side) {
    std::unordered_map<std::string, size_t> index;
    index.reserve(map.size());
    for (size_t i = 0; i < map.size(); ++i) {
        auto [_, inserted] = index.emplace(map[i].first, i);
        if (!inserted) {
            return std::unexpected(
              ::testing::AssertionFailure()
              << path << ": duplicate key in " << side << " map: '"
              << map[i].first << "'");
        }
    }
    return index;
}

inline std::string_view logical_type_name(::avro::LogicalType::Type type) {
    switch (type) {
    case ::avro::LogicalType::NONE:
        return "none";
    case ::avro::LogicalType::DECIMAL:
        return "decimal";
    case ::avro::LogicalType::DATE:
        return "date";
    case ::avro::LogicalType::TIME_MILLIS:
        return "time-millis";
    case ::avro::LogicalType::TIME_MICROS:
        return "time-micros";
    case ::avro::LogicalType::TIMESTAMP_MILLIS:
        return "timestamp-millis";
    case ::avro::LogicalType::TIMESTAMP_MICROS:
        return "timestamp-micros";
    case ::avro::LogicalType::DURATION:
        return "duration";
    case ::avro::LogicalType::UUID:
        return "uuid";
    case ::avro::LogicalType::MAP:
        return "map";
    }
    vunreachable("unexpected logical type");
}

// NB: float/double use operator== which means NaN != NaN and -0.0 == +0.0.
// This is fine for roundtrip tests where both sides come from the same Avro
// encoding, but won't detect NaN-handling bugs.
template<typename T>
inline ::testing::AssertionResult check_primitive(
  const ::avro::GenericDatum& expected,
  const ::avro::GenericDatum& actual,
  std::string_view path) {
    const auto& expected_v = expected.value<T>();
    const auto& actual_v = actual.value<T>();
    if (expected_v == actual_v) {
        return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure()
           << path << ": " << expected.type()
           << " mismatch (expected: " << ::testing::PrintToString(expected_v)
           << ", actual: " << ::testing::PrintToString(actual_v) << ")";
}

inline ::testing::AssertionResult compare_impl(
  const ::avro::GenericDatum& expected,
  const ::avro::GenericDatum& actual,
  std::string_view path,
  compare_options opts);

using map_kvs = std::vector<std::pair<std::string, ::avro::GenericDatum>>;

inline ::testing::AssertionResult compare_map_positional(
  const map_kvs& expected_kvs,
  const map_kvs& actual_kvs,
  std::string_view path,
  compare_options opts) {
    for (size_t i = 0; i < expected_kvs.size(); ++i) {
        if (expected_kvs[i].first != actual_kvs[i].first) {
            return ::testing::AssertionFailure()
                   << path << ": map key mismatch at index " << i
                   << " (expected '" << expected_kvs[i].first << "', actual '"
                   << actual_kvs[i].first << "')";
        }
        auto key_path = fmt::format("{}[\"{}\"]", path, expected_kvs[i].first);
        auto cmp = compare_impl(
          expected_kvs[i].second, actual_kvs[i].second, key_path, opts);
        if (!cmp) {
            return cmp;
        }
    }
    return ::testing::AssertionSuccess();
}

inline ::testing::AssertionResult compare_map_by_key(
  const map_kvs& expected_kvs,
  const map_kvs& actual_kvs,
  std::string_view path,
  compare_options opts) {
    auto expected_index = build_map_index(expected_kvs, path, "expected");
    if (!expected_index) {
        return expected_index.error();
    }
    auto actual_index = build_map_index(actual_kvs, path, "actual");
    if (!actual_index) {
        return actual_index.error();
    }
    for (const auto& [key, expected_value] : expected_kvs) {
        auto pos = actual_index->find(key);
        if (pos == actual_index->end()) {
            return ::testing::AssertionFailure()
                   << path << ": key missing from actual map: '" << key << "'";
        }
        auto key_path = fmt::format("{}[\"{}\"]", path, key);
        const auto& actual_value = actual_kvs[pos->second].second;
        auto cmp = compare_impl(expected_value, actual_value, key_path, opts);
        if (!cmp) {
            return cmp;
        }
    }
    return ::testing::AssertionSuccess();
}

inline ::testing::AssertionResult compare_impl(
  const ::avro::GenericDatum& expected,
  const ::avro::GenericDatum& actual,
  std::string_view path,
  compare_options opts) {
    if (expected.isUnion() != actual.isUnion()) {
        return ::testing::AssertionFailure() << path << ": union mismatch";
    }
    if (expected.isUnion() && expected.unionBranch() != actual.unionBranch()) {
        return ::testing::AssertionFailure()
               << path << ": union branch mismatch (expected: branch="
               << expected.unionBranch() << ", physical=" << expected.type()
               << ", logical="
               << logical_type_name(expected.logicalType().type())
               << "; actual: branch=" << actual.unionBranch()
               << ", physical=" << actual.type()
               << ", logical=" << logical_type_name(actual.logicalType().type())
               << ")";
    }
    if (expected.type() != actual.type()) {
        return ::testing::AssertionFailure()
               << path << ": type mismatch (expected " << expected.type()
               << ", actual " << actual.type() << ")";
    }
    if (expected.logicalType().type() != actual.logicalType().type()) {
        return ::testing::AssertionFailure()
               << path << ": logical type mismatch (expected "
               << logical_type_name(expected.logicalType().type())
               << ", actual " << logical_type_name(actual.logicalType().type())
               << ")";
    }

    switch (expected.type()) {
    case ::avro::AVRO_NULL:
        // Null carries no value; type equality is already verified above.
        return ::testing::AssertionSuccess();
    case ::avro::AVRO_BOOL:
        return check_primitive<bool>(expected, actual, path);
    case ::avro::AVRO_INT:
        return check_primitive<int32_t>(expected, actual, path);
    case ::avro::AVRO_LONG:
        return check_primitive<int64_t>(expected, actual, path);
    case ::avro::AVRO_FLOAT:
        return check_primitive<float>(expected, actual, path);
    case ::avro::AVRO_DOUBLE:
        return check_primitive<double>(expected, actual, path);
    case ::avro::AVRO_STRING:
        return check_primitive<std::string>(expected, actual, path);
    case ::avro::AVRO_BYTES:
        return check_primitive<std::vector<uint8_t>>(expected, actual, path);
    case ::avro::AVRO_FIXED:
        if (
          expected.value<::avro::GenericFixed>().value()
          == actual.value<::avro::GenericFixed>().value()) {
            return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure() << path << ": fixed mismatch";
    case ::avro::AVRO_ENUM: {
        const auto& expected_enum = expected.value<::avro::GenericEnum>();
        const auto& actual_enum = actual.value<::avro::GenericEnum>();
        // Compare both ordinal and symbol: ordinal catches schema drift where
        // two names resolve to different positions, symbol catches renames.
        if (
          expected_enum.value() == actual_enum.value()
          && expected_enum.symbol() == actual_enum.symbol()) {
            return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure()
               << path
               << ": enum mismatch (expected: " << expected_enum.symbol() << "/"
               << expected_enum.value() << ", actual: " << actual_enum.symbol()
               << "/" << actual_enum.value() << ")";
    }
    case ::avro::AVRO_RECORD: {
        const auto& expected_record = expected.value<::avro::GenericRecord>();
        const auto& actual_record = actual.value<::avro::GenericRecord>();
        auto expected_count = expected_record.fieldCount();
        auto actual_count = actual_record.fieldCount();
        if (actual_count < expected_count) {
            return ::testing::AssertionFailure()
                   << path << ": record field count mismatch (expected "
                   << expected_count << ", actual " << actual_count << ")";
        }
        if (
          actual_count != expected_count
          && opts.extra_fields
               == compare_options::extra_fields_policy::reject) {
            return ::testing::AssertionFailure()
                   << path << ": record field count mismatch (expected "
                   << expected_count << ", actual " << actual_count << ")";
        }
        for (size_t i = 0; i < expected_count; ++i) {
            auto p = fmt::format(
              "{}.{}", path, expected_record.schema()->nameAt(i));
            auto res = compare_impl(
              expected_record.fieldAt(i), actual_record.fieldAt(i), p, opts);
            if (!res) {
                return res;
            }
        }
        for (size_t i = expected_count; i < actual_count; ++i) {
            const auto& field = actual_record.fieldAt(i);
            if (!field.isUnion() || field.type() != ::avro::AVRO_NULL) {
                return ::testing::AssertionFailure()
                       << path << "." << actual_record.schema()->nameAt(i)
                       << ": extra field is not null";
            }
        }
        return ::testing::AssertionSuccess();
    }
    case ::avro::AVRO_ARRAY: {
        const auto& expected_array
          = expected.value<::avro::GenericArray>().value();
        const auto& actual_array = actual.value<::avro::GenericArray>().value();
        if (expected_array.size() != actual_array.size()) {
            return ::testing::AssertionFailure()
                   << path << ": array size mismatch (expected "
                   << expected_array.size() << ", actual "
                   << actual_array.size() << ")";
        }
        for (size_t i = 0; i < expected_array.size(); ++i) {
            auto p = fmt::format("{}[{}]", path, i);
            auto res = compare_impl(
              expected_array[i], actual_array[i], p, opts);
            if (!res) {
                return res;
            }
        }
        return ::testing::AssertionSuccess();
    }
    case ::avro::AVRO_MAP: {
        const auto& expected_kvs = expected.value<::avro::GenericMap>().value();
        const auto& actual_kvs = actual.value<::avro::GenericMap>().value();
        if (expected_kvs.size() != actual_kvs.size()) {
            return ::testing::AssertionFailure()
                   << path << ": map size mismatch (expected "
                   << expected_kvs.size() << ", actual " << actual_kvs.size()
                   << ")";
        }
        if (
          opts.map_matching
          == compare_options::map_matching_policy::positional) {
            return compare_map_positional(expected_kvs, actual_kvs, path, opts);
        }
        return compare_map_by_key(expected_kvs, actual_kvs, path, opts);
    }
    case ::avro::AVRO_UNION:
        // GenericDatum::type() unwraps the selected union branch, so the
        // switch should never reach here. Union handling is done above via
        // isUnion()/unionBranch() before the switch.
        vunreachable("unexpected AVRO_UNION in type() dispatch at {}", path);
    case ::avro::AVRO_SYMBOLIC:
        return ::testing::AssertionFailure() << path << ": symbolic type";
    case ::avro::AVRO_UNKNOWN:
        return ::testing::AssertionFailure() << path << ": unknown type";
    }
    return ::testing::AssertionFailure() << path << ": unreachable";
}

} // namespace detail

/// Deep-compare two Avro GenericDatum trees, returning a gtest
/// AssertionResult with a dotted path to the first mismatch.
///
/// \param expected  The reference datum tree.
/// \param actual    The datum tree under test.
/// \param path      Dotted path prefix used in mismatch messages.
/// \param opts      Comparison options.
inline ::testing::AssertionResult generic_datum_eq(
  const ::avro::GenericDatum& expected,
  const ::avro::GenericDatum& actual,
  std::string_view path = "root",
  compare_options opts = {}) {
    return detail::compare_impl(expected, actual, path, opts);
}

} // namespace serde::avro::testing
