/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/truncate_transform_visitor.h"

#include "absl/numeric/int128.h"
#include "bytes/iobuf.h"
#include "iceberg/values.h"

#include <cstdint>
#include <limits>

namespace iceberg {

namespace {
template<typename RemType, typename Val, typename Divider>
Val round_towards_minus_infinity(Val val, Divider divisor) {
    if (divisor == 0) [[unlikely]] {
        return val;
    }
    // use larger type to avoid overflow
    Val remainder = ((RemType{val} % divisor) + divisor) % divisor;
    if (val < std::numeric_limits<Val>::min() + remainder) [[unlikely]] {
        // simulate java underflow
        return std::numeric_limits<Val>::max() - remainder + val + 1
               - std::numeric_limits<Val>::min();
    }
    return val - remainder;
}
} // namespace

value truncate_transform_visitor::operator()(const int_value& value) {
    if (length > static_cast<uint32_t>(std::numeric_limits<int32_t>::max()))
      [[unlikely]] {
        return int_value{0};
    }
    int32_t length_capped = length;
    return int_value{
      round_towards_minus_infinity<int64_t>(value.val, length_capped)};
}

value truncate_transform_visitor::operator()(const long_value& value) {
    return long_value{round_towards_minus_infinity<int64_t>(value.val, length)};
}

value truncate_transform_visitor::operator()(const decimal_value& value) {
    return decimal_value{
      round_towards_minus_infinity<absl::int128>(value.val, length)};
}

value truncate_transform_visitor::operator()(const string_value& v) {
    value ret{
      std::in_place_type<primitive_value>, std::in_place_type<string_value>};
    auto& ret_iobuf
      = std::get<string_value>(std::get<primitive_value>(ret)).val;

    auto remaining_allowance = length;

    // assume valid UTF-8
    // only tell apart between first and subsequent bytes in a codepoint
    auto is_utf8_codepoint_initial_byte = [](const unsigned char c) {
        return (c & 0b11000000) != 0b10000000;
    };

    for (const auto& frag : v.val) {
        const auto frag_begin = frag.get();
        const auto frag_len = frag.size();
        const auto frag_end = frag_begin + frag_len;
        for (auto cur = frag_begin; cur != frag_end; ++cur) {
            if (is_utf8_codepoint_initial_byte(*cur)) {
                if (remaining_allowance == 0) {
                    // no allowance left for the new code point -> finalize
                    size_t len_to_copy = cur - frag_begin;
                    ret_iobuf.append(frag_begin, len_to_copy);
                    return ret;
                }
                --remaining_allowance;
            }
        }
        ret_iobuf.append(frag_begin, frag_len);
    }
    return ret;
}

value truncate_transform_visitor::operator()(const binary_value& v) {
    auto desired_length = std::min(size_t{length}, v.val.size_bytes());
    iobuf::iterator_consumer ic{v.val.cbegin(), v.val.cend()};
    return binary_value{iobuf_copy(ic, desired_length)};
}

} // namespace iceberg
