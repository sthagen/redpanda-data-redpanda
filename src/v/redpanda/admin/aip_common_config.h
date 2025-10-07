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

#include "serde/protobuf/base.h"
#include "serde/protobuf/rpc.h"

#include <seastar/util/noncopyable_function.hh>

#include <fmt/ranges.h>

#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace admin {

using field_type_getter_t = ss::noncopyable_function<serde::pb::field::value_t(
  std::span<const int32_t> field_numbers)>;

using field_path_converter_t = ss::noncopyable_function<
  std::optional<std::vector<int32_t>>(std::span<std::string_view> field_path)>;

template<serde::pb::Message MsgType>
field_type_getter_t make_field_type_getter() {
    return [](std::span<const int32_t> field_nums) {
        auto default_obj = MsgType{};
        auto val = default_obj.lookup_field(field_nums);
        if (!val) {
            throw serde::pb::rpc::internal_exception(
              fmt::format(
                "Unknown field lookup for field numbers: {}", field_nums));
        }
        return std::move(val->value);
    };
}

template<serde::pb::Message MsgType>
field_path_converter_t make_field_path_converter() {
    return [](std::span<std::string_view> field_path)
             -> std::optional<std::vector<int32_t>> {
        auto res = std::vector<int32_t>{};
        if (!MsgType::convert_field_path_to_numbers(field_path, &res)) {
            return std::nullopt;
        }
        return res;
    };
}

} // namespace admin
