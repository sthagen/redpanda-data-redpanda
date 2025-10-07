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

#include "redpanda/admin/aip_common_config.h"
#include "serde/protobuf/base.h"

#include <seastar/core/sstring.hh>

#include <string_view>
#include <vector>

namespace admin {

struct aip_ordering_config {
    // Maps a field path to field numbers. Returns nullopt if path is unknown.
    field_path_converter_t field_path_converter;

    // A getter for converting field numbers -> a value_t
    field_type_getter_t field_type_getter;

    // The ordering expression to parse (e.g. "foo, bar desc")
    ss::sstring ordering_expr;
};

template<serde::pb::Message MsgType>
auto make_ordering_config(std::string_view ordering_expr) {
    return admin::aip_ordering_config{
      .field_path_converter = make_field_path_converter<MsgType>(),
      .field_type_getter = make_field_type_getter<MsgType>(),
      .ordering_expr = ss::sstring{ordering_expr},
    };
}

/// sort_order parses AIP-132 ordering strings into comparators for sorting
/// protobuf messages. Supports full AIP-132: comma-separated fields, "desc" for
/// descending, nested fields, well-known type ordering. Whitespace is ignored.
/// Example: "foo, bar desc, nested.field"
/// Ref: https://google.aip.dev/132#ordering
class sort_order {
public:
    struct component {
        enum class order : uint8_t { ascending, descending };
        std::vector<int32_t> field_numbers;
        order ord;
    };

    static sort_order parse(const aip_ordering_config&);

    // Returns true if a < b, allows passing sort_order to std::sort()
    bool
    operator()(serde::pb::base_message& a, serde::pb::base_message& b) const;

private:
    // Only constructible via parse()
    explicit sort_order(std::vector<component> components);

    std::vector<component> _components;
};

} // namespace admin
