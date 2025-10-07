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

#include <memory>

namespace admin {

struct ast_node {
    virtual ~ast_node() = default;
    virtual bool evaluate(serde::pb::base_message& obj) const = 0;
};

/// A filter predicate object that can be applied to protobuf objects
class filter_predicate {
public:
    explicit filter_predicate(std::unique_ptr<ast_node> root);
    bool operator()(serde::pb::base_message& obj) const;

private:
    std::unique_ptr<ast_node> _root;
};

struct aip_filter_config {
    // A getter for converting field numbers -> a value_t
    field_type_getter_t field_type_getter;

    // A converter from field path in filter expression to field numbers.
    // If std::nullopt is returned the filter specified a path that does
    // not exist and filter creation will throw an exception.
    field_path_converter_t field_path_converter;

    // The user specified filter expression.
    ss::sstring filter_expression;
};

template<serde::pb::Message MsgType>
aip_filter_config make_aip_filter_config(std::string_view filter_expression) {
    return aip_filter_config{
      .field_type_getter = make_field_type_getter<MsgType>(),
      .field_path_converter = make_field_path_converter<MsgType>(),
      .filter_expression = ss::sstring{filter_expression},
    };
}

/// aip_filter_parser parses AIP-160 compliant, human-readable filter expression
/// strings into filter_predicate's to allow filtering protobuf objects.
///
/// Supports a subset of the AIP-160 spec:
/// - Field comparisons (`=`, `!=`, `<`, `>`, `<=`, `>=`)
/// - Logical AND chaining: condition1 AND condition2
/// - Nested field access: parent.child = value
/// - Escape sequences: field = "string with \"quotes\""
/// - Enum types
/// - RFC3339 timestamps and ISO-like duration parsing via abseil
///
/// Ref: https://google.aip.dev/160
class aip_filter_parser {
public:
    constexpr static size_t max_filter_length = 1024;

    static filter_predicate create_aip_filter(aip_filter_config);
};

} // namespace admin
