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

#include "bytes/iobuf.h"

#include <fmt/format.h>

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <utility>

namespace experimental::serde::json {

struct parser_config {
    /// Default max depth is very liberal.
    static constexpr auto default_max_depth = 1000;

    /// Maximum depth of nested structures (arrays and objects).
    size_t max_depth = default_max_depth;
};

enum class token {
    error,
    value_null,
    value_true,
    value_false,
    value_double,
    value_int,
    value_string,
    start_object,
    key,
    end_object,
    start_array,
    end_array,
    eof,
};

inline constexpr std::string_view format_as(token t) {
    switch (t) {
    case token::error:
        return "error";
    case token::value_null:
        return "value_null";
    case token::value_true:
        return "value_true";
    case token::value_false:
        return "value_false";
    case token::value_double:
        return "value_double";
    case token::value_int:
        return "value_int";
    case token::value_string:
        return "value_string";
    case token::start_object:
        return "start_object";
    case token::key:
        return "key";
    case token::end_object:
        return "end_object";
    case token::start_array:
        return "start_array";
    case token::end_array:
        return "end_array";
    case token::eof:
        return "eof";
    }
    std::unreachable();
}

inline std::ostream& operator<<(std::ostream& os, token t) {
    return os << format_as(t);
}

class parser {
public:
    explicit parser(iobuf buf, parser_config config = {});
    ~parser();

    /// Advance the parser to the next token. Returns true if the parser
    /// successfully advanced to the next token. Returns false if the
    /// parser reached the end of the input or if an error occurred.
    ss::future<bool> next();

    /// Return the current token without advancing the parser.
    token token() const;

    /// Return the current value of the parser.
    /// Can be called only if a previous call to token() returned
    /// token::value_int. May be called at most once.
    int64_t value_int();

    /// Return the current value of the parser.
    /// Can be called only if a previous call to token() returned
    /// token::value_double. May be called at most once.
    double value_double();

    /// Return the current value of the parser.
    /// Can be called only if a previous call to token() returned
    /// token::value_string. May be called at most once.
    iobuf value_string();

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

}; // namespace experimental::serde::json
