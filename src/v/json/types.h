// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "json/_include_first.h"

#include <fmt/format.h>
#include <rapidjson/error/error.h>
#include <rapidjson/pointer.h>
#include <rapidjson/rapidjson.h>

namespace json {

using SizeType = rapidjson::SizeType;
using Type = rapidjson::Type;

} // namespace json

template<>
struct fmt::formatter<rapidjson::Type> : fmt::formatter<int> {
    auto format(rapidjson::Type t, fmt::format_context& ctx) const {
        return fmt::formatter<int>::format(static_cast<int>(t), ctx);
    }
};

template<>
struct fmt::formatter<rapidjson::ParseErrorCode> : fmt::formatter<int> {
    auto format(rapidjson::ParseErrorCode e, fmt::format_context& ctx) const {
        return fmt::formatter<int>::format(static_cast<int>(e), ctx);
    }
};

template<>
struct fmt::formatter<rapidjson::PointerParseErrorCode> : fmt::formatter<int> {
    auto
    format(rapidjson::PointerParseErrorCode e, fmt::format_context& ctx) const {
        return fmt::formatter<int>::format(static_cast<int>(e), ctx);
    }
};
