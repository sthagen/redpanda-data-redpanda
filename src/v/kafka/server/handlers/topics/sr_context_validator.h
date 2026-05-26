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
#include "features/feature_table.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/handlers/topics/types.h"

#include <fmt/format.h>

#include <optional>
#include <string_view>

namespace kafka {

/// Returns an error message if the SR context string is invalid, nullopt if
/// valid. A valid context must start with '.', must not contain ':', and must
/// not be the reserved '.__GLOBAL' context.
inline std::optional<ss::sstring> validate_sr_context(std::string_view v) {
    if (
      !v.starts_with('.') || v.find(':') != std::string_view::npos
      || v == ".__GLOBAL") {
        return fmt::format(
          "redpanda.schema.registry.context `{}' is invalid: must start "
          "with '.', must not contain ':', and must not be the reserved "
          "'.__GLOBAL' context",
          v);
    }
    return std::nullopt;
}

struct schema_registry_context_create_validator {
    static constexpr const char* error_message
      = "Invalid redpanda.schema.registry.context: must start with '.', must "
        "not contain ':', and must not be the reserved '.__GLOBAL' context.";

    static constexpr error_code ec = error_code::invalid_config;

    static bool is_valid(const creatable_topic& c, features::feature_table*) {
        auto it = std::ranges::find(
          c.configs,
          topic_property_schema_registry_context,
          &createable_topic_config::name);
        if (it == c.configs.end() || !it->value.has_value()) {
            return true;
        }
        const auto& v = it->value.value();
        if (v.empty()) {
            return true;
        }
        return !validate_sr_context(v).has_value();
    }
};

} // namespace kafka
