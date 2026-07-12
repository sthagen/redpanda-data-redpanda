/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/admission_control_types.h"

#include "strings/string_switch.h"

#include <charconv>

namespace cloud_io {

std::optional<group_target>
try_parse_target_spec(std::string_view spec) noexcept {
    const auto colon = spec.find(':');
    if (colon == std::string_view::npos || colon == 0) {
        return std::nullopt;
    }
    const auto name = spec.substr(0, colon);
    const auto value_str = spec.substr(colon + 1);
    if (value_str.empty()) {
        return std::nullopt;
    }
    uint32_t value = 0;
    const auto [end, ec] = std::from_chars(
      value_str.data(), value_str.data() + value_str.size(), value);
    if (ec != std::errc{} || end != value_str.data() + value_str.size()) {
        return std::nullopt;
    }
    const auto g
      = string_switch<std::optional<group_id>>(name)
          .match(
            to_string_view(group_id::producer_upload),
            group_id::producer_upload)
          .match(
            to_string_view(group_id::consumer_fetch), group_id::consumer_fetch)
          .match(to_string_view(group_id::metastore), group_id::metastore)
          .match(
            to_string_view(group_id::default_group), group_id::default_group)
          .default_match(std::nullopt);
    if (!g.has_value()) {
        return std::nullopt;
    }
    return group_target{*g, value};
}

} // namespace cloud_io
