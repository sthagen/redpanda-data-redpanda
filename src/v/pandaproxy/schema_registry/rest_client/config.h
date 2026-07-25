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

#include "base/format_to.h"
#include "base/seastarx.h"
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"
#include "strings/string_switch.h"

#include <seastar/core/sstring.hh>

#include <optional>
#include <string_view>

namespace pandaproxy::schema_registry::rest_client {

/// The registry-wide (global / default) compatibility level reported by
/// `GET /config`, modeled as an open enum. The Schema Registry REST API defines
/// the values below; because a newer (or third-party Confluent-compatible)
/// server may report another, an unrecognized wire value maps to
/// registry_compatibility_level::unknown rather than being rejected, and the
/// verbatim string is preserved alongside it in \ref config_info::raw. This is
/// the open-enum counterpart of Redpanda's closed
/// schema_registry::compatibility_level.
enum class registry_compatibility_level {
    /// NONE — no compatibility checking.
    none,
    /// BACKWARD — the built-in default a registry reports when no global
    /// compatibility is set (operators may configure a different server
    /// default).
    backward,
    /// BACKWARD_TRANSITIVE.
    backward_transitive,
    /// FORWARD.
    forward,
    /// FORWARD_TRANSITIVE.
    forward_transitive,
    /// FULL.
    full,
    /// FULL_TRANSITIVE.
    full_transitive,
    /// A value this client does not recognize (e.g. one a newer server
    /// introduced). The original string is retained in \ref config_info::raw.
    unknown,
};

constexpr std::string_view to_string_view(registry_compatibility_level c) {
    switch (c) {
    case registry_compatibility_level::none:
        return "NONE";
    case registry_compatibility_level::backward:
        return "BACKWARD";
    case registry_compatibility_level::backward_transitive:
        return "BACKWARD_TRANSITIVE";
    case registry_compatibility_level::forward:
        return "FORWARD";
    case registry_compatibility_level::forward_transitive:
        return "FORWARD_TRANSITIVE";
    case registry_compatibility_level::full:
        return "FULL";
    case registry_compatibility_level::full_transitive:
        return "FULL_TRANSITIVE";
    case registry_compatibility_level::unknown:
        return "{unknown}";
    }
    return "{invalid}";
}

inline fmt::iterator
format_to(registry_compatibility_level c, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(c));
}

/// Map a Schema Registry `compatibilityLevel` wire string to a
/// registry_compatibility_level. An unrecognized (including empty) string
/// yields registry_compatibility_level::unknown — the open-enum contract — so
/// the caller keeps the original value in \ref config_info::raw rather than
/// losing it.
constexpr registry_compatibility_level
registry_compatibility_level_from_wire(std::string_view sv) {
    using enum registry_compatibility_level;
    return string_switch<registry_compatibility_level>(sv)
      .match(to_string_view(none), none)
      .match(to_string_view(backward), backward)
      .match(to_string_view(backward_transitive), backward_transitive)
      .match(to_string_view(forward), forward)
      .match(to_string_view(forward_transitive), forward_transitive)
      .match(to_string_view(full), full)
      .match(to_string_view(full_transitive), full_transitive)
      .default_match(unknown);
}

/// The result of `GET /config`: the registry's global configuration.
///
/// Only `compatibilityLevel` is modeled, as an open enum in \ref level with
/// the verbatim wire string kept in \ref raw. `level` is optional: a subject
/// config may carry only governance fields (e.g. `compatibilityGroup`). Every
/// other non-null top-level field is recorded in \ref unsupported as a JSON
/// pointer + type (mirroring source_schema_read::unsupported) for the
/// caller's unsupported-feature policy; a null value is treated as absent.
/// Redpanda's own server emits only `compatibilityLevel`.
struct config_info {
    std::optional<registry_compatibility_level> level;
    ss::sstring raw;
    chunked_vector<unsupported_feature> unsupported;
};

} // namespace pandaproxy::schema_registry::rest_client
