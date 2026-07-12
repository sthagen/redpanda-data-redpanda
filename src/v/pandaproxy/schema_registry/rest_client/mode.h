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
#include "strings/string_switch.h"

#include <seastar/core/sstring.hh>

#include <string_view>

namespace pandaproxy::schema_registry::rest_client {

/// The registry-wide (global / default-context) operating mode reported by
/// `GET /mode`, modeled as an open enum. The Schema Registry REST API defines
/// the values below; because a newer (or third-party Confluent-compatible)
/// server may report another, an unrecognized wire value maps to
/// \ref registry_mode::unknown rather than being rejected, and the verbatim
/// string is preserved alongside it in \ref mode_info::raw. Note this is a
/// superset of Redpanda's own three-valued \ref schema_registry::mode:
/// READONLY_OVERRIDE and FORWARD are values a client must recognize on the wire
/// even though this node's server never emits them.
enum class registry_mode {
    /// READWRITE — normal operation; reads and writes (registration) allowed.
    /// The built-in default a registry reports when no global mode was set.
    read_write,
    /// READONLY — writes rejected, reads allowed.
    read_only,
    /// READONLY_OVERRIDE — a read-only variant. Deprecated in favor of READONLY
    /// on the global context, but still recognized on the wire.
    read_only_override,
    /// IMPORT — bulk-import mode (schemas registered with explicit IDs and
    /// versions).
    import,
    /// FORWARD — forwarding mode; only ever valid at the global level.
    forward,
    /// A value this client does not recognize (e.g. one a newer server
    /// introduced). The original string is retained in \ref mode_info::raw.
    unknown,
};

constexpr std::string_view to_string_view(registry_mode m) {
    switch (m) {
    case registry_mode::read_write:
        return "READWRITE";
    case registry_mode::read_only:
        return "READONLY";
    case registry_mode::read_only_override:
        return "READONLY_OVERRIDE";
    case registry_mode::import:
        return "IMPORT";
    case registry_mode::forward:
        return "FORWARD";
    case registry_mode::unknown:
        return "{unknown}";
    }
    return "{invalid}";
}

inline fmt::iterator format_to(registry_mode m, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(m));
}

/// Map a Schema Registry `mode` wire string to a \ref registry_mode. An
/// unrecognized (including empty) string yields registry_mode::unknown — the
/// open-enum contract — so the caller keeps the original value in
/// \ref mode_info::raw rather than losing it. READONLY_OVERRIDE is deprecated
/// but still mapped.
constexpr registry_mode registry_mode_from_wire(std::string_view sv) {
    return string_switch<registry_mode>(sv)
      .match(
        to_string_view(registry_mode::read_write), registry_mode::read_write)
      .match(to_string_view(registry_mode::read_only), registry_mode::read_only)
      .match(
        to_string_view(registry_mode::read_only_override),
        registry_mode::read_only_override)
      .match(to_string_view(registry_mode::import), registry_mode::import)
      .match(to_string_view(registry_mode::forward), registry_mode::forward)
      .default_match(registry_mode::unknown);
}

/// The result of `GET /mode`: the registry's global operating mode. The
/// verbatim wire string is retained in \ref raw so a value mapped to
/// registry_mode::unknown (e.g. a mode a newer server introduced) is not lost;
/// \ref raw carries the same string for recognized values too.
struct mode_info {
    registry_mode mode;
    ss::sstring raw;
};

} // namespace pandaproxy::schema_registry::rest_client
