/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/format_to.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string_view>
#include <utility>

namespace cloud_io {

/// Identifies a cloud_io::admission_control admission policy.
enum class policy_type : uint8_t {
    /// No-op admission gate; the client pool's capacity is the only
    /// constraint.
    passthrough,
    /// Reservation-based admission policy. Each group has a private
    /// reservation sized to a configured target while it is active;
    /// idle reservations are reclaimed to a common pool and refilled
    /// into demanding groups.
    reservation,
};

constexpr std::string_view to_string_view(policy_type t) {
    switch (t) {
    case policy_type::passthrough:
        return "passthrough";
    case policy_type::reservation:
        return "reservation";
    }
    std::unreachable();
}

inline fmt::iterator format_to(policy_type t, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(t));
}

/// Caller-supplied intent label for a cloud_io operation. Used as the
/// admission key for cloud_io::admission_control.
enum class group_id : uint8_t {
    /// Object uploads on the Kafka produce path. Latency-critical.
    producer_upload,
    /// Reads serving Kafka fetch requests. Latency-critical.
    consumer_fetch,
    /// IO for the metastore. Latency-critical.
    metastore,
    /// Everything else: manifest I/O, archival writes, hydration,
    /// replication, housekeeping.
    default_group,
};

inline constexpr size_t num_group_ids = 4;

constexpr std::string_view to_string_view(group_id g) {
    switch (g) {
    case group_id::producer_upload:
        return "producer_upload";
    case group_id::consumer_fetch:
        return "consumer_fetch";
    case group_id::metastore:
        return "metastore";
    case group_id::default_group:
        return "default_group";
    }
    std::unreachable();
}

inline fmt::iterator format_to(group_id g, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(g));
}

namespace detail {
template<size_t... Is>
constexpr std::array<group_id, num_group_ids>
make_all_group_ids(std::index_sequence<Is...>) {
    return {static_cast<group_id>(Is)...};
}
} // namespace detail

/// All group_id values in enum order. Bounded by num_group_ids; when
/// adding a group_id enumerator, update num_group_ids to match — the
/// static_assert below catches the most common slip.
inline constexpr auto all_group_ids = detail::make_all_group_ids(
  std::make_index_sequence<num_group_ids>{});

static_assert(
  std::to_underlying(group_id::default_group) + 1 == num_group_ids,
  "num_group_ids must equal the number of group_id enumerators; "
  "update both when adding or removing a group.");

/// A parsed reservation target: a group and its target_reserved slot count.
struct group_target {
    group_id group;
    uint32_t slots;
    bool operator==(const group_target&) const = default;
};

/// Parse a "group_name:slots" target spec. Returns the (group, slots) target
/// when the spec is well-formed and the name matches a known group_id, else
/// nullopt.
std::optional<group_target>
try_parse_target_spec(std::string_view spec) noexcept;

/// Fixed-size array of T indexed by group_id.
template<typename T>
struct per_group {
    std::array<T, num_group_ids> data;

    T& operator[](group_id g) noexcept { return data[static_cast<size_t>(g)]; }
    const T& operator[](group_id g) const noexcept {
        return data[static_cast<size_t>(g)];
    }

    auto begin() noexcept { return data.begin(); }
    auto end() noexcept { return data.end(); }
    auto begin() const noexcept { return data.begin(); }
    auto end() const noexcept { return data.end(); }
};

/// Per-group target_reserved values for the reservation policy.
struct reservation_policy_config {
    per_group<uint32_t> target_reserved{};

    reservation_policy_config() = default;
    reservation_policy_config(
      std::initializer_list<std::pair<group_id, uint32_t>> entries) {
        for (const auto& [g, v] : entries) {
            target_reserved[g] = v;
        }
    }
};

/// Runtime configuration for cloud_io::admission_control. Populated at startup
/// from cluster config (see cloud_storage::configuration::get_config)
/// and passed by value down to client_pool / admission_control.
struct admission_control_config {
    policy_type policy = policy_type::passthrough;

    std::optional<reservation_policy_config> reservation;
};

} // namespace cloud_io
