/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/container/btree_set.h"
#include "base/format_to.h"
#include "base/seastarx.h"
#include "container/chunked_vector.h"
#include "model/metadata.h"
#include "security/types.h"
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sstring.hh>

#include <fmt/chrono.h>
#include <fmt/core.h>
#include <fmt/format.h>

#include <chrono>
#include <ctime>
#include <optional>
#include <type_traits>
#include <variant>
namespace debug_bundle {

inline constexpr ss::shard_id service_shard = 0;

using job_id_t = named_type<uuid_t, struct uuid_t_tag>;

/// Special date strings used by journalctl
enum class special_date { yesterday, today, now, tomorrow };

constexpr std::string_view to_string_view(special_date d) {
    switch (d) {
    case special_date::yesterday:
        return "yesterday";
    case special_date::today:
        return "today";
    case special_date::now:
        return "now";
    case special_date::tomorrow:
        return "tomorrow";
    }
}
inline fmt::iterator format_to(special_date d, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(d));
}

std::istream& operator>>(std::istream& i, special_date& d);

/// Defines which clock the debug bundle will use
using clock = ss::lowres_system_clock;
/// When provided a time, may either be a time point or a special_date
using time_variant = std::variant<clock::time_point, special_date>;

/// SCRAM credentials for authn
struct scram_creds {
    security::credential_user username;
    security::credential_password password;
    ss::sstring mechanism;

    friend bool operator==(const scram_creds&, const scram_creds&) = default;
};
/// Variant so it can be expanded as new authn methods are added to rpk
using debug_bundle_authn_options = std::variant<scram_creds>;

/// Used to collect topics and partitions for the "--partitions" option for "rpk
/// debug_bundle"
struct partition_selection {
    model::topic_namespace tn;
    absl::btree_set<model::partition_id> partitions;

    static std::optional<partition_selection>
      from_string_view(std::string_view);

    friend bool operator==(
      const partition_selection&, const partition_selection&) = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it, "{}/{}/{}", tn.ns, tn.tp, fmt::join(partitions, ","));
    }
};

struct label_selection {
    ss::sstring key;
    ss::sstring value;

    friend bool
    operator==(const label_selection&, const label_selection&) = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}={}", key, value);
    }
};

/// Parameters used to spawn rpk debug bundle
struct debug_bundle_parameters {
    std::optional<debug_bundle_authn_options> authn_options;
    std::optional<uint64_t> controller_logs_size_limit_bytes;
    std::optional<std::chrono::seconds> cpu_profiler_wait_seconds;
    std::optional<time_variant> logs_since;
    std::optional<uint64_t> logs_size_limit_bytes;
    std::optional<time_variant> logs_until;
    std::optional<std::chrono::seconds> metrics_interval_seconds;
    std::optional<uint64_t> metrics_samples;
    std::optional<std::vector<partition_selection>> partition;
    std::optional<bool> tls_enabled;
    std::optional<bool> tls_insecure_skip_verify;
    std::optional<ss::sstring> k8s_namespace;
    std::optional<std::vector<label_selection>> label_selector;

    friend bool operator==(
      const debug_bundle_parameters&, const debug_bundle_parameters&) = default;
};

/// The state of the debug bundle process
enum class debug_bundle_status { running, success, error, expired };

constexpr std::string_view to_string_view(debug_bundle_status s) {
    switch (s) {
    case debug_bundle_status::running:
        return "running";
    case debug_bundle_status::success:
        return "success";
    case debug_bundle_status::error:
        return "error";
    case debug_bundle_status::expired:
        return "expired";
    }
}
inline fmt::iterator format_to(debug_bundle_status s, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(s));
}

/// Status of the debug bundle process
struct debug_bundle_status_data {
    job_id_t job_id;
    debug_bundle_status status;
    clock::time_point created_timestamp;
    ss::sstring file_name;
    std::optional<size_t> file_size;
    chunked_vector<ss::sstring> cout;
    chunked_vector<ss::sstring> cerr;

    friend bool operator==(
      const debug_bundle_status_data& lhs,
      const debug_bundle_status_data& rhs) = default;
};
} // namespace debug_bundle

template<>
struct fmt::formatter<debug_bundle::time_variant>
  : formatter<std::string_view> {
    auto format(const debug_bundle::time_variant& t, format_context& ctx) const
      -> format_context::iterator {
        return std::visit(
          [&ctx](const auto& value) -> format_context::iterator {
              using T = std::decay_t<decltype(value)>;
              if constexpr (
                std::is_same_v<T, debug_bundle::clock::time_point>) {
                  auto tt = debug_bundle::clock::to_time_t(value);
                  auto tm = *std::localtime(&tt);
                  return fmt::format_to(ctx.out(), "{:%FT%T}", tm);
              } else {
                  return fmt::format_to(ctx.out(), "{}", value);
              }
          },
          t);
    }
};
