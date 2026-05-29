// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/base_property.h"

#include "base/vassert.h"
#include "config/config_store.h"
#include "container/chunked_hash_map.h"

#include <memory>

namespace config {
namespace {

/// Deduplicate `meta` keyed by `name`, returning a canonical pointer that is
/// stable for the lifetime of the process.
const base_property::metadata* intern_metadata(
  std::string_view name, std::string_view desc, base_property::metadata meta) {
    static thread_local chunked_hash_map<
      std::string_view,
      std::unique_ptr<const base_property::metadata>>
      metadata_table;
    if (auto it = metadata_table.find(name); it != metadata_table.end()) {
        return it->second.get();
    }
    meta.name = name;
    meta.desc = desc;
    auto up = std::make_unique<const base_property::metadata>(std::move(meta));
    const auto* raw = up.get();
    metadata_table.emplace(name, std::move(up));
    return raw;
}

} // namespace

base_property::base_property(
  config_store& conf,
  std::string_view name,
  std::string_view desc,
  base_property::metadata meta)
  : _meta(intern_metadata(name, desc, std::move(meta))) {
    conf._properties.emplace(_meta->name, this);
    for (const auto& alias : _meta->aliases) {
        auto [_, inserted] = conf._aliases.emplace(alias, this);

        vassert(inserted, "Two properties tried to register the same alias");
    }
}

std::string_view to_string_view(visibility v) {
    switch (v) {
    case config::visibility::tunable:
        return "tunable";
    case config::visibility::user:
        return "user";
    case config::visibility::deprecated:
        return "deprecated";
    }

    return "{invalid}";
}
fmt::iterator format_to(visibility v, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(v));
}

/**
 * Helper for property methods that should only be used
 * on live-settable properties.
 */
void base_property::assert_live_settable() const {
    vassert(
      _meta->needs_restart == needs_restart::no,
      "Property {} must be be marked as needs_restart::no",
      name());
}

}; // namespace config
