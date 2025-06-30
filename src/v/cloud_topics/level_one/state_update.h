/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "absl/container/btree_set.h"
#include "base/seastarx.h"
#include "cloud_topics/level_one/state.h"
#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"

#include <expected>

namespace experimental::cloud_topics::l1 {

enum class update_key : uint8_t {
    add_objects = 0,
};

using stm_update_error = named_type<ss::sstring, struct update_error_tag>;

struct new_object
  : public serde::
      envelope<new_object, serde::version<0>, serde::compat_version<0>> {
    struct metadata
      : public serde::
          envelope<metadata, serde::version<0>, serde::compat_version<0>> {
        friend bool operator==(const metadata&, const metadata&) = default;
        auto serde_fields() {
            return std::tie(
              base_offset, last_offset, max_timestamp, filepos, len);
        }

        kafka::offset base_offset;
        kafka::offset last_offset;
        model::timestamp max_timestamp;
        size_t filepos;
        size_t len;
    };

    friend bool operator==(const new_object&, const new_object&) = default;
    auto serde_fields() { return std::tie(oid, extent_metas); }

    object_id oid;
    size_t footer_pos;
    chunked_hash_map<
      model::topic_id,
      chunked_hash_map<model::partition_id, metadata>>
      extent_metas;

    using sorted_extents_by_tidp_t = chunked_hash_map<
      model::topic_id_partition,
      absl::btree_multiset<extent>>;
    void collect_extents_by_tidp(sorted_extents_by_tidp_t*) const;
};

struct add_objects_update
  : public serde::envelope<
      add_objects_update,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool operator==(const add_objects_update&, const add_objects_update&)
      = default;
    auto serde_fields() { return std::tie(new_objects); }

    static constexpr auto key{update_key::add_objects};
    static std::expected<add_objects_update, stm_update_error>
    build(const state&, chunked_vector<new_object>);

    std::expected<std::monostate, stm_update_error> can_apply(const state&);
    std::expected<std::monostate, stm_update_error> apply(state&);

    chunked_vector<new_object> new_objects;
};

} // namespace experimental::cloud_topics::l1

template<>
struct fmt::formatter<experimental::cloud_topics::l1::update_key> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto format(
      const experimental::cloud_topics::l1::update_key& k,
      FormatContext& ctx) const {
        switch (k) {
        case experimental::cloud_topics::l1::update_key::add_objects:
            return formatter<string_view>::format("add_objects", ctx);
        }
    }
};
