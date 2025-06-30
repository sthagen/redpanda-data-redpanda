/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/simple_metastore.h"

#include "cloud_topics/level_one/state.h"
#include "cloud_topics/level_one/state_update.h"

namespace experimental::cloud_topics::l1 {

new_object make_new_object(const metastore::object_metadata& o) {
    new_object new_o{
      .oid = object_id{o.oid()},
      .footer_pos = o.footer_pos,
    };
    for (const auto& c : o.ntp_metas) {
        auto& extents = new_o.extent_metas[c.tidp.topic_id];
        extents[c.tidp.partition] = new_object::metadata{
          .base_offset = c.base_offset,
          .last_offset = c.last_offset,
          .max_timestamp = c.max_timestamp,
          .filepos = c.pos,
          .len = c.size,
        };
    }
    return new_o;
}

ss::future<std::expected<metastore::offsets_response, metastore::errc>>
simple_metastore::get_offsets(const model::topic_id_partition& tpr) {
    auto prt_ref = state_.partition_state(tpr);
    if (!prt_ref.has_value()) {
        co_return std::unexpected(metastore::errc::missing_ntp);
    }
    const auto& prt = prt_ref->get();
    co_return offsets_response{
      .start_offset = prt.start_offset,
      .next_offset = prt.next_offset,
    };
}

ss::future<std::expected<void, metastore::errc>>
simple_metastore::add_objects(const chunked_vector<object_metadata>& objects) {
    chunked_vector<new_object> new_objects;
    for (const auto& o : objects) {
        new_objects.emplace_back(make_new_object(o));
    }
    auto update_res = add_objects_update::build(state_, std::move(new_objects));
    if (!update_res.has_value()) {
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    auto apply_res = update_res->apply(state_);
    vassert(apply_res.has_value(), "Apply must succeed if can_apply() is true");
    co_return std::expected<void, metastore::errc>{};
}

ss::future<std::expected<metastore::object_response, metastore::errc>>
simple_metastore::get_first_ge(
  const model::topic_id_partition& tpr, kafka::offset o) {
    auto prt_ref = state_.partition_state(tpr);
    if (!prt_ref.has_value()) {
        co_return std::unexpected(metastore::errc::missing_ntp);
    }
    auto& prt = prt_ref->get();
    auto it = std::ranges::lower_bound(
      prt.extents, o, std::less<>{}, &extent::last_offset);
    if (it != prt.extents.end()) {
        auto footer_pos = state_.objects[it->oid].footer_pos;
        co_return metastore::object_response{
          .oid = it->oid,
          .footer_pos = footer_pos,
        };
    }
    co_return std::unexpected(metastore::errc::out_of_range);
}

ss::future<std::expected<metastore::object_response, metastore::errc>>
simple_metastore::get_first_ge(
  const model::topic_id_partition& tpr, model::timestamp ts) {
    auto prt_ref = state_.partition_state(tpr);
    if (!prt_ref.has_value()) {
        co_return std::unexpected(metastore::errc::missing_ntp);
    }
    auto& prt = prt_ref->get();
    for (const auto& obj : prt.extents) {
        if (obj.max_timestamp >= ts) {
            auto footer_pos = state_.objects[obj.oid].footer_pos;
            co_return metastore::object_response{
              .oid = obj.oid,
              .footer_pos = footer_pos,
            };
        }
    }
    co_return std::unexpected(metastore::errc::out_of_range);
}

} // namespace experimental::cloud_topics::l1
