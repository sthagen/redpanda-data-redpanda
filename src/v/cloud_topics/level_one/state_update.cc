/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/state_update.h"

#include "model/fundamental.h"

namespace experimental::cloud_topics::l1 {

void new_object::collect_extents_by_tidp(sorted_extents_by_tidp_t* ret) const {
    for (const auto& [tid, p_extents] : extent_metas) {
        for (const auto& [p, extent_meta] : p_extents) {
            auto& ret_extents = (*ret)[model::topic_id_partition(tid, p)];
            ret_extents.insert(extent{
              .base_offset = extent_meta.base_offset,
              .last_offset = extent_meta.last_offset,
              .max_timestamp = extent_meta.max_timestamp,
              .filepos = extent_meta.filepos,
              .len = extent_meta.len,
              .oid = oid,
            });
        }
    }
}

std::expected<add_objects_update, stm_update_error> add_objects_update::build(
  const state& state, chunked_vector<new_object> objects) {
    add_objects_update update{
      .new_objects = std::move(objects),
    };
    auto allowed = update.can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    return update;
}

std::expected<std::monostate, stm_update_error>
add_objects_update::can_apply(const state& state) {
    if (new_objects.empty()) {
        return std::unexpected(stm_update_error{"No objects requested"});
    }
    new_object::sorted_extents_by_tidp_t new_extents;
    for (const auto& o : new_objects) {
        if (state.objects.contains(o.oid)) {
            return std::unexpected(
              stm_update_error{fmt::format("Object {} already exists", o.oid)});
        }
        o.collect_extents_by_tidp(&new_extents);
    }

    for (const auto& [tidp, extents] : new_extents) {
        // TODO: maybe we need some mount operation that adopts a partition log
        // and allows it to start a specific offset.
        auto p_state = state.partition_state(tidp);
        auto expected_next = p_state ? p_state->get().next_offset
                                     : kafka::offset{0};

        for (const auto& extent : extents) {
            if (extent.base_offset != expected_next) {
                return std::unexpected(stm_update_error(fmt::format(
                  "Input object breaks partition {} offset ordering: expected "
                  "next: {}, actual: {}",
                  tidp,
                  expected_next,
                  extent.base_offset)));
            }
            expected_next = kafka::next_offset(extent.last_offset);
        }
    }
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
add_objects_update::apply(state& state) {
    auto allowed = can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    new_object::sorted_extents_by_tidp_t extents_by_tpr;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&extents_by_tpr);
        state.objects.emplace(
          o.oid,
          object_entry{
            .total_data_size = 0,
            .removed_data_size = 0,
            .footer_pos = o.footer_pos,
          });
    }
    for (const auto& [tidp, extents] : extents_by_tpr) {
        auto& t_state = state.topic_to_state[tidp.topic_id];
        auto& p_state = t_state.pid_to_state[tidp.partition];
        for (const auto& e : extents) {
            p_state.extents.emplace(e);
        }
        p_state.next_offset = kafka::next_offset(
          p_state.extents.rbegin()->last_offset);

        for (const auto& extent : extents) {
            state.objects[extent.oid].total_data_size += extent.len;
        }
    }
    return std::monostate{};
}

} // namespace experimental::cloud_topics::l1
