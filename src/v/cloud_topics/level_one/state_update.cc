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

namespace {

using extent_iter_t = partition_state::extent_set_t::const_iterator;
struct extent_range {
    extent_iter_t base_it;
    extent_iter_t last_it;
};
std::optional<extent_range> get_range(
  const partition_state::extent_set_t& extents,
  kafka::offset base,
  kafka::offset last) {
    auto base_it = std::ranges::lower_bound(
      extents, base, std::less<>{}, &extent::base_offset);
    if (base_it == extents.end() || base_it->base_offset != base) {
        return std::nullopt;
    }
    // Check that the range's last offset aligns with an existing extent.
    auto last_it = std::ranges::lower_bound(
      extents, last, std::less<>{}, &extent::last_offset);
    if (last_it == extents.end() || last_it->last_offset != last) {
        return std::nullopt;
    }
    return extent_range{base_it, last_it};
}

} // namespace

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

std::expected<std::monostate, stm_update_error>
replace_objects_update::can_apply(const state& state) {
    if (new_objects.empty()) {
        return std::unexpected(stm_update_error{"No objects requested"});
    }
    new_object::sorted_extents_by_tidp_t new_extents_by_tp;
    for (const auto& o : new_objects) {
        if (state.objects.contains(o.oid)) {
            return std::unexpected(
              stm_update_error{fmt::format("Object {} already exists", o.oid)});
        }
        o.collect_extents_by_tidp(&new_extents_by_tp);
    }

    for (const auto& [tidp, new_prt_extents] : new_extents_by_tp) {
        auto req_base = new_prt_extents.begin()->base_offset;
        auto req_last = std::prev(new_prt_extents.end())->last_offset;

        auto p_state = state.partition_state(tidp);
        if (!p_state) {
            return std::unexpected(stm_update_error(
              fmt::format("Partition {} not tracked by state", tidp)));
        }

        // Check that the new range's offset aligns with existing extents.
        const auto& prt = p_state->get();
        auto iters = get_range(prt.extents, req_base, req_last);
        if (!iters.has_value()) {
            return std::unexpected(stm_update_error(fmt::format(
              "Partition {} doesn't contain extents that span exactly [{}, {}]",
              tidp,
              req_base,
              req_last)));
        }

        // Check that the new range of extents is contiguous, which in turn
        // ensures the resulting total set of extents will be contiguous.
        const auto& [base_it, last_it] = *iters;
        auto expected_next = base_it->base_offset;
        for (const auto& new_extent : new_prt_extents) {
            if (new_extent.base_offset != expected_next) {
                return std::unexpected(stm_update_error(fmt::format(
                  "Input object breaks partition {} offset ordering: expected "
                  "next: {}, actual: {}",
                  tidp,
                  expected_next,
                  new_extent.base_offset)));
            }
            expected_next = kafka::next_offset(new_extent.last_offset);
        }
    }
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
replace_objects_update::apply(state& state) {
    auto allowed = can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    new_object::sorted_extents_by_tidp_t new_extents_by_tp;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&new_extents_by_tp);
        state.objects.emplace(
          o.oid,
          object_entry{
            .total_data_size = 0,
            .removed_data_size = 0,
            .footer_pos = o.footer_pos,
          });
    }
    for (const auto& [tidp, new_extents] : new_extents_by_tp) {
        auto& p_state
          = state.topic_to_state[tidp.topic_id].pid_to_state[tidp.partition];
        auto requested_base = new_extents.begin()->base_offset;
        auto requested_last = new_extents.rbegin()->last_offset;
        auto iters = get_range(p_state.extents, requested_base, requested_last);
        auto [base_it, last_it] = *iters;
        auto end_it = std::next(last_it);
        for (auto iter = base_it; iter != end_it; ++iter) {
            auto& old_extent = *iter;
            state.objects[old_extent.oid].removed_data_size += old_extent.len;
        }

        p_state.extents.erase(base_it, end_it);
        for (const auto& e : new_extents) {
            p_state.extents.emplace(e);
        }
        // NOTE: we don't need to update the start or next offsets since we've
        // validated that the new extents replace exact ranges.

        for (const auto& extent : new_extents) {
            state.objects[extent.oid].total_data_size += extent.len;
        }
    }
    return std::monostate{};
}

std::expected<replace_objects_update, stm_update_error>
replace_objects_update::build(
  const state& state, chunked_vector<new_object> objects) {
    replace_objects_update update{
      .new_objects = std::move(objects),
    };
    auto allowed = update.can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    return update;
}

} // namespace experimental::cloud_topics::l1
