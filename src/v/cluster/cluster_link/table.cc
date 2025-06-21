/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/table.h"

#include "base/vassert.h"
#include "cluster/commands.h"
#include "cluster/logger.h"

namespace cluster::cluster_link {

using ::cluster_link::model::id_t;
using ::cluster_link::model::metadata;
using ::cluster_link::model::name_t;

namespace {
static constexpr auto accepted_commands = cluster::make_commands_list<
  cluster::cluster_link_upsert_cmd,
  cluster::cluster_link_remove_cmd>();
}

table::map_t table::all_links() const { return _link_metadata; }

size_t table::size() const { return _link_metadata.size(); }

void table::reset_links(map_t links) {
    name_index_t snap_name_index;
    chunked_vector<id_t> all_deletes;
    chunked_vector<id_t> all_inserts;
    chunked_vector<id_t> all_changes;

    for (const auto& [k, v] : _link_metadata) {
        auto it = links.find(k);
        if (it == links.end()) {
            all_deletes.push_back(k);
        } else if (v != it->second) {
            all_changes.push_back(k);
        }
    }

    for (const auto& [id, metadata] : links) {
        if (!_link_metadata.contains(id)) {
            all_inserts.push_back(id);
        }
        auto it = snap_name_index.emplace(metadata.name, id);
        if (!it.second) {
            throw std::logic_error(fmt::format(
              "panda link id={} is attempting to use a name {} which is "
              "already registered to {}",
              id,
              metadata.name,
              it.first->second));
        }
    }

    _link_metadata = std::move(links);
    _name_index = std::move(snap_name_index);

    for (const auto& deleted : all_deletes) {
        run_callbacks(deleted);
    }
    for (const auto& inserted : all_inserts) {
        run_callbacks(inserted);
    }
    for (const auto& changed : all_changes) {
        run_callbacks(changed);
    }
}
std::optional<std::reference_wrapper<const metadata>>
table::find_link_by_name(const name_t& name) const {
    auto id = find_id_by_name(name);
    if (!id) {
        return std::nullopt;
    }
    auto meta = find_link_by_id(id.value());
    vassert(
      meta.has_value(),
      "Inconsistent name index for {} expected id {}",
      name,
      id.value());

    return meta;
}

std::optional<std::reference_wrapper<const metadata>>
table::find_link_by_id(id_t id) const {
    auto it = _link_metadata.find(id);
    if (it == _link_metadata.end()) {
        return std::nullopt;
    }
    return std::cref(it->second);
}

std::optional<id_t> table::find_id_by_name(const name_t& name) const {
    auto it = _name_index.find(name);
    if (it == _name_index.end()) {
        return std::nullopt;
    }
    return it->second;
}

chunked_vector<id_t> table::get_all_link_ids() const {
    auto keys = _link_metadata | std::views::transform([](const auto& pair) {
                    return pair.first;
                });

    return {keys.begin(), keys.end()};
}

bool table::is_batch_applicable(const model::record_batch& b) const {
    return b.header().type == model::record_batch_type::cluster_link;
}

ss::future<std::error_code> table::apply_update(model::record_batch b) {
    auto offset = b.base_offset();
    auto cmd = co_await deserialize(std::move(b), accepted_commands);
    auto results = co_await container().map([cmd, offset](table& table) {
        return ss::visit(
          cmd,
          [&table, offset](const cluster::cluster_link_upsert_cmd& upsert) {
              auto existing_id = table.find_id_by_name(upsert.value.name);
              return table.upsert_link(
                existing_id.value_or(id_t{offset}), std::move(upsert.value));
          },
          [&table](const cluster::cluster_link_remove_cmd& remove) {
              return table.remove_link(remove.key);
          });
    });
    auto first_res = results.front();
    auto state_consistent = std::ranges::all_of(
      results, [first_res](cluster::errc res) { return first_res == res; });

    vassert(
      state_consistent,
      "State inconsistency across shards detected, expected result: {}, "
      "have: {}",
      first_res,
      results);

    co_return first_res;
}

ss::future<> table::fill_snapshot(cluster::controller_snapshot& snap) const {
    snap.cluster_links.links = all_links();
    return ss::now();
}

ss::future<>
table::apply_snapshot(model::offset, const cluster::controller_snapshot& snap) {
    return container().invoke_on_all(
      [&snap](table& table) { table.reset_links(snap.cluster_links.links); });
}

table::notification_id table::register_for_updates(notification_callback cb) {
    auto it = _callbacks.insert({++_latest_id, std::move(cb)});
    vassert(it.second, "Invalid duplicate in callbacks");
    return _latest_id;
}

void table::unregister_for_updates(notification_id id) { _callbacks.erase(id); }

void table::run_callbacks(id_t id) {
    for (const auto& [_, cb] : _callbacks) {
        cb(id);
    }
}

cluster::errc table::upsert_link(id_t id, metadata meta) {
    auto name_it = _name_index.find(meta.name);
    if (name_it != _name_index.end()) {
        if (name_it->second != id) {
            vlog(
              cluster::clusterlog.error,
              "panda link id={} is attempting to use a name {} which is "
              "already registered to {}",
              id,
              meta.name,
              name_it->second);
            return cluster::errc::cluster_link_service_error;
        }
    } else {
        _name_index.emplace(meta.name, id);
    }

    _link_metadata.insert_or_assign(id, std::move(meta));
    run_callbacks(id);
    return cluster::errc::success;
}

cluster::errc table::remove_link(const name_t& name) {
    auto name_it = _name_index.find(name);
    if (name_it == _name_index.end()) {
        return cluster::errc::success;
    }

    auto id = name_it->second;
    auto it = _link_metadata.find(id);
    vassert(
      it != _link_metadata.end(),
      "Inconsistent name index for {} expected id {}",
      name,
      id);
    _name_index.erase(name_it);
    _link_metadata.erase(it);

    run_callbacks(id);
    return cluster::errc::success;
}
} // namespace cluster::cluster_link
