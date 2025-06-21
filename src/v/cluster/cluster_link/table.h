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

#include "cluster/controller_snapshot.h"
#include "cluster_link/model/types.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>

namespace cluster::cluster_link {
/**
 * @brief Table that holds information about panda links
 */
class table : public ss::peering_sharded_service<table> {
public:
    using map_t = absl::flat_hash_map<
      ::cluster_link::model::id_t,
      ::cluster_link::model::metadata>;
    table() = default;
    table(const table&) = delete;
    table(table&&) = delete;
    table& operator=(const table&) = delete;
    table& operator=(table&&) = delete;
    ~table() = default;

    using notification_id
      = named_type<size_t, struct cluster_link_notification_tag>;
    using notification_callback
      = ss::noncopyable_function<void(::cluster_link::model::id_t)>;

    /// Number of links in the table
    size_t size() const;

    /// Finds link by name
    std::optional<std::reference_wrapper<const ::cluster_link::model::metadata>>
    find_link_by_name(const ::cluster_link::model::name_t& name) const;
    /// Finds link by id
    std::optional<std::reference_wrapper<const ::cluster_link::model::metadata>>
    find_link_by_id(::cluster_link::model::id_t id) const;
    /// Finds link ID by name
    std::optional<::cluster_link::model::id_t>
    find_id_by_name(const ::cluster_link::model::name_t& name) const;

    /// Returns a list of all link IDs in the table
    chunked_vector<::cluster_link::model::id_t> get_all_link_ids() const;

    bool is_batch_applicable(const model::record_batch&) const;
    ss::future<std::error_code> apply_update(model::record_batch);

    ss::future<> fill_snapshot(cluster::controller_snapshot&) const;
    ss::future<>
    apply_snapshot(model::offset, const cluster::controller_snapshot&);

    notification_id register_for_updates(notification_callback);
    void unregister_for_updates(notification_id);

private:
    /// Snapshot copy of all the panda links
    map_t all_links() const;
    /// Restores a panda link table from a snapshot
    void reset_links(map_t);

    /// Upserts a link, if the ID classes, throws a std::logic_error
    cluster::errc
      upsert_link(::cluster_link::model::id_t, ::cluster_link::model::metadata);
    /// Removes a link by ID
    cluster::errc remove_link(const ::cluster_link::model::name_t&);

    void run_callbacks(::cluster_link::model::id_t);

private:
    using name_index_t = absl::
      flat_hash_map<::cluster_link::model::name_t, ::cluster_link::model::id_t>;

    map_t _link_metadata;
    name_index_t _name_index;

    absl::flat_hash_map<notification_id, notification_callback> _callbacks;
    notification_id _latest_id{0};
};
} // namespace cluster::cluster_link
