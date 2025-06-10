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
#include "model/fundamental.h"
#include "model/record.h"
#include "panda_link/model/types.h"

#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>

namespace cluster::panda_link {
/**
 * @brief Table that holds information about panda links
 */
class table : public ss::peering_sharded_service<table> {
public:
    using map_t = absl::
      flat_hash_map<::panda_link::model::id_t, ::panda_link::model::metadata>;
    table() = default;
    table(const table&) = delete;
    table(table&&) = delete;
    table& operator=(const table&) = delete;
    table& operator=(table&&) = delete;
    ~table() = default;

    /// Number of links in the table
    size_t size() const;

    /// Finds link by name
    std::optional<std::reference_wrapper<const ::panda_link::model::metadata>>
    find_link_by_name(const ::panda_link::model::name_t& name) const;
    /// Finds link by id
    std::optional<std::reference_wrapper<const ::panda_link::model::metadata>>
    find_link_by_id(::panda_link::model::id_t id) const;
    /// Finds link ID by name
    std::optional<::panda_link::model::id_t>
    find_id_by_name(const ::panda_link::model::name_t& name) const;

    bool is_batch_applicable(const model::record_batch&) const;
    ss::future<std::error_code> apply_update(model::record_batch);

    ss::future<> fill_snapshot(cluster::controller_snapshot&) const;
    ss::future<>
    apply_snapshot(model::offset, const cluster::controller_snapshot&);

private:
    using name_index_t = absl::
      flat_hash_map<::panda_link::model::name_t, ::panda_link::model::id_t>;

    /// Snapshot copy of all the panda links
    map_t all_links() const;
    /// Restores a panda link table from a snapshot
    void reset_links(map_t);

    /// Upserts a link, if the ID classes, throws a std::logic_error
    cluster::errc
      upsert_link(::panda_link::model::id_t, ::panda_link::model::metadata);
    /// Removes a link by ID
    cluster::errc remove_link(const ::panda_link::model::name_t&);

    map_t _link_metadata;
    name_index_t _name_index;
};
} // namespace cluster::panda_link
