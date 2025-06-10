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

#include "base/seastarx.h"
#include "cluster/commands.h"
#include "cluster/controller_stm.h"
#include "cluster/fwd.h"
#include "cluster/panda_link/table.h"
#include "features/feature_table.h"
#include "features/fwd.h"
#include "model/timeout_clock.h"
#include "panda_link/model/types.h"
#include "rpc/connection_cache.h"
#include "rpc/fwd.h"

#include <seastar/core/sharded.hh>

namespace cluster::panda_link {
class frontend : public ss::peering_sharded_service<frontend> {
    using panda_link_cmd = std::
      variant<cluster::panda_link_upsert_cmd, cluster::panda_link_remove_cmd>;

public:
    frontend(
      model::node_id,
      cluster::partition_leaders_table*,
      table*,
      cluster::controller_stm*,
      rpc::connection_cache*,
      features::feature_table*,
      ss::abort_source*);

    struct mutation_result {
        cluster::errc ec;
    };

    ss::future<mutation_result> upsert_panda_link(
      ::panda_link::model::metadata, model::timeout_clock::time_point);
    ss::future<mutation_result> remove_panda_link(
      ::panda_link::model::name_t, model::timeout_clock::time_point);

    bool panda_link_active(bool check_license) const;

private:
    ss::future<mutation_result>
      do_mutation(panda_link_cmd, model::timeout_clock::time_point);
    ss::future<mutation_result> dispatch_mutation_to_remote(
      model::node_id, panda_link_cmd, model::timeout_clock::duration);
    ss::future<mutation_result>
      do_local_mutation(panda_link_cmd, model::timeout_clock::time_point);

    cluster::errc validate_mutation(const panda_link_cmd&) const;

public:
    /// Class used to validate the incoming mutation request
    /// Made public for testing purposes
    class validator {
    public:
        explicit validator(table*, size_t max_links);

        cluster::errc validate_mutation(const panda_link_cmd&) const;

    private:
        table* _table;
        size_t _max_links;
    };

private:
    model::node_id _self;
    cluster::partition_leaders_table* _leaders;
    rpc::connection_cache* _connections;
    table* _table;
    ss::abort_source* _as;
    cluster::controller_stm* _controller;
    features::feature_table* _features;

    mutex _mu{"panda-link::frontend::mu"};
};
} // namespace cluster::panda_link
