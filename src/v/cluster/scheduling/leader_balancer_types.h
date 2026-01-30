/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "absl/container/flat_hash_map.h"
#include "config/leaders_preference.h"
#include "container/chunked_hash_map.h"
#include "model/metadata.h"
#include "raft/fundamental.h"

#include <roaring/roaring64map.hh>

#include <ranges>

namespace cluster::leader_balancer_types {

struct reassignment {
    raft::group_id group;
    model::broker_shard from;
    model::broker_shard to;

    reassignment(
      const raft::group_id& g,
      const model::broker_shard& f,
      const model::broker_shard& t)
      : group(g)
      , from(f)
      , to(t) {}

    reassignment() = default;
};

using index_type = chunked_hash_map<
  model::broker_shard,
  chunked_hash_map<raft::group_id, std::vector<model::broker_shard>>>;

// Using revision id of create_topic command as a topic identifier.
using topic_id_t
  = named_type<model::revision_id::type, struct lb_topic_id_type>;

using group_id_to_topic_id = chunked_hash_map<raft::group_id, topic_id_t>;

template<typename ValueType>
using topic_map = chunked_hash_map<topic_id_t, ValueType>;

struct leaders_preference {
    config::leaders_preference::type_t type{
      config::leaders_preference::type_t::none};
    // O(N) find operations, fine so long as the rack preference number is small
    std::vector<model::rack_id> racks;

    leaders_preference() = default;
    explicit leaders_preference(const config::leaders_preference& cfg)
      : type{cfg.type} {
        switch (cfg.type) {
        case config::leaders_preference::type_t::none:
            break;
        case config::leaders_preference::type_t::racks:
            [[fallthrough]];
        case config::leaders_preference::type_t::ordered_racks:
            racks = std::vector<model::rack_id>(std::from_range, cfg.racks);
            break;
        }
    }
};

/// Indexes needed for leadership pinning.
struct preference_index {
    leaders_preference default_preference;
    topic_map<leaders_preference> topic2preference;
    absl::flat_hash_map<model::node_id, model::rack_id> node2rack;
};

using muted_groups_t = roaring::Roaring64Map;
/*
 * Leaders per shard.
 */
struct shard_load {
    model::broker_shard shard;
    size_t leaders{0};
};

class index {
public:
    virtual ~index() = default;
    virtual void update_index(const reassignment&) = 0;
};

class soft_constraint {
    virtual double evaluate_internal(const reassignment&) = 0;

public:
    virtual ~soft_constraint() = default;
    double evaluate(const reassignment& r) {
        auto ret = evaluate_internal(r);
        return ret;
    }
};

} // namespace cluster::leader_balancer_types
