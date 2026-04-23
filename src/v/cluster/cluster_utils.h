/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/errc.h"
#include "cluster/members_table.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "raft/fundamental.h"
#include "utils/functional.h"

#include <seastar/core/sstring.hh>

#include <algorithm>
#include <concepts>
#include <optional>
#include <ranges>
#include <system_error>
#include <vector>

namespace detail {

template<
  template<typename...> class Container = std::vector,
  std::ranges::sized_range Rng,
  typename Fn>
requires std::same_as<
  std::invoke_result_t<Fn, std::ranges::range_value_t<Rng>>,
  cluster::topic_result>
Container<cluster::topic_result>
make_error_topic_results(const Rng& topics, Fn fn) {
    Container<cluster::topic_result> results;
    results.reserve(topics.size());
    std::transform(
      topics.cbegin(), topics.cend(), std::back_inserter(results), fn);
    return results;
}

template<typename T>
concept has_tp_ns = requires {
    {
        std::declval<T>().tp_ns
    } -> std::convertible_to<const model::topic_namespace&>;
};

template<typename T>
const model::topic_namespace& extract_tp_ns(const T& t) {
    if constexpr (std::same_as<T, model::topic_namespace>) {
        return t;
    } else if constexpr (has_tp_ns<T>) {
        return t.tp_ns;
    } else if constexpr (
      std::same_as<T, cluster::custom_assignable_topic_configuration>) {
        return t.cfg.tp_ns;
    } else {
        static_assert(always_false_v<T>, "couldn't extract tp_ns");
    }
}

} // namespace detail

namespace cluster {

/// Creates the same topic_result for all requests
template<template<typename...> class Container = std::vector>
Container<topic_result> make_error_topic_results(
  const std::ranges::sized_range auto& topics, errc error_code) {
    return detail::make_error_topic_results<Container>(
      topics, [error_code](const auto& t) {
          return topic_result(detail::extract_tp_ns(t), error_code);
      });
}

bool are_replica_sets_equal(
  const std::vector<model::broker_shard>&,
  const std::vector<model::broker_shard>&);

custom_assignable_topic_configuration_vector
  without_custom_assignments(topic_configuration_vector);

template<class T>
inline std::vector<T>
subtract(const std::vector<T>& lhs, const std::vector<T>& rhs) {
    std::vector<T> ret;
    std::copy_if(
      lhs.begin(), lhs.end(), std::back_inserter(ret), [&rhs](const T& bs) {
          return std::find(rhs.begin(), rhs.end(), bs) == rhs.end();
      });
    return ret;
}

template<class T>
inline std::vector<T>
union_vectors(const std::vector<T>& lhs, const std::vector<T>& rhs) {
    std::vector<T> ret;
    // Inefficient but constant time for small vectors.
    std::copy_if(
      lhs.begin(), lhs.end(), std::back_inserter(ret), [&ret](const T& bs) {
          return std::find(ret.begin(), ret.end(), bs) == ret.end();
      });
    std::copy_if(
      rhs.begin(), rhs.end(), std::back_inserter(ret), [&ret](const T& bs) {
          return std::find(ret.begin(), ret.end(), bs) == ret.end();
      });
    return ret;
}

template<class T>
inline std::vector<T>
intersect(const std::vector<T>& lhs, const std::vector<T>& rhs) {
    std::vector<T> ret;
    ret.reserve(std::min(lhs.size(), rhs.size()));
    // Inefficient but constant time for inputs.
    std::copy_if(
      lhs.begin(), lhs.end(), std::back_inserter(ret), [&](const T& entry) {
          return std::find(rhs.begin(), rhs.end(), entry) != rhs.end();
      });
    return ret;
}

// Checks if lhs is a proper subset of rhs
inline bool is_proper_subset(
  const std::vector<model::broker_shard>& lhs,
  const std::vector<model::broker_shard>& rhs) {
    auto contains_all = std::all_of(
      lhs.begin(), lhs.end(), [&rhs](const auto& current) {
          return std::find(rhs.begin(), rhs.end(), current) != rhs.end();
      });

    return contains_all && rhs.size() > lhs.size();
}

/**
 * Subtracts second replica set from the first one. Result contains only brokers
 * that node_ids are present in the first list but not the other one
 */
template<class T>
requires std::is_same_v<T, model::broker_shard>
         || std::is_same_v<T, model::node_id>
inline std::vector<model::broker_shard> subtract_replica_sets_by_node_id(
  const std::vector<model::broker_shard>& lhs, const std::vector<T>& rhs) {
    std::vector<model::broker_shard> ret;
    std::copy_if(
      lhs.begin(),
      lhs.end(),
      std::back_inserter(ret),
      [&rhs](const model::broker_shard& lhs_bs) {
          return std::find_if(
                   rhs.begin(),
                   rhs.end(),
                   [&lhs_bs](const T& entry) {
                       if constexpr (std::is_same_v<T, model::broker_shard>) {
                           return entry.node_id == lhs_bs.node_id;
                       }
                       return entry == lhs_bs.node_id;
                   })
                 == rhs.end();
      });
    return ret;
}

// check if replica set contains a node
inline bool contains_node(
  const std::vector<model::broker_shard>& replicas, model::node_id id) {
    return std::find_if(
             replicas.begin(),
             replicas.end(),
             [id](const model::broker_shard& bs) { return bs.node_id == id; })
           != replicas.end();
}

inline bool
contains_node(const std::vector<raft::vnode>& replicas, model::node_id id) {
    return std::ranges::any_of(
      replicas, [id](const raft::vnode& vn) { return vn.id() == id; });
}

inline std::optional<ss::shard_id>
find_shard_on_node(const replicas_t& replicas, model::node_id node) {
    for (const auto& bs : replicas) {
        if (bs.node_id == node) {
            return bs.shard;
        }
    }
    return std::nullopt;
}

// check if replica is moving from node
inline bool moving_from_node(
  model::node_id node,
  const std::vector<model::broker_shard>& previous_replicas,
  const std::vector<model::broker_shard>& result_replicas) {
    if (!contains_node(previous_replicas, node)) {
        return false;
    }
    if (!contains_node(result_replicas, node)) {
        return true;
    }
    return false;
}

// check if replica is moving to node
inline bool moving_to_node(
  model::node_id node,
  const std::vector<model::broker_shard>& previous_replicas,
  const std::vector<model::broker_shard>& result_replicas) {
    if (contains_node(previous_replicas, node)) {
        return false;
    }
    if (contains_node(result_replicas, node)) {
        return true;
    }
    return false;
}

cluster::errc map_update_interruption_error_code(std::error_code);

/**
 * Check that the configuration is valid, if not return a string with the
 * error cause.
 *
 * @param current_brokers current broker vector
 * @param to_update broker being added
 * @return std::optional<ss::sstring> - present if there is an error, nullopt
 * otherwise
 */
std::optional<ss::sstring> check_result_configuration(
  const members_table::cache_t& current_brokers,
  const model::broker& to_update);

} // namespace cluster
