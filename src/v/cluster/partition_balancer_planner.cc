/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/partition_balancer_planner.h"

#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/types.h"

#include <optional>

namespace cluster {

namespace {

hard_constraint_evaluator
distinct_from(const absl::flat_hash_set<model::node_id>& nodes) {
    class impl : public hard_constraint_evaluator::impl {
    public:
        explicit impl(const absl::flat_hash_set<model::node_id>& nodes)
          : _nodes(nodes) {}

        bool evaluate(const allocation_node& node) const final {
            return !_nodes.contains(node.id());
        }

        void print(std::ostream& o) const final {
            fmt::print(
              o,
              "distinct from nodes: {}",
              std::vector(_nodes.begin(), _nodes.end()));
        }

    private:
        const absl::flat_hash_set<model::node_id>& _nodes;
    };

    return hard_constraint_evaluator(std::make_unique<impl>(nodes));
}

} // namespace

partition_balancer_planner::partition_balancer_planner(
  planner_config config,
  topic_table& topic_table,
  partition_allocator& partition_allocator)
  : _config(config)
  , _topic_table(topic_table)
  , _partition_allocator(partition_allocator) {
    _config.soft_max_disk_usage_ratio = std::min(
      _config.soft_max_disk_usage_ratio, _config.hard_max_disk_usage_ratio);
}

void partition_balancer_planner::init_node_disk_reports_from_health_report(
  const cluster_health_report& health_report, reallocation_request_state& rrs) {
    for (const auto& node_report : health_report.node_reports) {
        uint64_t total = 0;
        uint64_t free = 0;
        for (const auto& disk : node_report.local_state.disks) {
            total += disk.total;
            free += disk.free;
        }
        rrs.node_disk_reports.emplace(
          node_report.id, node_disk_space(node_report.id, total, total - free));
    }
}

void partition_balancer_planner::init_ntp_sizes_from_health_report(
  const cluster_health_report& health_report, reallocation_request_state& rrs) {
    for (const auto& node_report : health_report.node_reports) {
        for (const auto& tp_ns : node_report.topics) {
            for (const auto& partition : tp_ns.partitions) {
                rrs.ntp_sizes[model::ntp(
                  tp_ns.tp_ns.ns, tp_ns.tp_ns.tp, partition.id)]
                  = partition.size_bytes;
            }
        }
    }
}

void partition_balancer_planner::
  calculate_nodes_with_disk_constraints_violation(
    reallocation_request_state& rrs, plan_data& result) {
    for (const auto& n : rrs.node_disk_reports) {
        double used_space_ratio = n.second.original_used_ratio();
        if (used_space_ratio > _config.soft_max_disk_usage_ratio) {
            result.violations.full_nodes.emplace_back(
              n.first, uint32_t(used_space_ratio * 100.0));
        }
    }
}

void partition_balancer_planner::calculate_unavailable_nodes(
  const std::vector<raft::follower_metrics>& follower_metrics,
  reallocation_request_state& rrs,
  plan_data& result) {
    const auto now = raft::clock_type::now();
    for (const auto& follower : follower_metrics) {
        auto unavailable_dur = now - follower.last_heartbeat;
        if (unavailable_dur > _config.node_availability_timeout_sec) {
            rrs.unavailable_nodes.insert(follower.id);
            model::timestamp unavailable_since = model::to_timestamp(
              model::timestamp_clock::now()
              - std::chrono::duration_cast<model::timestamp_clock::duration>(
                unavailable_dur));
            result.violations.unavailable_nodes.emplace_back(
              follower.id, unavailable_since);
        }
    }
}

bool partition_balancer_planner::all_reports_received(
  const reallocation_request_state& rrs) {
    for (auto& s = _partition_allocator.state();
         const auto& node : s.allocation_nodes()) {
        if (
          !rrs.unavailable_nodes.contains(node.first)
          && !rrs.node_disk_reports.contains(node.first)) {
            vlog(clusterlog.info, "No disk report for node {}", node.first);
            return false;
        }
    }
    return true;
}

bool partition_balancer_planner::is_partition_movement_possible(
  const std::vector<model::broker_shard>& current_replicas,
  const reallocation_request_state& rrs) {
    // Check that nodes quorum is available
    size_t available_nodes_amount = std::count_if(
      current_replicas.begin(),
      current_replicas.end(),
      [&rrs](const model::broker_shard& bs) {
          return rrs.unavailable_nodes.find(bs.node_id)
                 == rrs.unavailable_nodes.end();
      });
    if (available_nodes_amount * 2 < current_replicas.size()) {
        return false;
    }
    return true;
}

std::optional<size_t> partition_balancer_planner::get_partition_size(
  const model::ntp& ntp, const reallocation_request_state& rrs) {
    const auto ntp_data = rrs.ntp_sizes.find(ntp);
    if (ntp_data == rrs.ntp_sizes.end()) {
        vlog(
          clusterlog.info,
          "Partition {} status was not found in cluster health "
          "report",
          ntp);
    } else {
        return ntp_data->second;
    }
    return std::nullopt;
}

partition_constraints partition_balancer_planner::get_partition_constraints(
  const partition_assignment& assignments,
  const topic_metadata& topic_metadata,
  size_t partition_size,
  double max_disk_usage_ratio,
  reallocation_request_state& rrs) const {
    allocation_constraints allocation_constraints;

    // Add constraint on least disk usage
    allocation_constraints.soft_constraints.push_back(
      ss::make_lw_shared<soft_constraint_evaluator>(
        least_disk_filled(max_disk_usage_ratio, rrs.node_disk_reports)));

    // Add constraint on partition max_disk_usage_ratio overfill
    allocation_constraints.hard_constraints.push_back(
      ss::make_lw_shared<hard_constraint_evaluator>(
        disk_not_overflowed_by_partition(
          max_disk_usage_ratio, partition_size, rrs.node_disk_reports)));

    // Add constraint on unavailable nodes
    allocation_constraints.hard_constraints.push_back(
      ss::make_lw_shared<hard_constraint_evaluator>(
        distinct_from(rrs.unavailable_nodes)));

    return partition_constraints(
      assignments.id,
      topic_metadata.get_configuration().replication_factor,
      std::move(allocation_constraints));
}

result<allocation_units> partition_balancer_planner::get_reallocation(
  const model::ntp& ntp,
  const partition_assignment& assignments,
  size_t partition_size,
  partition_constraints constraints,
  const std::vector<model::broker_shard>& stable_replicas,
  reallocation_request_state& rrs) {
    vlog(
      clusterlog.debug,
      "trying to find reallocation for ntp {} with stable_replicas: {}",
      ntp,
      stable_replicas);

    auto stable_assigments = partition_assignment(
      assignments.group, assignments.id, stable_replicas);

    auto reallocation = _partition_allocator.reallocate_partition(
      std::move(constraints), stable_assigments);

    if (!reallocation) {
        vlog(
          clusterlog.debug,
          "attempt to find reallocation for ntp {} with "
          "stable_replicas: {} failed, error: {}",
          ntp,
          stable_replicas,
          reallocation.error().message());

        return reallocation;
    }

    rrs.moving_partitions.insert(ntp);
    rrs.planned_moves_size += partition_size;
    for (const auto r :
         reallocation.value().get_assignments().front().replicas) {
        if (
          std::find(stable_replicas.begin(), stable_replicas.end(), r)
          == stable_replicas.end()) {
            auto disk_it = rrs.node_disk_reports.find(r.node_id);
            if (disk_it != rrs.node_disk_reports.end()) {
                disk_it->second.assigned += partition_size;
            }
        }
    }
    for (const auto r : assignments.replicas) {
        if (
          std::find(stable_replicas.begin(), stable_replicas.end(), r)
          == stable_replicas.end()) {
            auto disk_it = rrs.node_disk_reports.find(r.node_id);
            if (disk_it != rrs.node_disk_reports.end()) {
                disk_it->second.released += partition_size;
            }
        }
    }

    return reallocation;
}

/*
 * Function is trying to move ntp out of unavailable nodes
 * It can move to nodes that are violating soft_max_disk_usage_ratio constraint
 */
void partition_balancer_planner::get_unavailable_nodes_reassignments(
  plan_data& result, reallocation_request_state& rrs) {
    if (rrs.unavailable_nodes.empty()) {
        return;
    }

    for (const auto& t : _topic_table.topics_map()) {
        for (const auto& a : t.second.get_assignments()) {
            // End adding movements if batch is collected
            if (rrs.planned_moves_size >= _config.movement_disk_size_batch) {
                return;
            }

            auto ntp = model::ntp(t.first.ns, t.first.tp, a.id);
            if (rrs.moving_partitions.contains(ntp)) {
                continue;
            }

            std::vector<model::broker_shard> stable_replicas;
            for (const auto& bs : a.replicas) {
                if (!rrs.unavailable_nodes.contains(bs.node_id)) {
                    stable_replicas.push_back(bs);
                }
            }

            if (stable_replicas.size() == a.replicas.size()) {
                continue;
            }

            auto partition_size = get_partition_size(ntp, rrs);
            if (
              !partition_size.has_value()
              || !is_partition_movement_possible(a.replicas, rrs)) {
                result.failed_reassignments_count += 1;
                continue;
            }

            auto constraints = get_partition_constraints(
              a,
              t.second.metadata,
              partition_size.value(),
              _config.hard_max_disk_usage_ratio,
              rrs);

            auto new_allocation_units = get_reallocation(
              ntp,
              a,
              partition_size.value(),
              std::move(constraints),
              stable_replicas,
              rrs);
            if (new_allocation_units) {
                result.reassignments.emplace_back(ntp_reassignments{
                  .ntp = ntp,
                  .allocation_units = std::move(new_allocation_units.value())});
            } else {
                result.failed_reassignments_count += 1;
            }
        }
    }
}

/*
 * Function is trying to move ntps out of node that are violating
 * soft_max_disk_usage_ratio. It takes nodes in reverse used space ratio order.
 * For each node it is trying to collect set of partitions to move. Partitions
 * are selected in ascending order of their size.
 *
 * If more than one replica in a group is on a node violating disk usage
 * constraints, we try to reallocate all such replicas. But if a reallocation
 * request fails, we retry while leaving some of these replicas intact.
 */
void partition_balancer_planner::get_full_node_reassignments(
  plan_data& result, reallocation_request_state& rrs) {
    std::vector<const node_disk_space*> sorted_full_nodes;
    for (const auto& kv : rrs.node_disk_reports) {
        const auto* node_disk = &kv.second;
        if (node_disk->final_used_ratio() > _config.soft_max_disk_usage_ratio) {
            sorted_full_nodes.push_back(node_disk);
        }
    }
    std::sort(
      sorted_full_nodes.begin(),
      sorted_full_nodes.end(),
      [](const auto* lhs, const auto* rhs) {
          return lhs->final_used_ratio() > rhs->final_used_ratio();
      });

    if (sorted_full_nodes.empty()) {
        return;
    }

    absl::flat_hash_map<model::node_id, std::vector<model::ntp>> ntp_on_nodes;
    for (const auto& t : _topic_table.topics_map()) {
        for (const auto& a : t.second.get_assignments()) {
            for (const auto& r : a.replicas) {
                ntp_on_nodes[r.node_id].emplace_back(
                  t.first.ns, t.first.tp, a.id);
            }
        }
    }

    for (const auto* node_disk : sorted_full_nodes) {
        if (rrs.planned_moves_size >= _config.movement_disk_size_batch) {
            return;
        }

        absl::btree_multimap<size_t, model::ntp> ntp_on_node_sizes;
        for (const auto& ntp : ntp_on_nodes[node_disk->node_id]) {
            auto partition_size_opt = get_partition_size(ntp, rrs);
            if (partition_size_opt.has_value()) {
                ntp_on_node_sizes.emplace(partition_size_opt.value(), ntp);
            } else {
                result.failed_reassignments_count += 1;
            }
        }

        auto ntp_size_it = ntp_on_node_sizes.begin();
        while (node_disk->final_used_ratio() > _config.soft_max_disk_usage_ratio
               && ntp_size_it != ntp_on_node_sizes.end()) {
            if (rrs.planned_moves_size >= _config.movement_disk_size_batch) {
                return;
            }

            const auto& partition_to_move = ntp_size_it->second;
            if (rrs.moving_partitions.contains(partition_to_move)) {
                ntp_size_it++;
                continue;
            }

            const auto& topic_metadata = _topic_table.topics_map().at(
              model::topic_namespace_view(partition_to_move));
            const auto& current_assignments
              = topic_metadata.get_assignments().find(
                partition_to_move.tp.partition);

            if (!is_partition_movement_possible(
                  current_assignments->replicas, rrs)) {
                result.failed_reassignments_count += 1;
                ntp_size_it++;
                continue;
            }

            auto constraints = get_partition_constraints(
              *current_assignments,
              topic_metadata.metadata,
              ntp_size_it->first,
              _config.soft_max_disk_usage_ratio,
              rrs);

            struct full_node_replica {
                model::broker_shard bs;
                node_disk_space disk;
            };
            std::vector<full_node_replica> full_node_replicas;
            std::vector<model::broker_shard> stable_replicas;

            for (const auto& r : current_assignments->replicas) {
                if (rrs.unavailable_nodes.contains(r.node_id)) {
                    continue;
                }

                auto disk_it = rrs.node_disk_reports.find(r.node_id);
                vassert(
                  disk_it != rrs.node_disk_reports.end(),
                  "disk report for node {} must be present",
                  r.node_id);
                const auto& disk = disk_it->second;

                if (
                  disk.final_used_ratio() < _config.soft_max_disk_usage_ratio) {
                    stable_replicas.push_back(r);
                } else {
                    full_node_replicas.push_back(full_node_replica{
                      .bs = r,
                      .disk = disk,
                    });
                }
            }

            // We start with a small set of stable replicas that are on "good"
            // nodes and try to find a reallocation. If that fails, we add one
            // replica from the set of full_node_replicas (starting from the
            // least full) to stable replicas and retry until we get a valid
            // reallocation.
            std::sort(
              full_node_replicas.begin(),
              full_node_replicas.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.disk.final_used_ratio()
                         < rhs.disk.final_used_ratio();
              });

            bool success = false;
            for (const auto& replica : full_node_replicas) {
                auto new_allocation_units = get_reallocation(
                  partition_to_move,
                  *current_assignments,
                  ntp_size_it->first,
                  constraints,
                  stable_replicas,
                  rrs);

                if (new_allocation_units) {
                    result.reassignments.emplace_back(ntp_reassignments{
                      .ntp = partition_to_move,
                      .allocation_units = std::move(
                        new_allocation_units.value())});
                    success = true;
                    break;
                } else {
                    stable_replicas.push_back(replica.bs);
                }
            }
            if (!success) {
                result.failed_reassignments_count += 1;
            }

            ntp_size_it++;
        }
    }
}

/*
 * Cancel movement if new assignments contains unavailble node
 * and previous replica set doesn't contain this node
 */
void partition_balancer_planner::get_unavailable_node_movement_cancellations(
  std::vector<model::ntp>& cancellations,
  const reallocation_request_state& rrs) {
    for (const auto& update : _topic_table.updates_in_progress()) {
        if (
          update.second.state
          != topic_table::in_progress_state::update_requested) {
            continue;
        }

        absl::flat_hash_set<model::node_id> previous_replicas_set;
        for (const auto& r : update.second.previous_replicas) {
            previous_replicas_set.insert(r.node_id);
        }

        auto current_assignments = _topic_table.get_partition_assignment(
          update.first);
        if (!current_assignments.has_value()) {
            continue;
        }
        for (const auto& r : current_assignments->replicas) {
            if (
              rrs.unavailable_nodes.contains(r.node_id)
              && !previous_replicas_set.contains(r.node_id)) {
                cancellations.push_back(update.first);
                continue;
            }
        }
    }
}

partition_balancer_planner::plan_data
partition_balancer_planner::plan_reassignments(
  const cluster_health_report& health_report,
  const std::vector<raft::follower_metrics>& follower_metrics) {
    reallocation_request_state rrs;
    plan_data result;

    init_node_disk_reports_from_health_report(health_report, rrs);
    calculate_unavailable_nodes(follower_metrics, rrs, result);
    calculate_nodes_with_disk_constraints_violation(rrs, result);

    if (_topic_table.has_updates_in_progress()) {
        get_unavailable_node_movement_cancellations(result.cancellations, rrs);
        if (!result.cancellations.empty()) {
            result.status
              = partition_balancer_planner::status::cancellations_planned;
        }
        return result;
    }

    if (!all_reports_received(rrs)) {
        result.status = partition_balancer_planner::status::waiting_for_reports;
        return result;
    }

    if (
      !_topic_table.has_updates_in_progress()
      && !result.violations.is_empty()) {
        init_ntp_sizes_from_health_report(health_report, rrs);
        get_unavailable_nodes_reassignments(result, rrs);
        get_full_node_reassignments(result, rrs);
        if (!result.reassignments.empty()) {
            result.status
              = partition_balancer_planner::status::movement_planned;
        }

        return result;
    }

    return result;
}

} // namespace cluster
