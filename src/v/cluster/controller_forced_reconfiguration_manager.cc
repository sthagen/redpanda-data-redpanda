/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/controller_forced_reconfiguration_manager.h"

#include "cluster/controller.h"
#include "cluster/members_frontend.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"

#include <exception>
#include <expected>
#include <ranges>
#include <system_error>

namespace cluster {

controller_forced_reconfiguration_manager::
  controller_forced_reconfiguration_manager(controller* controller) noexcept
  : _controller_ptr(controller) {}

ss::future<std::error_code>
controller_forced_reconfiguration_manager::start(ss::abort_source& shard0_as) {
    _as_sub = shard0_as.subscribe(
      [this]() noexcept { this->_abort_source.request_abort(); });
    return ss::make_ready_future<std::error_code>(cluster::errc::success);
}

ss::future<> controller_forced_reconfiguration_manager::stop() noexcept {
    _abort_source.request_abort();
    _execution_lock.broken();
    co_await _gate.close();
}

auto controller_forced_reconfiguration_manager::gather_holders()
  -> std::expected<holder_bundle, cluster::error_info> {
    if (_abort_source.abort_requested()) {
        return std::unexpected{cluster::error_info{
          .err = cluster::errc::shutting_down, .message = "shutting down"}};
    }

    // hold gate
    auto maybe_gate_holder = _gate.try_hold();
    if (!maybe_gate_holder) {
        return std::unexpected{cluster::error_info{
          .err = cluster::errc::shutting_down, .message = "shutting down"}};
    }
    auto gate_holder = *std::move(maybe_gate_holder);

    // hold lock
    auto maybe_lock_holder = _execution_lock.try_get_units();

    if (!maybe_lock_holder) {
        return std::unexpected{cluster::error_info{
          .err = cluster::errc::concurrent_modification_error,
          .message
          = "controller forced reconfiguration operation already in flight"}};
    }

    return holder_bundle{
      .gate_holder = std::move(gate_holder),
      .lock_holder = std::move(*maybe_lock_holder),
    };
}

std::expected<std::vector<raft::vnode>, cluster::error_info>
controller_forced_reconfiguration_manager::calculation_reconfiguration_group(
  const std::vector<model::node_id>& dead_nodes,
  std::vector<raft::vnode> current_voters,
  uint16_t surviving_node_size) {
    const auto reconfiguration_group_size = surviving_node_size * 2 - 1;
    if (
      current_voters.size() < static_cast<size_t>(reconfiguration_group_size)) {
        return std::unexpected{cluster::error_info{
          .err = cluster::errc::invalid_request,
          .message = fmt::format(
            "not enough voters in the raft configuration to force "
            "reconfigure. surviving_node_count: {}, required voter group "
            "size: {}, available voter group size: {}",
            surviving_node_size,
            reconfiguration_group_size,
            current_voters.size())}};
    }

    auto dead_section = std::ranges::partition(
      current_voters, [&dead_nodes](auto old_voter) {
          const bool is_alive = std::ranges::find(dead_nodes, old_voter.id())
                                == dead_nodes.end();
          if (!is_alive) {
              vlog(
                clusterlog.info,
                "Force dropping node_id: {}, from voters",
                old_voter.id());
          }
          return is_alive;
      });

    auto living_section = std::ranges::subrange{
      current_voters.begin(), current_voters.end() - dead_section.size()};

    if (living_section.size() == 0) {
        vlog(
          clusterlog.info, "cannot force reconfigure to a voter pool of zero");
        return std::unexpected(
          cluster::error_info{
            .err = cluster::errc::invalid_request,
            .message = "cannot force reconfigure to a voter pool of zero"});
    }

    vlog(
      clusterlog.info,
      "found surviving voters {}",
      std::vector<raft::vnode>{std::from_range, living_section});

    std::ranges::sort(
      dead_section, [](const raft::vnode& first, const raft::vnode& second) {
          return first.id() < second.id();
      });

    current_voters.resize(surviving_node_size * 2 - 1);

    return current_voters;
}

ss::future<cluster::error_info> controller_forced_reconfiguration_manager::
  initialize_controller_forced_reconfiguration(
    std::vector<model::node_id> dead_nodes, uint16_t surviving_node_count) {
    // constant
    const auto timeout = std::chrono::minutes{15};

    vlog(
      clusterlog.info,
      "invocation of controller_forced_reconfiguration with dead_nodes: {}, "
      "and surviving quorum size of: {}",
      dead_nodes,
      surviving_node_count);

    auto maybe_holders = gather_holders();
    if (!maybe_holders.has_value()) {
        co_return std::move(maybe_holders).error();
    }

    vassert(
      _controller_ptr, "illegal state, _controller_ptr should never be null");
    auto& controller = *_controller_ptr;

    // step 0.0, check that recovery mode is active
    if (!config::node().recovery_mode_enabled()) {
        co_return cluster::error_info{
          .err = cluster::errc::invalid_request,
          .message = "recovery mode is a precondition for controller force "
                     "recovery, try "
                     "rebooting the broker into recovery mode?"};
    }

    {
        // step 0.5, check that there is no raft0 leader
        const auto maybe_leader = controller._raft0->get_leader_id();
        if (maybe_leader) {
            co_return cluster::error_info{
              .err = cluster::errc::invalid_request,
              .message = fmt::format(
                "controller forced recovery is a last resort, use the "
                "existing controller leader: {} to repair the cluster with "
                "node-wise recovery",
                *maybe_leader)};
        }
    }

    { // step 1, forcibly reconfigure raft0
        vlog(clusterlog.info, "starting raft0 force reconfiguration");
        const auto& current_group_configuration = controller._raft0->config();
        const auto& current_node_configuration
          = controller._raft0->config().current_config();

        std::vector<raft::vnode> reconfiguration_learner_set{
          current_node_configuration.learners};

        auto maybe_reconfiguration_voter_set
          = calculation_reconfiguration_group(
            dead_nodes,
            current_node_configuration.voters,
            surviving_node_count);

        if (!maybe_reconfiguration_voter_set.has_value()) {
            co_return maybe_reconfiguration_voter_set.error();
        }
        auto reconfiguration_voter_set = std::move(
          *maybe_reconfiguration_voter_set);

        std::erase_if(
          reconfiguration_learner_set, [&dead_nodes](auto old_learner) {
              const bool erasing = std::ranges::find(
                                     dead_nodes, old_learner.id())
                                   != dead_nodes.end();
              if (erasing) {
                  vlog(
                    clusterlog.info,
                    "Force dropping node_id: {}, from learners",
                    old_learner.id());
              }
              return erasing;
          });

        {
            const auto new_revision_id = model::revision_id{
              static_cast<int64_t>(
                ++current_group_configuration.revision_id())};
            vlog(
              clusterlog.info,
              "force reconfiguring raft0 to voter group: {}",
              reconfiguration_voter_set);
            const auto ec
              = co_await controller._raft0->force_replace_configuration_locally(
                std::move(reconfiguration_voter_set),
                std::move(reconfiguration_learner_set),
                new_revision_id);
            if (ec) {
                co_return cluster::error_info{
                  .err = ec,
                  .message
                  = "controller force reconfiguration failed on raft force "
                    "reconfiguration"};
            }
        }
        vlog(clusterlog.info, "finished raft0 force reconfiguration");
    }

    { // step 2, wait for a new leader and branch on leadership
        vlog(clusterlog.info, "starting raft0 leadership wait");
        uint64_t leadership_wait_counter{0};
        model::node_id leader{};

        while (true) {
            ++leadership_wait_counter;

            vlog(
              clusterlog.info,
              "waiting for cluster leader iteration: {}",
              leadership_wait_counter);

            auto maybe_leader = controller._raft0->get_leader_id();

            // case not leader, loop
            if (!maybe_leader) {
                try {
                    co_await ss::sleep_abortable(
                      std::chrono::seconds{5}, _abort_source);
                } catch (const ss::sleep_aborted& e) {
                    co_return cluster::error_info{
                      .err = cluster::errc::shutting_down,
                      .message = "shutdown on waiting for controller leader"};
                }
                continue;
            }

            // leader determined
            leader = *maybe_leader;
            break;
        }

        vlog(
          clusterlog.info,
          "interim controller leader elected with id: {}",
          leader);
        if (leader != controller.self()) {
            co_return error_info{
              .err = cluster::errc::success,
              .message = "exiting controller forced recovery as follower"};
        }
        vlog(
          clusterlog.info,
          "finished raft0 leadership wait with interim leader: {}",
          leader);
    }

    { // step 3 force all partitions out of failed brokers
        vlog(clusterlog.info, "starting all partition force recovery");
        auto maybe_quorum_loss_partitions
          = co_await controller.get_topics_frontend()
              .local()
              .partitions_with_lost_majority(dead_nodes);
        if (!maybe_quorum_loss_partitions) {
            co_return cluster::error_info{
              .err = maybe_quorum_loss_partitions.error(),
              .message
              = "failed to gather list of partitions with quorum loss"};
        }
        auto& quorum_loss_partitions = maybe_quorum_loss_partitions.value();

        // todo find reasonable timeout
        auto ec = co_await controller.get_topics_frontend()
                    .local()
                    .force_recover_partitions_from_nodes(
                      dead_nodes,
                      std::move(quorum_loss_partitions),
                      ss::lowres_clock::now() + timeout);

        if (ec) {
            co_return cluster::error_info{
              .err = ec,
              .message
              = "controller force reconfiguration failed on force recovering "
                "partitions"};
        }
        vlog(clusterlog.info, "finished all partition force recovery");
    }

    // we succeed the CFR even if some nodes fail to decommission or remove
    std::vector<model::node_id> failed_to_decom_nodes{};
    std::error_code last_decom_error{};
    { // step 4 decommission and remove all dead brokers
        vlog(clusterlog.info, "starting dead node decommission");
        for (const auto dead_node : dead_nodes) {
            const auto ec = co_await controller.get_members_frontend()
                              .local()
                              .decommission_node(dead_node);
            if (ec) {
                last_decom_error = ec;
                failed_to_decom_nodes.emplace_back(dead_node);
                vlog(
                  clusterlog.warn,
                  "controller force reconfiguration failed to "
                  "decommission "
                  "node: {}, with error: {}",
                  dead_node,
                  ec);
                continue;
            }
        }
        vlog(clusterlog.info, "finished dead node decommission");

        if (!failed_to_decom_nodes.empty()) {
            co_return cluster::error_info{
              .err = last_decom_error,
              .message = fmt::format(
                "leader exiting, failed to decomission nodes: {}. If survivors "
                "nodes are stable, decommission these brokers normally",
                failed_to_decom_nodes)};
        }
    }
    co_return cluster::error_info{
      .err = cluster::errc::success,
      .message = "leader exiting successful controller forced recovery"};
}
} // namespace cluster
