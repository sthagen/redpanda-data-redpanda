/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/shard_placement_table.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/topic_table.h"
#include "ssx/async_algorithm.h"

namespace cluster {

std::ostream&
operator<<(std::ostream& o, shard_placement_table::hosted_status s) {
    switch (s) {
    case shard_placement_table::hosted_status::receiving:
        return o << "receiving";
    case shard_placement_table::hosted_status::hosted:
        return o << "hosted";
    case shard_placement_table::hosted_status::obsolete:
        return o << "obsolete";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(
  std::ostream& o, const shard_placement_table::shard_local_state& ls) {
    fmt::print(
      o, "{{log_revision: {} status: {}}}", ls.log_revision, ls.status);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const shard_placement_table::placement_state& gs) {
    fmt::print(
      o,
      "{{local: {}, target: {}, shard_revision: {}, "
      "is_initial_at_revision: {}, next: {}}}",
      gs.local,
      gs.target,
      gs.shard_revision,
      gs._is_initial_at_revision,
      gs._next);
    return o;
}

ss::future<> shard_placement_table::initialize(
  const topic_table& topics, model::node_id self) {
    // We expect topic_table to remain unchanged throughout the loop because the
    // method is supposed to be called after local controller replay is finished
    // but before we start getting new controller updates from the leader.
    auto tt_version = topics.topics_map_revision();
    ssx::async_counter counter;
    for (const auto& [ns_tp, md_item] : topics.all_topics_metadata()) {
        vassert(
          tt_version == topics.topics_map_revision(),
          "topic_table unexpectedly changed");

        co_await ssx::async_for_each_counter(
          counter,
          md_item.get_assignments().begin(),
          md_item.get_assignments().end(),
          [&](const partition_assignment& p_as) {
              vassert(
                tt_version == topics.topics_map_revision(),
                "topic_table unexpectedly changed");

              model::ntp ntp{ns_tp.ns, ns_tp.tp, p_as.id};
              auto replicas_view = topics.get_replicas_view(ntp, md_item, p_as);
              auto target = placement_target_on_node(replicas_view, self);
              if (!target) {
                  return;
              }

              auto shard_rev = model::shard_revision_id{
                replicas_view.last_cmd_revision()};

              if (ss::this_shard_id() == assignment_shard_id) {
                  _ntp2target.emplace(ntp, target.value());
              }

              // We add an initial hosted marker for the partition on the shard
              // from the original replica set (even in the case of cross-shard
              // move). The reason for this is that if there is an ongoing
              // cross-shard move, we can't be sure if it was done before the
              // previous shutdown or not, so during reconciliation we'll first
              // look for kvstore state on the original shard, and, if there is
              // none (meaning that the update was finished previously), use the
              // state on the destination shard.

              auto orig_shard = find_shard_on_node(
                replicas_view.orig_replicas(), self);

              if (ss::this_shard_id() == target->shard) {
                  vlog(
                    clusterlog.info,
                    "expecting partition {} with log revision {} on this shard "
                    "(original shard {})",
                    ntp,
                    target->log_revision,
                    orig_shard);
              }

              auto placement = placement_state(*target, shard_rev);

              if (orig_shard && target->shard != orig_shard) {
                  // cross-shard transfer, orig_shard gets the hosted marker
                  if (ss::this_shard_id() == orig_shard) {
                      placement.local = shard_local_state::initial(
                        target->log_revision);
                      _states.emplace(ntp, placement);
                  } else if (ss::this_shard_id() == target->shard) {
                      _states.emplace(ntp, placement);
                  }
              } else if (ss::this_shard_id() == target->shard) {
                  // in other cases target shard gets the hosted marker
                  placement.local = shard_local_state::initial(
                    target->log_revision);
                  _states.emplace(ntp, placement);
              }
          });
    }
}

ss::future<> shard_placement_table::set_target(
  const model::ntp& ntp,
  std::optional<shard_placement_target> target,
  model::shard_revision_id shard_rev,
  shard_callback_t shard_callback) {
    vassert(
      ss::this_shard_id() == assignment_shard_id,
      "method can only be invoked on shard {}",
      assignment_shard_id);

    auto units = co_await _mtx.get_units();

    bool is_initial = false;
    if (target) {
        auto [it, inserted] = _ntp2target.try_emplace(ntp, *target);
        if (inserted) {
            vlog(
              clusterlog.trace,
              "[{}] insert target: {}, shard_rev: {}",
              ntp,
              target,
              shard_rev);
            is_initial = true;
        } else {
            if (it->second == *target) {
                vlog(
                  clusterlog.trace,
                  "[{}] modify target no-op, cur: {}, shard_rev: {}",
                  ntp,
                  it->second,
                  shard_rev);
                co_return;
            }

            vlog(
              clusterlog.trace,
              "[{}] modify target: {} -> {}, shard_rev: {}",
              ntp,
              it->second,
              target,
              shard_rev);
            is_initial = it->second.log_revision != target->log_revision;
            it->second = *target;
        }
    } else {
        auto prev_it = _ntp2target.find(ntp);
        if (prev_it == _ntp2target.end()) {
            vlog(
              clusterlog.trace,
              "[{}] remove target no-op, shard_rev: {}",
              ntp,
              shard_rev);
            co_return;
        }

        vlog(
          clusterlog.trace,
          "[{}] remove target: {}, shard_rev: {}",
          ntp,
          prev_it->second,
          shard_rev);
        _ntp2target.erase(prev_it);
    }

    co_await container().invoke_on_all(
      [&ntp, target, shard_rev, is_initial, shard_callback](
        shard_placement_table& other) {
          other.set_target_on_this_shard(
            ntp, target, shard_rev, is_initial, shard_callback);
      });
}

void shard_placement_table::set_target_on_this_shard(
  const model::ntp& ntp,
  std::optional<shard_placement_target> target,
  model::shard_revision_id shard_rev,
  bool is_initial,
  shard_callback_t shard_callback) {
    vlog(
      clusterlog.trace,
      "[{}] setting target on this shard: {}, rev: {}",
      ntp,
      target,
      shard_rev);

    if (target && target->shard == ss::this_shard_id()) {
        auto& state = _states.emplace(ntp, placement_state{}).first->second;
        state.shard_revision = shard_rev;
        state.target = target;
        if (is_initial) {
            state._is_initial_at_revision = shard_rev;
        }
    } else {
        // modify only if already present
        auto it = _states.find(ntp);
        if (it == _states.end()) {
            return;
        }

        if (!it->second.local) {
            // We are on a shard that was previously a target, but didn't get to
            // starting the transfer.
            _states.erase(it);
        } else {
            it->second.shard_revision = shard_rev;
            it->second.target = target;
        }
    }

    // Notify the caller that something has changed on this shard.
    shard_callback(ntp, shard_rev);
}

std::optional<shard_placement_table::placement_state>
shard_placement_table::state_on_this_shard(const model::ntp& ntp) const {
    auto it = _states.find(ntp);
    if (it != _states.end()) {
        return it->second;
    }
    return std::nullopt;
}

ss::future<std::error_code> shard_placement_table::prepare_create(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    auto state_it = _states.find(ntp);
    vassert(state_it != _states.end(), "[{}] expected state", ntp);
    auto& state = state_it->second;
    vassert(
      state.target && state.target->log_revision == expected_log_rev
        && state.target->shard == ss::this_shard_id(),
      "[{}] unexpected target: {} (expected log revision: {})",
      ntp,
      state.target,
      expected_log_rev);

    if (
      state.local && state.local->log_revision != state.target->log_revision) {
        // wait until partition with obsolete log revision is removed
        co_return errc::waiting_for_reconfiguration_finish;
    }

    if (!state.local) {
        if (state._is_initial_at_revision == state.shard_revision) {
            state.local = shard_local_state::initial(
              state.target->log_revision);
        } else {
            // x-shard transfer hasn't started yet, wait for it.
            co_return errc::waiting_for_partition_shutdown;
        }
    }

    if (state.local->status != hosted_status::hosted) {
        // x-shard transfer is in progress, wait for it to end.
        co_return errc::waiting_for_partition_shutdown;
    }

    // ready to create
    co_return errc::success;
}

ss::future<result<ss::shard_id>> shard_placement_table::prepare_transfer(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    auto state_it = _states.find(ntp);
    vassert(state_it != _states.end(), "[{}] expected state", ntp);
    auto& state = state_it->second;

    if (!state.local || state.local->status == hosted_status::receiving) {
        // This shard needs to transfer partition state somewhere else, but
        // haven't yet received it itself. Wait for it.
        co_return errc::waiting_for_partition_shutdown;
    }

    if (state.local->status == hosted_status::obsolete) {
        // Previous finish_transfer_on_source() failed? Retry it.
        co_await do_delete(ntp, state);
        co_return errc::success;
    }

    vassert(
      state.local->log_revision == expected_log_rev
        && state.local->status == hosted_status::hosted,
      "[{}] unexpected local state: {} (expected log revision: {})",
      ntp,
      state.local,
      expected_log_rev);

    if (!state._next) {
        vassert(
          state.target && state.target->log_revision == expected_log_rev
            && state.target->shard != ss::this_shard_id(),
          "[{}] unexpected target: {} (expected log revision: {})",
          ntp,
          state.target,
          expected_log_rev);

        auto destination = state.target->shard;
        // check if destination is ready
        auto ec = co_await container().invoke_on(
          destination, [&ntp, expected_log_rev](shard_placement_table& dest) {
              auto& dest_state
                = dest._states.emplace(ntp, placement_state{}).first->second;
              if (!dest_state.local) {
                  // at this point we commit to the transfer on the
                  // destination shard
                  dest_state.local = shard_local_state::receiving(
                    expected_log_rev);
                  return errc::success;
              }

              if (dest_state.local->log_revision != expected_log_rev) {
                  // someone has to delete obsolete log revision first
                  return errc::waiting_for_reconfiguration_finish;
              }

              if (dest_state.local->status != hosted_status::receiving) {
                  // probably still finishing a previous transfer to this
                  // shard and we are already trying to transfer it back.
                  return errc::waiting_for_partition_shutdown;
              }

              // transfer still in progress, we must retry it.
              return errc::success;
          });

        if (ec != errc::success) {
            co_return ec;
        }

        // at this point we commit to the transfer on the source shard
        state._next = destination;
    }

    co_return state._next.value();
}

ss::future<> shard_placement_table::finish_transfer_on_destination(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    auto it = _states.find(ntp);
    if (it == _states.end()) {
        co_return;
    }
    auto& state = it->second;
    if (state.local && state.local->log_revision == expected_log_rev) {
        vassert(
          state.local->status == hosted_status::receiving,
          "[{}] unexpected local status, current: {}",
          ntp,
          it->second.local);
        it->second.local->status = hosted_status::hosted;
    }
    vlog(
      clusterlog.trace,
      "[{}] finished transfer on destination, placement: {}",
      ntp,
      state);
}

ss::future<> shard_placement_table::finish_transfer_on_source(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    auto it = _states.find(ntp);
    vassert(it != _states.end(), "[{}] expected state", ntp);
    auto& state = it->second;
    vassert(
      state.local && state.local->log_revision == expected_log_rev,
      "[{}] unexpected current: {} (expected log revision: {})",
      ntp,
      state.local,
      expected_log_rev);

    co_await do_delete(ntp, state);
}

ss::future<std::error_code> shard_placement_table::finish_delete(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    auto it = _states.find(ntp);
    vassert(it != _states.end(), "[{}] expected state", ntp);
    auto& state = it->second;
    vassert(
      state.local && state.local->log_revision == expected_log_rev,
      "[{}] unexpected current: {} (expected log revision: {})",
      ntp,
      state.local,
      expected_log_rev);

    if (state.local->status == hosted_status::receiving) {
        // If transfer to this shard is still in progress, we'll wait for the
        // source shard to finish or cancel it before deleting.
        co_return errc::waiting_for_partition_shutdown;
    }

    if (state._next) {
        // notify destination shard that the transfer won't finish
        co_await container().invoke_on(
          state._next.value(),
          [&ntp, expected_log_rev](shard_placement_table& dest) {
              auto it = dest._states.find(ntp);
              if (
                it != dest._states.end() && it->second.local
                && it->second.local->log_revision == expected_log_rev
                && it->second.local->status == hosted_status::receiving) {
                  it->second.local->status = hosted_status::obsolete;
              }

              // TODO: notify reconciliation fiber
          });
    }

    co_await do_delete(ntp, state);
    co_return errc::success;
}

ss::future<> shard_placement_table::do_delete(
  const model::ntp& ntp, placement_state& state) {
    state._next = std::nullopt;
    // TODO: set obsolete, delete kvstore state etc.
    state.local = std::nullopt;
    if (!state.target || (state.target->shard != ss::this_shard_id())) {
        _states.erase(ntp);
    }
    co_return;
}

} // namespace cluster
