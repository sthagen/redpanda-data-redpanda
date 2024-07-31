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
#include "cluster/data_migration_backend.h"

#include "cluster/partition_leaders_table.h"
#include "config/node_config.h"
#include "data_migration_frontend.h"
#include "data_migration_types.h"
#include "data_migration_worker.h"
#include "fwd.h"
#include "logger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "ssx/async_algorithm.h"
#include "ssx/future-util.h"
#include "types.h"

#include <seastar/core/abort_source.hh>

#include <chrono>
#include <optional>
#include <ranges>

using namespace std::chrono_literals;

namespace cluster::data_migrations {

backend::backend(
  migrations_table& table,
  frontend& frontend,
  ss::sharded<worker>& worker,
  partition_leaders_table& leaders_table,
  topic_table& topic_table,
  shard_table& shard_table,
  ss::abort_source& as)
  : _self(*config::node().node_id())
  , _table(table)
  , _frontend(frontend)
  , _worker(worker)
  , _leaders_table(leaders_table)
  , _topic_table(topic_table)
  , _shard_table(shard_table)
  , _as(as) {}

void backend::start() {
    vassert(
      ss::this_shard_id() == data_migrations_shard, "Called on wrong shard");

    _is_raft0_leader = _is_coordinator
      = _self == _leaders_table.get_leader(model::controller_ntp);
    _plt_raft0_leadership_notification_id
      = _leaders_table.register_leadership_change_notification(
        model::controller_ntp,
        [this](model::ntp, model::term_id, model::node_id leader_node_id) {
            _is_raft0_leader = leader_node_id == _self;
            if (_is_raft0_leader != _is_coordinator) {
                ssx::spawn_with_gate(
                  _gate, [this]() { return handle_raft0_leadership_update(); });
            }
        });
    _table_notification_id = _table.register_notification([this](id id) {
        ssx::spawn_with_gate(
          _gate, [this, id]() { return handle_migration_update(id); });
    });
    _topic_table_notification_id = _topic_table.register_delta_notification(
      [this](topic_table::delta_range_t deltas) {
          _unprocessed_deltas.reserve(
            _unprocessed_deltas.size() + deltas.size());
          for (const auto& delta : deltas) {
              _unprocessed_deltas.push_back(delta);
          }
          wakeup();
      });

    _shard_notification_id = _shard_table.register_notification(
      [this](
        const model::ntp& ntp,
        raft::group_id g,
        std::optional<ss::shard_id> shard) {
          handle_shard_update(ntp, g, shard);
      });

    ssx::repeat_until_gate_closed(_gate, [this]() { return loop_once(); });
}

ss::future<> backend::stop() {
    _mutex.broken();
    _sem.broken();
    _timer.cancel();
    _shard_table.unregister_delta_notification(_shard_notification_id);
    _topic_table.unregister_delta_notification(_topic_table_notification_id);
    _leaders_table.unregister_leadership_change_notification(
      model::controller_ntp, _plt_raft0_leadership_notification_id);
    _table.unregister_notification(_table_notification_id);
    co_await _worker.invoke_on_all(&worker::stop);
    co_await _gate.close();
    vlog(dm_log.debug, "backend stopped");
}

ss::future<> backend::loop_once() {
    {
        auto units = co_await _mutex.get_units(_as);
        co_await work_once();
    }
    co_await _sem.wait(_as);
    _sem.consume(_sem.available_units());
}

ss::future<> backend::work_once() {
    // process pending deltas
    auto unprocessed_deltas = std::move(_unprocessed_deltas);
    for (auto&& delta : unprocessed_deltas) {
        co_await process_delta(std::move(delta));
    }

    // process RPC responses
    auto rpc_responses = std::move(_rpc_responses);
    for (const auto& [node_id, response] : rpc_responses) {
        co_await ssx::async_for_each(
          response.actual_states, [this](const auto& ntp_resp) {
              if (auto rs_it = get_rstate(ntp_resp.migration, ntp_resp.state)) {
                  mark_migration_step_done_for_ntp(
                    (*rs_it)->second, ntp_resp.ntp);
                  to_advance_if_done(*rs_it);
              }
          });
    }

    // process topic work results
    auto topic_work_results = std::move(_topic_work_results);
    chunked_vector<model::topic_namespace> retriable_topic_work;
    co_await ssx::async_for_each(
      topic_work_results, [this, &retriable_topic_work](auto& result) {
          if (auto rs_it = get_rstate(result.migration, result.sought_state)) {
              switch (result.ec) {
              case errc::success:
                  mark_migration_step_done_for_nt((*rs_it)->second, result.nt);
                  to_advance_if_done(*rs_it);
                  break;
              case errc::shutting_down:
                  break;
              default:
                  // any other errors deemed retryable
                  vlog(
                    dm_log.info,
                    "as part of migration {}, topic work for moving nt {} to "
                    "state "
                    "{} returned {}, retrying",
                    result.migration,
                    result.nt,
                    result.sought_state,
                    result.ec);
                  retriable_topic_work.push_back(std::move(result.nt));
              }
          }
      });

    auto next_tick = model::timeout_clock::time_point::max();

    // prepare RPC and topic work requests
    auto now = model::timeout_clock::now();
    chunked_vector<model::node_id> to_send_rpc;
    for (const auto& [node_id, deadline] : _nodes_to_retry) {
        if (deadline <= now) {
            to_send_rpc.push_back(node_id);
        } else {
            next_tick = std::min(deadline, next_tick);
        }
    }
    chunked_vector<model::topic_namespace> to_schedule_topic_work;
    co_await ssx::async_for_each(
      _topic_work_to_retry,
      [&to_schedule_topic_work, &next_tick, now](const auto& entry) {
          const auto& [nt, deadline] = entry;
          if (deadline <= now) {
              to_schedule_topic_work.push_back(nt);
          } else {
              next_tick = std::min(deadline, next_tick);
          }
      });

    // defer RPC retries and topic work
    // todo: configure timeout
    auto new_deadline = now + 5s;
    for (const auto& node_id : rpc_responses | std::views::keys) {
        if (_node_states.contains(node_id)) {
            _nodes_to_retry.try_emplace(node_id, new_deadline);
            next_tick = std::min(next_tick, new_deadline);
        }
    }
    co_await ssx::async_for_each(
      retriable_topic_work, [this, &next_tick, new_deadline](const auto& nt) {
          auto it = _topic_migration_map.find(nt);
          if (it == _topic_migration_map.end()) {
              return;
          }
          auto migration_id = it->second;

          auto& mrstate = _migration_states.find(migration_id)->second;
          auto& tstate = mrstate.outstanding_topics[nt];
          if (
            tstate.topic_scoped_work_needed && !tstate.topic_scoped_work_done) {
              _topic_work_to_retry.try_emplace(std::move(nt), new_deadline);
              next_tick = std::min(next_tick, new_deadline);
          }
      });

    // schedule fibers
    for (auto node_id : to_send_rpc) {
        _nodes_to_retry.erase(node_id);
        co_await send_rpc(node_id);
    }
    co_await ssx::async_for_each(
      to_schedule_topic_work, [this](const auto& nt) {
          _topic_work_to_retry.erase(nt);
          return schedule_topic_work(nt);
      });
    spawn_advances();
    if (next_tick == model::timeout_clock::time_point::max()) {
        _timer.cancel();
    } else {
        _timer.rearm(next_tick);
    }
}

void backend::wakeup() { _sem.signal(1 - _sem.available_units()); }

std::optional<backend::migration_reconciliation_states_t::iterator>
backend::get_rstate(id migration, state expected_sought_state) {
    auto rs_it = _migration_states.find(migration);
    if (rs_it == _migration_states.end()) {
        // migration gone, ignore
        return std::nullopt;
    }
    migration_reconciliation_state& rs = rs_it->second;
    if (rs.scope.sought_state > expected_sought_state) {
        // migration advanced since then, ignore
        return std::nullopt;
    }
    return rs_it;
}

void backend::mark_migration_step_done_for_ntp(
  migration_reconciliation_state& rs, const model::ntp& ntp) {
    auto& rs_topics = rs.outstanding_topics;
    auto rs_topic_it = rs_topics.find({ntp.ns, ntp.tp.topic});
    if (rs_topic_it != rs_topics.end()) {
        auto& tstate = rs_topic_it->second;
        auto& rs_parts = tstate.outstanding_partitions;
        auto rs_part_it = rs_parts.find(ntp.tp.partition);
        if (rs_part_it != rs_parts.end()) {
            for (const auto& affected_node_id : rs_part_it->second) {
                auto nstate_it = _node_states.find(affected_node_id);
                nstate_it->second.erase(ntp);
                if (nstate_it->second.empty()) {
                    _node_states.erase(nstate_it);
                    _nodes_to_retry.erase(affected_node_id);
                }
            }
            rs_parts.erase(rs_part_it);
            rs.erase_tstate_if_done(rs_topic_it);
        }
    }
}

void backend::mark_migration_step_done_for_nt(
  migration_reconciliation_state& rs, const model::topic_namespace& nt) {
    auto& rs_topics = rs.outstanding_topics;
    auto rs_topic_it = rs_topics.find(nt);
    if (rs_topic_it != rs_topics.end()) {
        auto& tstate = rs_topic_it->second;
        tstate.topic_scoped_work_done = true;
        rs.erase_tstate_if_done(rs_topic_it);
    }
}

void backend::migration_reconciliation_state::erase_tstate_if_done(
  topic_map_t::iterator it) {
    auto& tstate = it->second;
    if (
      tstate.outstanding_partitions.empty()
      && (!tstate.topic_scoped_work_needed || tstate.topic_scoped_work_done)) {
        outstanding_topics.erase(it);
    }
}

ss::future<> backend::send_rpc(model::node_id node_id) {
    check_ntp_states_request req;
    co_await ssx::async_for_each(
      _node_states[node_id], [this, &req](const auto& pair) {
          auto& [ntp, migration_id] = pair;
          req.sought_states.push_back(
            {.ntp = ntp,
             .migration = migration_id,
             .state = *_migration_states.find(migration_id)
                         ->second.scope.sought_state});
      });

    ssx::spawn_with_gate(
      _gate, [this, node_id, req = std::move(req)]() mutable {
          vlog(dm_log.debug, "sending RPC to node {}: {}", node_id, req);
          ss::future<check_ntp_states_reply> reply
            = (_self == node_id) ? check_ntp_states_locally(std::move(req))
                                 : _frontend.check_ntp_states_on_foreign_node(
                                   node_id, std::move(req));
          return reply.then([node_id, this](check_ntp_states_reply&& reply) {
              vlog(
                dm_log.debug, "got RPC response from {}: {}", node_id, reply);
              _rpc_responses[node_id] = std::move(reply);
              return wakeup();
          });
      });
}

ss::future<> backend::schedule_topic_work(model::topic_namespace nt) {
    auto it = _topic_migration_map.find(nt);
    if (it == _topic_migration_map.end()) {
        co_return;
    }
    auto migration_id = it->second;

    auto& mrstate = _migration_states.find(migration_id)->second;
    auto& tstate = mrstate.outstanding_topics[nt];
    if (!tstate.topic_scoped_work_needed || tstate.topic_scoped_work_done) {
        co_return;
    }
    const auto maybe_migration = _table.get_migration(migration_id);
    if (!maybe_migration) {
        vlog(dm_log.trace, "migration {} gone, ignoring", migration_id);
        co_return;
    }
    topic_work tw{
      .migration_id = migration_id,
      .sought_state = *mrstate.scope.sought_state,
      .info = get_topic_work_info(nt, maybe_migration->get())};

    ssx::spawn_with_gate(
      _gate, [this, nt = std::move(nt), tw = std::move(tw)]() {
          return do_topic_work(nt, tw).then([this](topic_work_result&& twr) {
              _topic_work_results.push_back(std::move(twr));
              return wakeup();
          });
      });
}

ss::future<backend::topic_work_result>
backend::do_topic_work(model::topic_namespace nt, topic_work tw) noexcept {
    errc ec;
    try {
        vlog(
          dm_log.debug,
          "doing topic work on migration {} nt {} towards state: {}",
          tw.migration_id,
          nt,
          tw.sought_state);
        ec = co_await std::visit(
          [this, &nt, &tw](auto& info) {
              return do_topic_work(nt, tw.sought_state, info);
          },
          tw.info);
        vlog(
          dm_log.debug,
          "completed topic work on migration {} nt {} towards state: {}",
          tw.migration_id,
          nt,
          tw.sought_state);
    } catch (...) {
        vlog(
          dm_log.warn,
          "exception occured during topic work on migration {} nt {} "
          "towards state: {}",
          tw.migration_id,
          nt,
          tw.sought_state,
          std::current_exception());
        ec = errc::topic_operation_error;
    }
    co_return topic_work_result{
      .nt = std::move(nt),
      .migration = tw.migration_id,
      .sought_state = tw.sought_state,
      .ec = ec,
    };
}

ss::future<errc> backend::do_topic_work(
  model::topic_namespace nt, state sought_state, inbound_topic_work_info itwi) {
    // this assert is in accordance to the logic in get_work_scope
    vassert(false, "no state transition needs inbound topic work");
    std::ignore = nt;
    std::ignore = sought_state;
    std::ignore = itwi;
}

ss::future<errc> backend::do_topic_work(
  model::topic_namespace nt,
  state sought_state,
  outbound_topic_work_info otwi) {
    // this assert is in accordance to the logic in get_work_scope
    vassert(
      sought_state == state::finished,
      "only ->finished state transition requires topic work");
    std::ignore = nt;
    std::ignore = otwi;
    co_await ss::sleep(5s);
    co_return errc::success;
}

void backend::to_advance_if_done(
  migration_reconciliation_states_t::const_iterator it) {
    auto& rs = it->second;
    if (rs.outstanding_topics.empty()) {
        auto sought_state = *rs.scope.sought_state;
        auto [ar_it, ins] = _advance_requests.try_emplace(
          it->first, sought_state);
        if (!ins && ar_it->second.sought_state < sought_state) {
            ar_it->second = advance_info(sought_state);
        }
        _migration_states.erase(it);
    }
}

void backend::spawn_advances() {
    for (auto& [migration_id, advance_info] : _advance_requests) {
        if (advance_info.sent) {
            continue;
        }
        advance_info.sent = true;
        auto& sought_state = advance_info.sought_state;
        ssx::spawn_with_gate(_gate, [this, migration_id, sought_state]() {
            return _frontend.update_migration_state(migration_id, sought_state)
              .then([migration_id, sought_state](std::error_code ec) {
                  vlogl(
                    dm_log,
                    (ec == make_error_code(errc::success))
                      ? ss::log_level::debug
                      : ss::log_level::warn,
                    "request to advance migration {} into state {} has "
                    "been processed with error code {}",
                    migration_id,
                    sought_state,
                    ec);
              });
        });
    }
}

ss::future<> backend::handle_raft0_leadership_update() {
    auto units = co_await _mutex.get_units(_as);
    if (_is_raft0_leader == _is_coordinator) {
        co_return;
    }
    _is_coordinator = _is_raft0_leader;
    if (_is_coordinator) {
        vlog(dm_log.debug, "stepping up as a coordinator");
        // start coordinating
        for (auto& [id, mrstate] : _migration_states) {
            for (auto& [nt, tstate] : mrstate.outstanding_topics) {
                co_await reconcile_topic(nt, tstate, id, mrstate.scope, false);
            }
        }
        // resend advance requests
        for (auto& [migration_id, advance_info] : _advance_requests) {
            advance_info.sent = false;
        }
        wakeup();
    } else {
        vlog(dm_log.debug, "stepping down as a coordinator");
        // stop coordinating
        for (auto& [id, mrstate] : _migration_states) {
            co_await ssx::async_for_each(
              mrstate.outstanding_topics | std::views::values,
              std::mem_fn(&topic_reconciliation_state::clear));
        }
        _nodes_to_retry.clear();
        _node_states.clear();
        _topic_work_to_retry.clear();
    }
}

ss::future<> backend::handle_migration_update(id id) {
    auto units = co_await _mutex.get_units(_as);
    vlog(dm_log.debug, "received data migration {} notification", id);

    bool need_wakeup = false;

    auto new_maybe_metadata = _table.get_migration(id);
    auto new_state = new_maybe_metadata ? std::make_optional<state>(
                       new_maybe_metadata->get().state)
                                        : std::nullopt;
    vlog(dm_log.debug, "migration {} new state is {}", id, new_state);

    // forget about the migration if it went forward or is gone
    auto old_it = std::as_const(_migration_states).find(id);
    if (old_it != _migration_states.cend()) {
        const migration_reconciliation_state& old_mrstate = old_it->second;
        vlog(
          dm_log.debug,
          "migration {} old sought state is {}",
          id,
          old_mrstate.scope.sought_state);
        if (
          !new_maybe_metadata || new_state >= old_mrstate.scope.sought_state) {
            vlog(
              dm_log.debug, "dropping migration {} reconciliation state", id);
            drop_migration_reconciliation_rstate(old_it);
        }
    }
    // create new state if needed
    if (new_maybe_metadata) {
        const auto& new_metadata = new_maybe_metadata->get();
        auto scope = get_work_scope(new_metadata);
        if (scope.sought_state.has_value()) {
            vlog(
              dm_log.debug, "creating migration {} reconciliation state", id);
            auto new_it = _migration_states.emplace_hint(old_it, id, scope);
            co_await reconcile_migration(new_it->second, new_metadata);
            need_wakeup = true;
        }
    }
    // delete old advance requests
    if (auto it = _advance_requests.find(id); it != _advance_requests.end()) {
        if (!new_state || it->second.sought_state <= new_state) {
            _advance_requests.erase(it);
        }
    }

    if (_is_coordinator && need_wakeup) {
        wakeup();
    }
}

ss::future<> backend::process_delta(cluster::topic_table_delta&& delta) {
    vlog(dm_log.debug, "processing topic table delta={}", delta);
    model::topic_namespace nt{delta.ntp.ns, delta.ntp.tp.topic};
    auto it = _topic_migration_map.find(nt);
    if (it == _topic_migration_map.end()) {
        co_return;
    }
    auto migration_id = it->second;

    // coordination
    vassert(
      delta.type == topic_table_delta_type::replicas_updated
        || delta.type == topic_table_delta_type::disabled_flag_updated,
      "topic {} altered with topic_table_delta_type={} during "
      "migration {}",
      nt,
      delta.type,
      migration_id);
    auto& mrstate = _migration_states.find(migration_id)->second;
    auto& tstate = mrstate.outstanding_topics[nt];
    clear_tstate_belongings(nt, tstate);
    tstate.clear();
    // We potentially re-enqueue an already coordinated partition here.
    // The first RPC reply will clear it.
    co_await reconcile_topic(nt, tstate, migration_id, mrstate.scope, false);

    // local partition work
    if (has_local_replica(delta.ntp)) {
        _work_states[nt].try_emplace(
          delta.ntp.tp.partition,
          migration_id,
          *_migration_states.find(migration_id)->second.scope.sought_state);
    } else {
        auto topic_work_it = _work_states.find(nt);
        if (topic_work_it != _work_states.end()) {
            auto& topic_work_state = topic_work_it->second;
            auto rwstate_it = topic_work_state.find(delta.ntp.tp.partition);
            if (rwstate_it != topic_work_state.end()) {
                auto& rwstate = rwstate_it->second;
                if (rwstate.shard) {
                    stop_partition_work(delta.ntp, rwstate);
                }
                topic_work_state.erase(rwstate_it);
                if (topic_work_state.empty()) {
                    _work_states.erase(topic_work_it);
                }
            }
        }
    }
}

void backend::handle_shard_update(
  const model::ntp& ntp, raft::group_id, std::optional<ss::shard_id> shard) {
    if (auto maybe_rwstate = get_replica_work_state(ntp)) {
        auto& rwstate = maybe_rwstate->get();
        if (rwstate.status == migrated_replica_status::can_run) {
            update_partition_shard(ntp, rwstate, shard);
        }
    }
}

ss::future<check_ntp_states_reply>
backend::check_ntp_states_locally(check_ntp_states_request&& req) {
    vlog(dm_log.debug, "processing node request {}", req);
    check_ntp_states_reply reply;
    for (const auto& ntp_req : req.sought_states) {
        vlog(
          dm_log.trace,
          "received an RPC to promote ntp {} to state {} for migration {}",
          ntp_req.ntp,
          ntp_req.state,
          ntp_req.migration);
        // due to async notification processing we may get fresher state
        // than we have in rwstate; this is fine
        const auto maybe_migration = _table.get_migration(ntp_req.migration);
        if (!maybe_migration) {
            // migration either not yet there or gone, and we cannot tell
            // for sure => no reply
            vlog(
              dm_log.trace,
              "migration {} not found, ignoring",
              ntp_req.migration);
            continue;
        }

        const auto& metadata = maybe_migration->get();
        if (metadata.state >= ntp_req.state) {
            vlog(
              dm_log.trace,
              "migration {} already in state {}, no partition work needed",
              ntp_req.migration,
              metadata.state);
            // report progress migration-wise, whether or not made by us
            reply.actual_states.push_back(
              {.ntp = ntp_req.ntp,
               .migration = metadata.id,
               .state = metadata.state});
            continue;
        }

        auto maybe_rwstate = get_replica_work_state(ntp_req.ntp);
        if (!maybe_rwstate) {
            vlog(
              dm_log.warn,
              "migration_id={} got RPC to move ntp {} to state {}, but "
              "missing "
              "partition state for it",
              ntp_req.migration,
              ntp_req.ntp,
              ntp_req.state);
            continue;
        }
        auto& rwstate = maybe_rwstate->get();
        if (ntp_req.state != rwstate.sought_state) {
            vlog(
              dm_log.warn,
              "migration_id={} got RPC to move ntp {} to state {}, but in "
              "raft0 its desired state is {}, ignoring",
              ntp_req.migration,
              ntp_req.ntp,
              ntp_req.state,
              metadata.state);
            continue;
        }
        vlog(
          dm_log.trace,
          "migration_id={} got both RPC and raft0 message to move ntp {} "
          "to "
          "state {}, replica state is {}",
          ntp_req.migration,
          ntp_req.ntp,
          ntp_req.state,
          rwstate.status);
        // raft0 and RPC agree => time to do it!
        switch (rwstate.status) {
        case migrated_replica_status::waiting_for_rpc:
            rwstate.status = migrated_replica_status::can_run;
            [[fallthrough]];
        case migrated_replica_status::can_run: {
            auto new_shard = _shard_table.shard_for(ntp_req.ntp);
            update_partition_shard(ntp_req.ntp, rwstate, new_shard);
        } break;
        case migrated_replica_status::done:
            reply.actual_states.push_back(
              {.ntp = ntp_req.ntp,
               .migration = metadata.id,
               .state = ntp_req.state});
        }
    }

    vlog(dm_log.debug, "node request reply: {}", reply);
    return ssx::now(std::move(reply));
}

void backend::update_partition_shard(
  const model::ntp& ntp,
  replica_work_state& rwstate,
  std::optional<ss::shard_id> new_shard) {
    vlog(
      dm_log.trace,
      "for ntp {} for migration {} seeking state {} updating shard: {} => "
      "{}",
      ntp,
      rwstate.migration_id,
      rwstate.sought_state,
      rwstate.shard,
      new_shard);
    if (new_shard != rwstate.shard) {
        if (rwstate.shard) {
            stop_partition_work(ntp, rwstate);
        }
        rwstate.shard = new_shard;
        if (new_shard) {
            start_partition_work(ntp, rwstate);
        }
    }
}

void backend::clear_tstate_belongings(
  const model::topic_namespace& nt, const topic_reconciliation_state& tstate) {
    const auto& partitions = tstate.outstanding_partitions;
    for (const auto& [partition, nodes] : partitions) {
        for (const model::node_id& node : nodes) {
            auto ns_it = _node_states.find(node);
            ns_it->second.erase({nt.ns, nt.tp, partition});
            if (ns_it->second.empty()) {
                _nodes_to_retry.erase(node);
                _node_states.erase(ns_it);
            }
        }
    }
    _topic_work_to_retry.erase(nt);
}

void backend::drop_migration_reconciliation_rstate(
  migration_reconciliation_states_t::const_iterator rs_it) {
    const auto& topics = rs_it->second.outstanding_topics;
    for (const auto& [nt, tstate] : topics) {
        clear_tstate_belongings(nt, tstate);
        _work_states.erase(nt);
        _topic_migration_map.erase(nt);
    }
    _migration_states.erase(rs_it);
}

ss::future<> backend::reconcile_topic(
  const model::topic_namespace& nt,
  topic_reconciliation_state& tstate,
  id migration,
  work_scope scope,
  bool schedule_local_work) {
    if (!schedule_local_work && !_is_coordinator) {
        vlog(
          dm_log.debug,
          "not tracking topic {} transition towards state {} as part of "
          "migration {}",
          nt,
          scope.sought_state,
          migration);
        co_return;
    }
    vlog(
      dm_log.debug,
      "tracking topic {} transition towards state {} as part of "
      "migration {}, schedule_local_work={}, _is_coordinator={}",
      nt,
      scope.sought_state,
      migration,
      schedule_local_work,
      _is_coordinator);
    auto now = model::timeout_clock::now();
    if (scope.partition_work_needed) {
        if (auto maybe_assignments = _topic_table.get_topic_assignments(nt)) {
            co_await ssx::async_for_each(
              *maybe_assignments | std::views::values,
              [this, nt, &tstate, scope, migration, now, schedule_local_work](
                const auto& assignment) {
                  model::ntp ntp{nt.ns, nt.tp, assignment.id};
                  auto nodes = assignment.replicas
                               | std::views::transform(
                                 &model::broker_shard::node_id);
                  if (_is_coordinator) {
                      auto [it, ins] = tstate.outstanding_partitions.emplace(
                        std::piecewise_construct,
                        std::tuple{assignment.id},
                        std::tuple{nodes.begin(), nodes.end()});
                      vassert(
                        ins,
                        "tried to repeatedly track partition {} "
                        "as part of migration {}",
                        ntp,
                        migration);
                  }
                  for (const auto& node_id : nodes) {
                      if (_is_coordinator) {
                          auto [it, ins] = _node_states[node_id].emplace(
                            ntp, migration);
                          vassert(
                            ins,
                            "tried to track partition {} on node {} as part of "
                            "migration {}, while it is already tracked as part "
                            "of migration {}",
                            ntp,
                            node_id,
                            migration,
                            it->second);
                          _nodes_to_retry.insert_or_assign(node_id, now);
                      }
                      if (schedule_local_work && _self == node_id) {
                          vlog(
                            dm_log.debug,
                            "tracking ntp {} transition towards state {} as "
                            "part "
                            "of "
                            "migration {}",
                            ntp,
                            scope.sought_state,
                            migration);
                          auto& topic_work_state = _work_states[nt];
                          auto [it, _] = topic_work_state.try_emplace(
                            assignment.id, migration, *scope.sought_state);
                          auto& rwstate = it->second;
                          if (
                            rwstate.sought_state != scope.sought_state
                            || rwstate.migration_id != migration) {
                              if (it->second.shard) {
                                  stop_partition_work(ntp, rwstate);
                              }
                              rwstate = {migration, *scope.sought_state};
                          }
                      }
                  }
              });
        }
    }
    if (_is_coordinator && scope.topic_work_needed) {
        tstate.topic_scoped_work_needed = true;
        _topic_work_to_retry.insert_or_assign(nt, now);
    }
}

ss::future<> backend::reconcile_migration(
  migration_reconciliation_state& mrstate, const migration_metadata& metadata) {
    vlog(
      dm_log.debug,
      "tracking migration {} transition towards state {}",
      metadata.id,
      mrstate.scope.sought_state);
    co_await std::visit(
      [this, &metadata, &mrstate](const auto& migration) mutable {
          return ss::do_with(
            migration.topic_nts(),
            [this, &metadata, &mrstate](const auto& nts) {
                // poor man's `nts | std::views::enumerate`
                auto enumerated_nts = std::views::transform(
                  nts, [index = -1](const auto& nt) mutable {
                      return std::forward_as_tuple(++index, nt);
                  });
                return ssx::async_for_each(
                  enumerated_nts,
                  [this, &metadata, &mrstate](const auto& idx_nt) {
                      auto& [idx, nt] = idx_nt;
                      auto& tstate = mrstate.outstanding_topics[nt];
                      tstate.idx_in_migration = idx;
                      _topic_migration_map.emplace(nt, metadata.id);
                      return reconcile_topic(
                        nt, tstate, metadata.id, mrstate.scope, true);
                  });
            });
      },
      metadata.migration);
}

std::optional<std::reference_wrapper<backend::replica_work_state>>
backend::get_replica_work_state(const model::ntp& ntp) {
    model::topic_namespace nt{ntp.ns, ntp.tp.topic};
    if (auto it = _work_states.find(nt); it != _work_states.end()) {
        auto& topic_work_state = it->second;
        auto rwstate_it = topic_work_state.find(ntp.tp.partition);
        if (rwstate_it != topic_work_state.end()) {
            return rwstate_it->second;
        }
    }
    return std::nullopt;
}

inbound_partition_work_info backend::get_partition_work_info(
  const model::ntp& ntp, const inbound_migration& im, id migration_id) {
    auto idx = _migration_states.find(migration_id)
                 ->second.outstanding_topics[{ntp.ns, ntp.tp.topic}]
                 .idx_in_migration;
    auto& inbound_topic = im.topics[idx];
    return {
      .alias = inbound_topic.alias,
      .cloud_storage_location = inbound_topic.cloud_storage_location};
}

outbound_partition_work_info backend::get_partition_work_info(
  const model::ntp&, const outbound_migration& om, id) {
    return {om.copy_to};
}

partition_work_info backend::get_partition_work_info(
  const model::ntp& ntp, const migration_metadata& metadata) {
    return std::visit(
      [this, &ntp, &metadata](auto& migration) -> partition_work_info {
          return get_partition_work_info(ntp, migration, metadata.id);
      },
      metadata.migration);
}

inbound_topic_work_info backend::get_topic_work_info(
  const model::topic_namespace& nt,
  const inbound_migration& im,
  id migration_id) {
    auto idx = _migration_states.find(migration_id)
                 ->second.outstanding_topics[nt]
                 .idx_in_migration;
    auto& inbound_topic = im.topics[idx];
    return {
      .alias = inbound_topic.alias,
      .cloud_storage_location = inbound_topic.cloud_storage_location};
}

outbound_topic_work_info backend::get_topic_work_info(
  const model::topic_namespace&, const outbound_migration& om, id) {
    return {om.copy_to};
}

topic_work_info backend::get_topic_work_info(
  const model::topic_namespace& nt, const migration_metadata& metadata) {
    return std::visit(
      [this, &nt, &metadata](auto& migration) -> topic_work_info {
          return get_topic_work_info(nt, migration, metadata.id);
      },
      metadata.migration);
}

void backend::start_partition_work(
  const model::ntp& ntp, const backend::replica_work_state& rwstate) {
    vlog(
      dm_log.trace,
      "while working on migration {}, asking worker on shard "
      "{} to advance ntp {} to state {}",
      rwstate.migration_id,
      rwstate.shard,
      ntp,
      rwstate.sought_state);
    const auto maybe_migration = _table.get_migration(rwstate.migration_id);
    if (!maybe_migration) {
        vlog(dm_log.trace, "migration {} gone, ignoring", rwstate.migration_id);
        return;
    }

    partition_work work{
      .migration_id = rwstate.migration_id,
      .sought_state = rwstate.sought_state,
      .info = get_partition_work_info(ntp, maybe_migration->get())};

    ssx::spawn_with_gate(
      _gate, [this, &ntp, &rwstate, work = std::move(work)]() mutable {
          return _worker
            .invoke_on(
              *rwstate.shard,
              &worker::perform_partition_work,
              model::ntp{ntp},
              std::move(work))
            .then([this, ntp = ntp, rwstate](errc ec) mutable {
                if (ec == errc::success) {
                    vlog(
                      dm_log.trace,
                      "as part of migration {} worker on shard {} has "
                      "advanced "
                      "ntp {} to state {}",
                      rwstate.migration_id,
                      rwstate.shard,
                      ntp,
                      rwstate.sought_state);
                    on_partition_work_completed(
                      std::move(ntp),
                      rwstate.migration_id,
                      rwstate.sought_state);
                } else {
                    // worker should always retry unless we instructed
                    // it to abort or it is shutting down
                    vlog(
                      dm_log.warn,
                      "while working on migration {} worker on shard "
                      "{} stopped trying to advance ntp {} to state {}",
                      rwstate.migration_id,
                      rwstate.shard,
                      std::move(ntp),
                      rwstate.sought_state);
                }
            });
      });
}

void backend::stop_partition_work(
  const model::ntp& ntp, const backend::replica_work_state& rwstate) {
    vlog(
      dm_log.info,
      "while working on migration {}, asking worker on shard "
      "{} to stop trying to advance ntp {} to state {}",
      rwstate.migration_id,
      rwstate.shard,
      ntp,
      rwstate.sought_state);
    ssx::spawn_with_gate(_gate, [this, &rwstate, &ntp]() {
        return _worker.invoke_on(
          *rwstate.shard,
          &worker::abort_partition_work,
          model::ntp{ntp},
          rwstate.migration_id,
          rwstate.sought_state);
    });
}

void backend::on_partition_work_completed(
  model::ntp&& ntp, id migration, state state) {
    auto maybe_rwstate = get_replica_work_state(ntp);
    if (!maybe_rwstate) {
        return;
    }
    auto& rwstate = maybe_rwstate->get();
    if (rwstate.migration_id == migration && rwstate.sought_state == state) {
        rwstate.status = migrated_replica_status::done;
        rwstate.shard = std::nullopt;
    }
}

bool backend::has_local_replica(const model::ntp& ntp) {
    auto maybe_assignment = _topic_table.get_partition_assignment(ntp);
    if (!maybe_assignment) {
        return false;
    }
    for (const auto& replica : maybe_assignment->replicas) {
        if (_self == replica.node_id) {
            return true;
        }
    }
    return false;
}

backend::work_scope
backend::get_work_scope(const migration_metadata& metadata) {
    return std::visit(
      [&metadata](const auto& migration) {
          migration_direction_tag<std::decay_t<decltype(migration)>> tag;
          return get_work_scope(tag, metadata);
      },
      metadata.migration);
}

backend::work_scope backend::get_work_scope(
  migration_direction_tag<inbound_migration>,
  const migration_metadata& metadata) {
    switch (metadata.state) {
    case state::preparing:
        return {state::prepared, true, false};
    default:
        return {{}, false, false};
    };
}

backend::work_scope backend::get_work_scope(
  migration_direction_tag<outbound_migration>,
  const migration_metadata& metadata) {
    switch (metadata.state) {
    case state::preparing:
        return {state::prepared, true, false};
    case state::executing:
        return {state::executed, true, false};
    case state::cut_over:
        return {state::finished, false, true};
    default:
        return {{}, false, false};
    };
}

void backend::topic_reconciliation_state::clear() {
    outstanding_partitions.clear();
    topic_scoped_work_needed = false;
    topic_scoped_work_done = false;
}

} // namespace cluster::data_migrations
