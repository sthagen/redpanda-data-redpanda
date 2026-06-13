/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/scheduler.h"

#include "cloud_topics/level_one/maintenance/log_collector.h"
#include "cloud_topics/level_one/maintenance/log_info_collector.h"
#include "cloud_topics/level_one/maintenance/logger.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/scheduling_policies.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

namespace cloud_topics::l1 {

compaction_scheduler::compaction_scheduler(
  compaction_cluster_state state,
  ss::sharded<file_io>* io,
  ss::sharded<l1::replicated_metastore>* metastore,
  ss::sharded<level_one_reader_probe>* l1_reader_probe)
  : _io(io)
  , _metastore(metastore)
  , _log_collector(make_default_log_collector(
      [this](
        const model::ntp& ntp,
        const model::topic_id_partition& tidp,
        std::string_view ctx) { manage_partition(ntp, tidp, ctx); },
      [this](const model::ntp& ntp, std::string_view ctx) {
          unmanage_partition(ntp, ctx);
      },
      [this](const model::ntp& ntp) { return is_managed(ntp); },
      state))
  , _log_info_collector(make_default_log_info_collector(
      &_metastore->local(),
      &state.metadata_cache->local(),
      state.shard_table,
      state.partition_manager))
  , _scheduling_policy(make_default_scheduling_policy())
  , _worker_manager(
      _compaction_queue,
      _leveling_queue,
      io,
      metastore,
      state.metadata_cache,
      _probe,
      l1_reader_probe)
  , _compaction_interval(
      config::shard_local_cfg().cloud_topics_compaction_interval_ms.bind())
  , _leveling_interval(
      config::shard_local_cfg().cloud_topics_leveling_interval_ms.bind())
  , _compaction_disabled(
      config::shard_local_cfg().cloud_topics_compaction_disabled.bind())
  , _leveling_disabled(
      config::shard_local_cfg().cloud_topics_leveling_disabled.bind())
  , _compaction_queue(_scheduling_policy->get_comparator())
  , _leveling_queue(
      leveling_extent_reclamation_policy{
        config::shard_local_cfg()
          .cloud_topics_reconciliation_max_object_size.bind()}
        .get_comparator())
  , _scheduler_update_queue([](const std::exception_ptr& ex) {
      vlog(compaction_log.error, "Scheduler loop update queue error: {}", ex);
  }) {
    _compaction_interval.watch([this]() { _compaction_sem.signal(); });
    _leveling_interval.watch([this]() { _leveling_sem.signal(); });
}

compaction_scheduler::compaction_scheduler(log_info_collector info_collector)
  : _log_info_collector(std::move(info_collector))
  , _scheduling_policy(make_default_scheduling_policy())
  , _worker_manager(
      _compaction_queue,
      _leveling_queue,
      nullptr,
      nullptr,
      nullptr,
      _probe,
      nullptr)
  , _compaction_interval(
      config::shard_local_cfg().cloud_topics_compaction_interval_ms.bind())
  , _leveling_interval(
      config::shard_local_cfg().cloud_topics_leveling_interval_ms.bind())
  , _compaction_disabled(
      config::shard_local_cfg().cloud_topics_compaction_disabled.bind())
  , _leveling_disabled(
      config::shard_local_cfg().cloud_topics_leveling_disabled.bind())
  , _compaction_queue(_scheduling_policy->get_comparator())
  , _leveling_queue(
      leveling_extent_reclamation_policy{
        config::shard_local_cfg()
          .cloud_topics_reconciliation_max_object_size.bind()}
        .get_comparator())
  , _scheduler_update_queue([](const std::exception_ptr& ex) {
      vlog(compaction_log.error, "Scheduler loop update queue error: {}", ex);
  }) {
    _compaction_interval.watch([this]() { _compaction_sem.signal(); });
    _leveling_interval.watch([this]() { _leveling_sem.signal(); });
}

bool compaction_scheduler::is_managed(const model::ntp& ntp) const noexcept {
    auto it = _ntp_to_tidp.find(ntp);
    if (it == _ntp_to_tidp.end()) {
        return false;
    }

    auto& tidp = it->second;

    return _logs.contains(tidp);
}

void compaction_scheduler::manage_partition(
  const model::ntp& ntp,
  const model::topic_id_partition& tidp,
  std::string_view ctx) {
    vlog(
      compaction_log.info, "Asked to manage CTP: {}/{} ({})", ntp, tidp, ctx);
    auto [it, success] = _logs.insert(
      ss::make_lw_shared<log_compaction_meta>(tidp, ntp));
    _logs_list.push_back(*it->get());
    _ntp_to_tidp.emplace(ntp, tidp);
    vassert(success, "Could not manage CTP {} (concurrency issue?)", ntp);
    _probe.set_log_count(_logs.size());
}

void compaction_scheduler::unmanage_partition(
  const model::ntp& ntp, std::string_view ctx) {
    auto tidp_entry = _ntp_to_tidp.extract(ntp);
    if (!tidp_entry.has_value()) {
        vassert(false, "Could not unmanage CTP {} (concurrency issue?)", ntp);
    }

    auto& tidp = tidp_entry->second;

    vlog(compaction_log.info, "Asked to unmanage CTP: {} ({})", ntp, ctx);

    auto handle_opt = _logs.extract(tidp);
    if (!handle_opt) {
        vassert(false, "Could not unmanage CTP {} (concurrency issue?)", ntp);
    }

    auto handle = std::move(handle_opt).value();

    // Evict any queued (non-inflight) compaction entry for this CTP so it does
    // not linger in the queue holding the meta alive. An inflight log is not in
    // the queue (it was popped on dispatch); it is stopped below.
    _compaction_queue.clear(tidp);

    // Unlink so that any leveling jobs for this CTP still in `_leveling_queue`
    // are dropped when dequeued (`try_acquire_leveling_work` skips unlinked
    // metas), and so a queued compaction entry, if any, is ignored too.
    handle->link.unlink();

    // Request that compaction and leveling of this CTP be stopped, if in
    // flight. `handle` is a `lw_shared_ptr`- we can allow it to go out of scope
    // here without fear of UAF elsewhere.
    _worker_manager.request_stop_leveling(handle);
    _worker_manager.request_stop_compaction(std::move(handle));
    _probe.set_log_count(_logs.size());
}

void compaction_scheduler::watch_config_changes() {
    _compaction_disabled.watch([this] {
        _scheduler_update_queue.submit([this] {
            return _compaction_disabled() ? pause_compaction_loop()
                                          : resume_compaction_loop();
        });
    });
    _leveling_disabled.watch([this] {
        _scheduler_update_queue.submit([this] {
            return _leveling_disabled() ? pause_leveling_loop()
                                        : resume_leveling_loop();
        });
    });
}

bool compaction_scheduler::should_run_compaction_loop() const {
    return !_gate.is_closed() && !_as.abort_requested()
           && _compaction_target_loop_state == loop_state::active;
}

bool compaction_scheduler::should_run_leveling_loop() const {
    return !_gate.is_closed() && !_as.abort_requested()
           && _leveling_target_loop_state == loop_state::active;
}

ss::future<> compaction_scheduler::resume_compaction_loop() {
    if (_compaction_target_loop_state == loop_state::stopped) {
        co_return;
    }
    _compaction_target_loop_state = loop_state::active;
    if (_compaction_loop_fut.has_value()) {
        co_return;
    }
    // Bring the per-shard compaction fibers back before feeding them
    // work.
    co_await _worker_manager.resume_all_workers(
      maintenance_job_type::compaction);
    _compaction_loop_fut = ssx::spawn_with_gate_then(
      _gate, [this] { return compaction_scheduling_loop(); });
}

ss::future<> compaction_scheduler::pause_compaction_loop() {
    if (_compaction_target_loop_state == loop_state::stopped) {
        co_return;
    }
    _compaction_target_loop_state = loop_state::paused;
    if (!_compaction_loop_fut.has_value()) {
        co_return;
    }

    vlog(compaction_log.info, "Compaction disabled; pausing compaction loop");

    _compaction_sem.signal();
    co_await clear_compaction_loop_fut();

    co_await _worker_manager.pause_all_workers(
      maintenance_job_type::compaction);

    _compaction_queue.clear_all();
    _probe.set_compaction_queue_length(0);
}

ss::future<> compaction_scheduler::clear_compaction_loop_fut() {
    if (_compaction_loop_fut.has_value()) {
        co_await std::move(_compaction_loop_fut).value();
        _compaction_loop_fut.reset();
    }
}

ss::future<> compaction_scheduler::resume_leveling_loop() {
    if (_leveling_target_loop_state == loop_state::stopped) {
        co_return;
    }
    _leveling_target_loop_state = loop_state::active;
    if (_leveling_loop_fut.has_value()) {
        co_return;
    }
    // Bring the per-shard leveling fibers back before feeding them
    // work.
    co_await _worker_manager.resume_all_workers(maintenance_job_type::leveling);
    _leveling_loop_fut = ssx::spawn_with_gate_then(
      _gate, [this] { return leveling_scheduling_loop(); });
}

ss::future<> compaction_scheduler::pause_leveling_loop() {
    if (_leveling_target_loop_state == loop_state::stopped) {
        co_return;
    }
    _leveling_target_loop_state = loop_state::paused;
    if (!_leveling_loop_fut.has_value()) {
        co_return;
    }

    vlog(compaction_log.info, "Leveling disabled; pausing leveling loop");

    _leveling_sem.signal();
    co_await clear_leveling_loop_fut();

    co_await _worker_manager.pause_all_workers(maintenance_job_type::leveling);

    _leveling_queue.clear_all();
    _probe.set_leveling_queue_length(0);
}

ss::future<> compaction_scheduler::clear_leveling_loop_fut() {
    if (_leveling_loop_fut.has_value()) {
        co_await std::move(_leveling_loop_fut).value();
        _leveling_loop_fut.reset();
    }
}

ss::future<> compaction_scheduler::compaction_scheduling_loop() {
    vlog(compaction_log.debug, "Starting compaction scheduling loop");
    while (should_run_compaction_loop()) {
        try {
            co_await do_compaction_scheduling();
        } catch (...) {
            auto e = std::current_exception();
            vlogl(
              compaction_log,
              ssx::is_shutdown_exception(e) ? ss::log_level::debug
                                            : ss::log_level::warn,
              "Compaction scheduling iteration failed: {}",
              e);
        }
    }
}

ss::future<> compaction_scheduler::do_compaction_scheduling() {
    auto compaction_interval = _compaction_interval();
    try {
        co_await _compaction_sem.wait(
          _compaction_interval(),
          std::max(_compaction_sem.current(), size_t(1)));
    } catch (const ss::semaphore_timed_out&) {
        // Fall through
    }

    if (!should_run_compaction_loop()) {
        co_return;
    }

    if (compaction_interval != _compaction_interval()) {
        // Cluster config was changed while waiting.
        co_return;
    }

    co_await _log_info_collector.collect_compaction_info(
      _logs, _logs_list, _compaction_queue);

    _probe.set_compaction_queue_length(_compaction_queue.size());

    co_await _worker_manager.alert_compaction_workers();
}

ss::future<> compaction_scheduler::leveling_scheduling_loop() {
    vlog(compaction_log.debug, "Starting leveling scheduling loop");
    while (should_run_leveling_loop()) {
        try {
            co_await do_leveling_scheduling();
        } catch (...) {
            auto e = std::current_exception();
            vlogl(
              compaction_log,
              ssx::is_shutdown_exception(e) ? ss::log_level::debug
                                            : ss::log_level::warn,
              "Leveling scheduling iteration failed: {}",
              e);
        }
    }
}

ss::future<> compaction_scheduler::do_leveling_scheduling() {
    auto leveling_interval = _leveling_interval();
    try {
        co_await _leveling_sem.wait(
          _leveling_interval(), std::max(_leveling_sem.current(), size_t(1)));
    } catch (const ss::semaphore_timed_out&) {
        // Fall through
    }

    if (!should_run_leveling_loop()) {
        co_return;
    }

    if (leveling_interval != _leveling_interval()) {
        co_return;
    }

    co_await _log_info_collector.collect_leveling_info(
      _logs, _logs_list, _leveling_queue);

    _probe.set_leveling_queue_length(_leveling_queue.size());

    co_await _worker_manager.alert_leveling_workers();
}

ss::future<> compaction_scheduler::start() {
    _probe.setup_metrics();
    co_await _worker_manager.start();
    co_await _log_collector->start();
    // Arm the config watches before launching the loops, and route the
    // initial launches through `_scheduler_update_queue` like any other
    // transition. Each task reads the live binding when it runs, so a toggle
    // landing at any point during startup is either observed directly or
    // serialized behind it as a watch task.
    watch_config_changes();
    if (!_compaction_disabled()) {
        _scheduler_update_queue.submit(
          [this] { return resume_compaction_loop(); });
    }
    if (!_leveling_disabled()) {
        _scheduler_update_queue.submit(
          [this] { return resume_leveling_loop(); });
    }
}

ss::future<> compaction_scheduler::stop() {
    vlog(compaction_log.debug, "Stopping compaction scheduling loop");
    _as.request_abort();

    co_await _scheduler_update_queue.shutdown();

    _compaction_target_loop_state = loop_state::stopped;
    _leveling_target_loop_state = loop_state::stopped;

    _compaction_sem.broken();
    _leveling_sem.broken();

    // Stop making new jobs.
    auto close_fut = _gate.close();

    // Stop collecting logs.
    if (_log_collector) {
        co_await _log_collector->stop();
    }

    // Clear logs.
    _logs.clear();

    // Stop workers and inflight compactions.
    co_await _worker_manager.stop();

    co_await clear_compaction_loop_fut();
    co_await clear_leveling_loop_fut();

    co_await std::move(close_fut);
}

} // namespace cloud_topics::l1
