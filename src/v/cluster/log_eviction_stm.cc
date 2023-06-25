// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/log_eviction_stm.h"

#include "bytes/iostream.h"
#include "raft/consensus.h"
#include "raft/types.h"
#include "utils/gate_guard.h"

#include <seastar/core/future-util.hh>

namespace cluster {

struct snapshot_data
  : serde::
      envelope<snapshot_data, serde::version<0>, serde::compat_version<0>> {
    model::offset effective_start_offset{};

    auto serde_fields() { return std::tie(effective_start_offset); }

    friend std::ostream& operator<<(std::ostream& os, const snapshot_data& d) {
        fmt::print(
          os, "{{ effective_start_offset: {} }}", d.effective_start_offset);
        return os;
    }
};

log_eviction_stm::log_eviction_stm(
  raft::consensus* raft,
  ss::logger& logger,
  ss::abort_source& as,
  storage::kvstore& kvstore)
  : persisted_stm("log_eviction_stm.snapshot", logger, raft, kvstore)
  , _logger(logger)
  , _as(as) {}

ss::future<> log_eviction_stm::start() {
    ssx::spawn_with_gate(_gate, [this] { return monitor_log_eviction(); });
    ssx::spawn_with_gate(
      _gate, [this] { return write_raft_snapshots_in_background(); });
    return persisted_stm::start();
}

ss::future<> log_eviction_stm::stop() {
    _reap_condition.broken();
    co_await persisted_stm::stop();
}

ss::future<> log_eviction_stm::write_raft_snapshots_in_background() {
    /// This method is executed as a background fiber and it attempts to write
    /// snapshots as close to effective_start_offset as possible.
    auto gh = _gate.hold();
    while (!_as.abort_requested()) {
        try {
            /// This background fiber can be woken-up via apply() when special
            /// batches are processed or by the storage layer when local
            /// eviction is triggered.
            co_await _reap_condition.wait();
        } catch (const ss::broken_condition_variable&) {
            /// stop() has been called
            break;
        }
        auto evict_until = std::max(
          _delete_records_eviction_offset, _storage_eviction_offset);
        if (evict_until > model::offset{}) {
            auto index_lb = _raft->log().index_lower_bound(evict_until);
            if (index_lb) {
                vassert(
                  index_lb <= evict_until,
                  "Calculated boundary {} must be <= effective_start {} ",
                  index_lb,
                  evict_until);
                try {
                    co_await do_write_raft_snapshot(*index_lb);
                } catch (const std::exception& e) {
                    vlog(
                      _logger.error,
                      "Error occurred when attempting to write snapshot: "
                      "{}, ntp: {}",
                      e,
                      _raft->ntp());
                }
            }
        }
    }
}

ss::future<> log_eviction_stm::monitor_log_eviction() {
    /// This method is executed as a background fiber and is listening for
    /// eviction events from the storage layer. These events will trigger a
    /// write snapshot, and the log will be prefix truncated.
    auto gh = _gate.hold();
    while (!_as.abort_requested()) {
        try {
            _storage_eviction_offset = co_await _raft->monitor_log_eviction(
              _as);
            vlog(
              _logger.trace,
              "Handling log deletion notification for offset: {}, ntp: {}",
              _storage_eviction_offset,
              _raft->ntp());
            const auto max_collectible_offset
              = _raft->log().stm_manager()->max_collectible_offset();
            const auto next_eviction_offset = std::min(
              max_collectible_offset, _storage_eviction_offset);
            _reap_condition.signal();
            /// Do not attempt to process another eviction event from storage
            /// until the current has completed fully
            co_await _last_snapshot_monitor.wait(
              next_eviction_offset, model::no_timeout, _as);
        } catch (const ss::abort_requested_exception&) {
            // ignore abort requested exception, shutting down
        } catch (const ss::gate_closed_exception&) {
            // ignore gate closed exception, shutting down
        } catch (const std::exception& e) {
            vlog(
              _logger.info,
              "Error handling log eviction - {}, ntp: {}",
              e,
              _raft->ntp());
        }
    }
}

ss::future<> log_eviction_stm::do_write_raft_snapshot(model::offset index_lb) {
    if (index_lb <= _raft->last_snapshot_index()) {
        co_return;
    }
    co_await _raft->visible_offset_monitor().wait(
      index_lb, model::no_timeout, _as);
    co_await _raft->refresh_commit_index();
    co_await _raft->log().stm_manager()->ensure_snapshot_exists(index_lb);
    const auto max_collectible_offset
      = _raft->log().stm_manager()->max_collectible_offset();
    if (index_lb > max_collectible_offset) {
        vlog(
          _logger.trace,
          "Can only evict up to offset: {}, ntp: {}",
          max_collectible_offset,
          _raft->ntp());
        index_lb = max_collectible_offset;
    }
    co_await _raft->write_snapshot(raft::write_snapshot_cfg(index_lb, iobuf()));
    _last_snapshot_monitor.notify(index_lb);
}

ss::future<result<model::offset, std::error_code>>
log_eviction_stm::sync_effective_start(model::timeout_clock::duration timeout) {
    /// Call this method to ensure followers have processed up until the
    /// most recent known version of the special batch. This is particularly
    /// useful to know if the start offset is up to date in the case
    /// leadership has recently changed for example.
    auto term = _raft->term();
    if (!co_await sync(timeout)) {
        if (term != _raft->term()) {
            co_return errc::not_leader;
        } else {
            co_return errc::timeout;
        }
    }
    co_return effective_start_offset();
}

model::offset log_eviction_stm::effective_start_offset() const {
    /// The start offset is the max of either the last snapshot index or the
    /// most recent delete records eviciton offset. This is because even after
    /// bootstrap the larger of the two will reflect the most recent event that
    /// has occurred, and will be the correct start offset.
    ///
    /// NOTE: Cannot replace last_snapshot_index with _storage_eviction_offset
    /// as this is the requested eviction offset and its not persisted anywhere.
    /// In the event this is set but a crash occurred before write_snapshot was
    /// called (occurs in background) it would appear that the start offset was
    /// incremented then returned to a previous value.
    return model::next_offset(
      std::max(_raft->last_snapshot_index(), _delete_records_eviction_offset));
}

ss::future<std::error_code> log_eviction_stm::truncate(
  model::offset rp_truncate_offset,
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    /// Create the special prefix_truncate batch, it is a model::record_batch
    /// with exactly one record within it, the point at which to truncate
    storage::record_batch_builder builder(
      model::record_batch_type::prefix_truncate, model::offset(0));
    /// Everything below the requested offset should be truncated, requested
    /// offset itself will be the new low_watermark (readable)
    auto key = serde::to_iobuf(rp_truncate_offset - model::offset{1});
    builder.add_raw_kv(std::move(key), iobuf());
    auto batch = std::move(builder).build();

    /// After command replication all that can be guaranteed is that the command
    /// was replicated
    vlog(
      _logger.info,
      "Replicating prefix_truncate command, truncate_offset: {} current "
      "start offset: {}, current last snapshot offset: {}, current last "
      "visible "
      "offset: {}",
      rp_truncate_offset,
      effective_start_offset(),
      _raft->last_snapshot_index(),
      _raft->last_visible_index());

    auto ec = co_await replicate_command(std::move(batch), deadline, as);
    if (ec) {
        vlog(
          _logger.error,
          "Failed to observe replicated command in log, reason: {}",
          ec.message());
        co_return ec;
    }
    co_return errc::success;
}

ss::future<std::error_code> log_eviction_stm::replicate_command(
  model::record_batch batch,
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto fut = _raft->replicate(
      _raft->term(),
      model::make_memory_record_batch_reader(std::move(batch)),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    /// Execute the replicate command bound by timeout and cancellable via
    /// abort_source mechanism
    result<raft::replicate_result> result{{}};
    try {
        if (as) {
            result = co_await ssx::with_timeout_abortable(
              std::move(fut), deadline, *as);
        } else {
            result = co_await ss::with_timeout(deadline, std::move(fut));
        }
    } catch (const ss::timed_out_error&) {
        result = errc::timeout;
    }

    if (!result) {
        vlog(
          _logger.error,
          "Failed to replicate prefix_truncate command, reason: {}",
          result.error());
        co_return result.error();
    }

    /// The following will return when the command replicated above has been
    /// processed by the apply() method. This effectively bumps the start offset
    /// to the requested value and since apply is deterministic this is
    /// guaranteed to occur. No guarantees of data removal / availability can be
    /// made at or after this point, since that occurs in a background fiber.
    auto applied = co_await wait_no_throw(
      result.value().last_offset, deadline, as);
    if (!applied) {
        if (as && as->get().abort_requested()) {
            co_return errc::shutting_down;
        }
        co_return errc::timeout;
    }
    co_return errc::success;
}

ss::future<> log_eviction_stm::apply(model::record_batch batch) {
    /// The work done within apply() must be deterministic that way the start
    /// offset will always be the same value across all replicas
    ///
    /// Here all apply() does is move forward the in memory start offset, a
    /// background fiber is responsible for evicting as much as possible
    if (unlikely(
          batch.header().type == model::record_batch_type::prefix_truncate)) {
        /// record_batches of type ::prefix_truncate are always of size 1
        const auto truncate_offset = serde::from_iobuf<model::offset>(
          batch.copy_records().begin()->release_key());
        if (truncate_offset > _delete_records_eviction_offset) {
            vlog(
              _logger.debug,
              "Moving effective start offset to "
              "truncate_point: {} last_applied: {} ntp: {}",
              truncate_offset,
              last_applied_offset(),
              _raft->ntp());

            /// Set the new in memory start offset
            _delete_records_eviction_offset = truncate_offset;
            /// Wake up the background reaping thread
            _reap_condition.signal();
            /// Writing a local snapshot is just an optimization, delete-records
            /// is infrequently called and theres no better time to persist the
            /// fact that a new start offset has been written to disk
            co_await make_snapshot();
        }
    }
}

ss::future<> log_eviction_stm::handle_eviction() {
    /// In the case there is a gap detected in the log, the only path
    /// forward is to read the raft snapshot and begin processing from the
    /// raft last_snapshot_index
    auto raft_snapshot = co_await _raft->open_snapshot();
    if (!raft_snapshot) {
        throw std::runtime_error{fmt_with_ctx(
          fmt::format,
          "encountered a gap in the raft log (last_applied: {}, log start "
          "offset: {}), but can't find the snapshot - ntp: {}",
          last_applied_offset(),
          _raft->start_offset(),
          _raft->ntp())};
    }

    auto last_snapshot_index = raft_snapshot->metadata.last_included_index;
    co_await raft_snapshot->close();
    _delete_records_eviction_offset = model::offset{};
    _storage_eviction_offset = last_snapshot_index;
    set_next(model::next_offset(last_snapshot_index));
    vlog(
      _logger.info,
      "Handled log eviction new effective start offset: {} for ntp: {}",
      effective_start_offset(),
      _c->ntp());
}

ss::future<>
log_eviction_stm::apply_snapshot(stm_snapshot_header header, iobuf&& data) {
    auto snapshot = serde::from_iobuf<snapshot_data>(std::move(data));
    vlog(
      _logger.info,
      "Applying snapshot {} at offset: {} for ntp: {}",
      snapshot,
      header.offset,
      _raft->ntp());

    _delete_records_eviction_offset = snapshot.effective_start_offset;
    _last_snapshot_offset = header.offset;
    _insync_offset = header.offset;
    return ss::now();
}

ss::future<stm_snapshot> log_eviction_stm::take_snapshot() {
    vlog(
      _logger.trace,
      "Taking snapshot at offset: {} for ntp: {}",
      last_applied_offset(),
      _raft->ntp());
    iobuf snap_data = serde::to_iobuf(
      snapshot_data{.effective_start_offset = _delete_records_eviction_offset});
    co_return stm_snapshot::create(
      0, last_applied_offset(), std::move(snap_data));
}

ss::future<> log_eviction_stm::ensure_snapshot_exists(model::offset) {
    /// This class drives eviction, therefore it cannot wait until its own
    /// snapshot exists until writing a snapshot
    return ss::now();
}

} // namespace cluster
