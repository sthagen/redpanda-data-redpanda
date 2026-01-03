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

#include "lsm/db/impl.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/logger.h"
#include "lsm/core/internal/merging_iterator.h"
#include "lsm/db/iter.h"
#include "lsm/db/table_builder.h"
#include "lsm/io/persistence.h"
#include "lsm/sst/block_cache.h"
#include "lsm/sst/builder.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>
#include <memory>
#include <utility>

namespace lsm::db {

using internal::operator""_level;

impl::impl(ctor, io::persistence p, ss::lw_shared_ptr<internal::options> o)
  : _persistence(std::move(p))
  , _opts(std::move(o))
  , _mem(ss::make_lw_shared<memtable>())
  , _table_cache(
      std::make_unique<table_cache>(
        _persistence.data.get(),
        _opts->max_open_files,
        ss::make_lw_shared<sst::block_cache>(
          _opts->block_cache_size / _opts->sst_block_size)))
  , _versions(
      std::make_unique<version_set>(
        _persistence.metadata.get(), _table_cache.get(), _opts))
  , _gc_actor(_persistence.data.get(), _opts, _table_cache.get()) {}

ss::future<std::unique_ptr<impl>> impl::open(
  ss::lw_shared_ptr<internal::options> opts, io::persistence persistence) {
    vlog(log.trace, "open_start");
    auto db = std::make_unique<impl>(
      ctor{}, std::move(persistence), std::move(opts));
    co_await db->_gc_actor.start();
    auto fut = co_await ss::coroutine::as_future(db->recover());
    if (fut.failed()) {
        auto ex = fut.get_exception();
        co_await db->close().handle_exception([](const std::exception_ptr&) {});
        std::rethrow_exception(ex);
    }
    // If we're readonly, we don't need to start any compaction loop.
    if (db->_opts->readonly) {
        vlog(log.trace, "open_end readonly=true");
        co_return db;
    }
    db->_background_work = ss::with_scheduling_group(
      db->_opts->compaction_scheduling_group, [db = db.get()] {
          return ss::do_until(
            [db] { return db->_as.abort_requested(); },
            [db] {
                return db->_start_background_work_signal.wait(db->_as)
                  .then([db] {
                      db->_background_work_running = true;
                      vlog(log.trace, "compaction_loop_start");
                      return db->run_background_compaction();
                  })
                  .then_wrapped([db](ss::future<> fut) {
                      vlog(log.trace, "compaction_loop_end");
                      db->_background_work_running = false;
                      db->maybe_schedule_compaction();
                      db->_background_work_finished_signal.broadcast();
                      try {
                          fut.get();
                          return;
                      } catch (const abort_requested_exception& ex) {
                          vlog(
                            log.debug,
                            "compaction_loop_error abort=true error=\"{}\"",
                            ex.what());
                      } catch (const io_error_exception& ex) {
                          vlog(
                            log.warn,
                            "compaction_loop_error io=true error=\"{}\"",
                            ex.what());
                      } catch (...) {
                          auto ep = std::current_exception();
                          vlog(
                            log.error,
                            "compaction_loop_error unexpected=true "
                            "error=\"{}\"",
                            ep);
                      }
                      // Signal so that we immediately retry
                      // TODO(lsm): consider some kind of backoff or
                      // backpressure?
                      db->_start_background_work_signal.signal();
                  });
            });
      });
    vlog(log.trace, "open_end readonly=false");
    co_return db;
}

ss::future<> impl::apply(ss::lw_shared_ptr<memtable> batch) {
    if (_opts->readonly) [[unlikely]] {
        throw invalid_argument_exception(
          "attempted to write to a readonly database");
    }
    if (batch->empty()) {
        co_return;
    }
    co_await make_room_for_write();
    _mem->merge(std::move(batch));
}

ss::future<> impl::make_room_for_write() {
    bool allow_delay = true;
    while (true) {
        if (
          allow_delay
          && _versions->current()->num_files(0_level)
               > _opts->level_zero_slowdown_writes_trigger) {
            // We're in throttling mode
            vlog(log.debug, "throttling_writes reason=l0_file_count");
            try {
                co_await ss::sleep_abortable(std::chrono::seconds(1), _as);
            } catch (...) {
                throw abort_requested_exception(
                  "shutdown requested during write throttling");
            }
            // Only throttle once.
            allow_delay = false;
            continue;
        }
        if (_mem->approximate_memory_usage() <= _opts->write_buffer_size) {
            // We're under our write buffer limit, let's proceed
            // Note there is a scheduling point here, so this is a soft
            // limit.
            co_return;
        }
        if (_imm) {
            vlog(log.warn, "blocking_writes reason=memtable_full");
            // We are over the write buffer limit and we have a pending
            // memtable flush, wait for it to finish.
            co_await _background_work_finished_signal.wait(_as);
            continue;
        }
        if (
          _versions->current()->num_files(0_level)
          > _opts->level_zero_stop_writes_trigger) {
            vlog(log.warn, "blocking_writes reason=l0_full");
            // We've hit out L0 file limit, wait for compaction to finish.
            co_await _background_work_finished_signal.wait(_as);
            continue;
        }
        // We're over our limit, let's make a new memtable
        vlog(log.trace, "scheduling_memtable_flush");
        _imm = std::exchange(_mem, ss::make_lw_shared<memtable>());
        maybe_schedule_compaction();
    }
}

ss::future<lookup_result> impl::get(internal::key_view key) {
    // Lookup in the mutable memtable
    {
        auto result = _mem->get(key);
        if (!result.is_missing()) {
            co_return result;
        }
    }
    // Lookup in the frozen memtable
    if (_imm) {
        auto result = (*_imm)->get(key);
        if (!result.is_missing()) {
            co_return result;
        }
    }
    // Lookup in the files
    auto current = _versions->current();
    version::get_stats stats{};
    auto result = co_await current->get(key, &stats);
    if (current->update_stats(stats)) {
        maybe_schedule_compaction();
    }
    co_return result;
}

ss::future<std::unique_ptr<internal::iterator>>
impl::create_iterator(iterator_options opts) {
    std::optional<internal::sequence_number> iter_seqno = max_applied_seqno();
    std::unique_ptr<internal::iterator> iter;
    if (!iter_seqno) {
        // If there is no data in the database, then we create
        // a view of empty data, since we cannot pin before 0.
        iter = internal::iterator::create_empty();
    } else {
        iter = create_db_iterator(
          co_await create_internal_iterator(),
          opts.snapshot ? (*opts.snapshot)->seqno() : iter_seqno.value(),
          _opts,
          [this](internal::key_view key) {
              return _versions->current()->record_read_sample(key).then(
                [this](bool compaction_needed) {
                    if (compaction_needed) {
                        maybe_schedule_compaction();
                    }
                });
          });
    }
    // If there is a non-empty memtable, wrap our existing iterator on top of
    // it and clamp further writes to the memtable to be applied.
    if (auto table = opts.memtable->get(); table && !table->empty()) {
        chunked_vector<std::unique_ptr<internal::iterator>> merged;
        merged.push_back(table->create_iterator());
        merged.push_back(std::move(iter));
        iter = create_db_iterator(
          internal::create_merging_iterator(std::move(merged)),
          table->last_seqno().value(),
          _opts,
          [](internal::key_view) { return ss::now(); });
    }
    co_return iter;
}

ss::future<std::unique_ptr<internal::iterator>>
impl::create_internal_iterator() {
    chunked_vector<std::unique_ptr<internal::iterator>> list;
    list.push_back(_mem->create_iterator());
    if (_imm) {
        list.push_back((*_imm)->create_iterator());
    }
    co_await _versions->current()->add_iterators(&list);
    co_return internal::create_merging_iterator(std::move(list));
}

ss::future<> impl::flush() {
    if (_opts->readonly) [[unlikely]] {
        throw invalid_argument_exception(
          "attempted to flush a readonly database");
    }
    auto applied_seqno = max_applied_seqno();
    while (applied_seqno > max_persisted_seqno()) {
        if (_imm) {
            co_await _background_work_finished_signal.wait(_as);
        } else if (!_mem->empty()) {
            _imm = std::exchange(_mem, ss::make_lw_shared<memtable>());
            maybe_schedule_compaction();
        }
    }
}

ss::future<> impl::close() {
    vlog(log.trace, "close_start");
    _as.request_abort_ex(abort_requested_exception("database closing"));
    auto fut = std::exchange(_background_work, std::nullopt);
    if (fut) {
        fut = co_await ss::coroutine::as_future(std::move(*fut));
    }
    co_await _gc_actor.stop();
    co_await _table_cache->close();
    co_await _persistence.data->close();
    co_await _persistence.metadata->close();
    vlog(log.trace, "close_end");
    if (fut && fut->failed()) {
        std::rethrow_exception(fut->get_exception());
    }
}

ss::future<> impl::recover() {
    vlog(log.trace, "recover_start");
    co_await _versions->recover();
    // If requested, then pre-open all the files we know about.
    if (auto max_fibers = _opts->max_pre_open_fibers) {
        chunked_vector<ss::lw_shared_ptr<file_meta_data>> all_files;
        for (auto level : _opts->levels) {
            auto files = _versions->current()->get_overlapping_inputs(
              level.number, std::nullopt, std::nullopt);
            for (auto& file : files) {
                all_files.push_back(std::move(file));
            }
        }
        vlog(log.trace, "recover_pre_open_start files={}", all_files.size());
        co_await ss::max_concurrent_for_each(
          all_files, max_fibers, [this](ss::lw_shared_ptr<file_meta_data> f) {
              return _table_cache->create_iterator(f->handle, f->file_size)
                .discard_result();
          });
        vlog(log.trace, "recover_pre_open_end files={}", all_files.size());
    }
    vlog(log.trace, "recover_end");
}

void impl::maybe_schedule_compaction() {
    if (_background_work && _background_work_running) {
        return;
    }
    if (!_imm && !_versions->needs_compaction()) {
        return;
    }
    if (_as.abort_requested()) {
        return;
    }
    _start_background_work_signal.signal();
}

namespace {
struct compaction_state {
    struct output {
        internal::file_handle handle;
        uint64_t file_size = 0;
        internal::key smallest, largest;
        internal::sequence_number oldest, newest;
    };

    output& current_output() { return outputs.back(); }

    ss::future<> open_current_builder(
      internal::file_handle h,
      io::data_persistence* p,
      sst::builder::options opts) {
        outputs.emplace_back(h);
        auto w = co_await p->open_sequential_writer(h);
        builder.emplace(std::move(w), opts);
    }
    ss::future<> finish_current_builder() {
        auto b = std::exchange(builder, std::nullopt);
        co_await b->finish().finally([&b] { return b->close(); });
        uint64_t current_bytes = b->file_size();
        current_output().file_size = current_bytes;
        total_bytes += current_bytes;
    }

    std::exception_ptr err;
    chunked_vector<output> outputs;
    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <=
    // smallest_snapshot, we can drop all entries for the same key with
    // sequence numbers < S.
    internal::sequence_number smallest_snapshot;
    std::optional<sst::builder> builder;
    uint64_t total_bytes = 0;
};
} // namespace

ss::future<> impl::run_background_compaction() {
    if (_as.abort_requested()) {
        co_return;
    }
    if (_imm) {
        co_return co_await flush_memtable();
    }
    auto maybe_compaction = _versions->pick_compaction();
    if (!maybe_compaction) {
        co_return;
    }
    auto compaction = *std::move(maybe_compaction);
    auto input_level = compaction.level();
    auto output_level = input_level + 1_level;
    auto num_input_files
      = compaction.num_input_files(compaction::which::input_level)
        + compaction.num_input_files(compaction::which::output_level);
    vlog(
      log.trace,
      "compaction_start input_level={} output_level={} input_files={}",
      input_level,
      output_level,
      num_input_files);
    if (compaction.is_trivial_move()) {
        auto input_level_files = compaction.num_input_files(
          compaction::which::input_level);
        vassert(
          input_level_files == 1,
          "trivial compactions should only be for a single input file: {}",
          input_level_files);
        auto file = compaction.input(compaction::which::input_level, 0);
        compaction.edit()->remove_file(compaction.level(), file->handle);
        compaction.edit()->add_file({
          .level = compaction.level() + 1_level,
          .file_handle = file->handle,
          .file_size = file->file_size,
          .smallest = file->smallest,
          .largest = file->largest,
          .oldest_seqno = file->oldest_seqno,
          .newest_seqno = file->newest_seqno,
        });
        vlog(log.trace, "manifest_write_start");
        co_await _versions->log_and_apply(std::move(*compaction.edit()));
        vlog(log.trace, "manifest_write_end");
        vlog(
          log.trace,
          "compaction_end input_level={} output_level={} output_files=1 "
          "output_bytes={} trivial_move=true",
          input_level,
          output_level,
          file->file_size);
        co_return;
    }
    compaction_state state{
      // We need to preserve intermediate data between snapshots so we can
      // surface the correct snapshot isolation. So we use the oldest snapshot,
      // otherwise the lastest sequence number because we don't have to preserve
      // any intermediate versions.
      // Note we can call `value` on `last_seqno` because we have to have some
      // data in the database in order to trigger compaction.
      .smallest_snapshot = _snapshots.oldest_seqno().value_or(
        _versions->last_seqno().value()),
    };
    auto max_file_size = _opts->levels[output_level].max_file_size;
    sst::builder::options sst_options{
      .block_size = _opts->sst_block_size,
      .filter_period = _opts->sst_filter_period,
      .compression = _opts->levels[output_level].compression,
    };
    try {
        auto input = co_await _versions->make_input_iterator(&compaction);
        std::optional<internal::key> current_key;
        internal::sequence_number last_seqno_for_key
          = internal::sequence_number::max();
        co_await input->seek_to_first();
        while (input->valid() && !_as.abort_requested()) {
            if (_imm) {
                // Always prioritize memtable flushes
                co_await flush_memtable();
                _background_work_finished_signal.signal();
            }
            auto key = input->key();
            if (state.builder && compaction.should_stop_before(key)) {
                co_await state.finish_current_builder();
            }
            bool drop = false;
            if (!current_key || key.user_key() != current_key->user_key()) {
                // First occurrence of this user key
                current_key = internal::key(key);
                last_seqno_for_key = internal::sequence_number::max();
            }
            auto key_seqno = key.seqno();
            // NOLINTNEXTLINE(*branch-clone*)
            if (last_seqno_for_key <= state.smallest_snapshot) {
                // Hidden by a newer entry for the same user key
                drop = true;
            } else if (
              key.is_tombstone() && key_seqno <= state.smallest_snapshot
              && compaction.is_base_level_for_key(key)) {
                // For this user key:
                // (1) there is no data in higher levels
                // (2) data in lower levels will have larger sequence
                // numbers (3) data in layers that are being compacted here
                // and have smaller sequence numbers will be dropped in the
                // next few iterations of this loop (by rule (A) above).
                // Therefore this deletion marker is obsolete and can be
                // dropped.
                drop = true;
            }
            last_seqno_for_key = key_seqno;
            if (!drop) {
                if (!state.builder) {
                    co_await state.open_current_builder(
                      {
                        .id = _versions->new_file_id(),
                        .epoch = _opts->database_epoch,
                      },
                      _persistence.data.get(),
                      sst_options);
                }
                auto& current = state.current_output();
                if (state.builder->num_entries() == 0) {
                    current.smallest = internal::key(key);
                    current.oldest = key_seqno;
                    current.newest = key_seqno;
                }
                current.largest = internal::key(key);
                current.oldest = std::min(key_seqno, current.oldest);
                current.newest = std::max(key_seqno, current.newest);
                co_await state.builder->add(internal::key(key), input->value());
                // Close output file if it is big enough
                if (state.builder->file_size() >= max_file_size) {
                    co_await state.finish_current_builder();
                }
            }
            co_await input->next();
        }
    } catch (const base_exception& ex) {
        state.err = std::make_exception_ptr(ex);
    }
    if (state.builder) {
        co_await state.finish_current_builder();
    }
    _as.check(); // Do this after we clean up the builder
    if (state.err) {
        std::rethrow_exception(state.err);
    }
    auto* edit = compaction.edit();
    compaction.add_input_deletions(edit);
    for (auto& output : state.outputs) {
        edit->add_file({
          .level = compaction.level() + 1_level,
          .file_handle = output.handle,
          .file_size = output.file_size,
          .smallest = std::move(output.smallest),
          .largest = std::move(output.largest),
          .oldest_seqno = output.oldest,
          .newest_seqno = output.newest,
        });
    }
    vlog(log.trace, "manifest_write_start");
    co_await _versions->log_and_apply(std::move(*edit));
    vlog(log.trace, "manifest_write_end");
    // Go and cleanup any obsolete files where both the epoch, and file ID is
    // less than what we have committed, and is not in our working set.
    co_await _gc_actor.tell(
      gc_message{
        .highest_used_file_id = _versions->highest_used_file_id(),
        .live_files = _versions->get_live_files(),
      });
    vlog(
      log.trace,
      "compaction_end input_level={} output_level={} output_files={} "
      "output_bytes={}",
      input_level,
      output_level,
      state.outputs.size(),
      state.total_bytes);
}

ss::future<> impl::flush_memtable() {
    vassert(_imm, "immutable memtable required in order to flush a memtable");
    auto v = _versions->current();
    auto id = _versions->new_file_id();
    auto& imm = *_imm;
    auto level = imm->empty()
                   ? 0_level
                   : v->pick_level_for_memtable_output(
                       imm->min_key().user_key(), imm->max_key().user_key());
    auto mem_bytes = imm->approximate_memory_usage();
    vlog(
      log.trace,
      "flush_memtable_start level={} mem_bytes={}",
      level,
      mem_bytes);
    sst::builder::options sst_options{
      .block_size = _opts->sst_block_size,
      .filter_period = _opts->sst_filter_period,
      .compression = _opts->levels[level].compression,
    };
    auto result = co_await build_table(
      _persistence.data.get(),
      {.id = id, .epoch = _opts->database_epoch},
      imm->create_iterator(),
      sst_options,
      &_as);
    if (!result) {
        _versions->reuse_file_id(id);
        vlog(
          log.trace,
          "flush_memtable_end level={} file_bytes=0 empty=true",
          level);
        co_return;
    }
    version_edit edit(*_opts);
    edit.set_last_seqno(result->newest_seqno);
    edit.add_file({
      .level = level,
      .file_handle = {.id = id, .epoch = _opts->database_epoch},
      .file_size = result->file_size,
      .smallest = std::move(result->smallest),
      .largest = std::move(result->largest),
      .oldest_seqno = result->oldest_seqno,
      .newest_seqno = result->newest_seqno,
    });
    vlog(log.trace, "manifest_write_start");
    co_await _versions->log_and_apply(std::move(edit));
    vlog(log.trace, "manifest_write_end");
    vlog(
      log.trace,
      "flush_memtable_end level={} file_bytes={}",
      level,
      result->file_size);
    // Now that the new version has been applied, it's safe to remove the
    // immutable memtable, as readers will pick up the new file instead.
    //
    // Note that due to the above scheduling point, it's possible for a
    // reader to pick up both the memtable and the new version with the
    // file. This is OK because all iterators deduplicate already.
    _imm = std::nullopt;
}

std::optional<internal::sequence_number> impl::max_persisted_seqno() const {
    return _versions->last_seqno();
}

std::optional<internal::sequence_number> impl::max_applied_seqno() const {
    if (auto seqno = _mem->last_seqno()) {
        return seqno;
    }
    if (_imm) {
        if (auto seqno = (*_imm)->last_seqno()) {
            return seqno;
        }
    }
    return max_persisted_seqno();
}

ss::optimized_optional<std::unique_ptr<snapshot>> impl::create_snapshot() {
    if (auto seqno = max_applied_seqno()) {
        return _snapshots.create(*seqno);
    }
    return std::nullopt;
}

} // namespace lsm::db
