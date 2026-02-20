/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batch_cache/batch_cache.h"

#include "config/configuration.h"
#include "ssx/future-util.h"
#include "storage/batch_cache.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"

#include <seastar/core/preempt.hh>

#include <chrono>

namespace cloud_topics {

batch_cache::batch_cache(
  storage::log_manager* log_manager, std::chrono::milliseconds gc_interval)
  : _gc_interval(gc_interval)
  , _lm(log_manager)
  , _probe(config::shard_local_cfg().disable_metrics()) {}

batch_cache::batch_cache(
  ss::sharded<storage::api>& log_manager, std::chrono::milliseconds gc_interval)
  : batch_cache(&log_manager.local().log_mgr(), gc_interval) {}

ss::future<> batch_cache::start() {
    _cleanup_timer.set_callback([this] {
        auto gh = _gate.hold();
        ssx::spawn_with_gate(_gate, [this] { return cleanup_index_entries(); });
    });
    _cleanup_timer.arm(_gc_interval);
    return ss::now();
}

ss::future<> batch_cache::stop() {
    _cleanup_timer.cancel();
    for (auto& [_, entry] : _entries) {
        if (entry.monitor) {
            entry.monitor->stop();
        }
    }
    co_await _gate.close();
}

void batch_cache::put(
  const model::topic_id_partition& tidp, const model::record_batch& b) {
    vassert(
      b.term() > model::term_id{-1},
      "Batch without term in the cache: {}",
      b.header());
    if (_lm == nullptr) {
        return;
    }
    _gate.check();
    auto& entry = _entries[tidp];
    if (!entry.index) {
        auto cache_ix = _lm->create_cache(storage::with_cache::yes);
        if (!cache_ix.has_value()) {
            return;
        }
        entry.index = std::make_unique<storage::batch_cache_index>(
          std::move(*cache_ix));
    }
    entry.index->put(b, storage::batch_cache::is_dirty_entry::no);
    _probe.register_put(b.size_bytes());

    // Notify any readers waiting for this offset.
    if (entry.monitor) {
        entry.monitor->notify(b.last_offset());
    }
}

std::optional<model::record_batch>
batch_cache::get(const model::topic_id_partition& tidp, model::offset o) {
    if (_lm == nullptr) {
        return std::nullopt;
    }
    _gate.check();
    if (auto it = _entries.find(tidp);
        it != _entries.end() && it->second.index) {
        auto rb = it->second.index->get(o);
        if (rb.has_value()) {
            vassert(
              rb->term() > model::term_id{-1},
              "Batch without term in the cache: {}",
              rb->header());
            vassert(
              rb->base_offset() <= o && o <= rb->last_offset(),
              "Unexpected batch for {}, got range: [{},{}] for offset {}",
              tidp,
              rb->base_offset(),
              rb->last_offset(),
              o);
            _probe.register_get(rb->size_bytes());
        } else {
            _probe.register_miss();
        }
        return rb;
    }
    _probe.register_miss();
    return std::nullopt;
}

ss::future<> batch_cache::wait_for_offset(
  const model::topic_id_partition& tidp,
  model::offset offset,
  model::offset last_known,
  model::timeout_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto& entry = _entries[tidp];
    if (!entry.monitor) {
        entry.monitor = std::make_unique<offset_monitor<model::offset>>();
        entry.monitor->notify(last_known);
    }
    return entry.monitor->wait(offset, deadline, as);
}

ss::future<> batch_cache::cleanup_index_entries() {
    // NOTE: the memory is reclaimed asynchronously.  In some cases
    // the index may no longer reference any live entries.  If this
    // is the case we need to delete the batch_cache_index from the
    // '_entries' collection to avoid accumulating orphaned entries.
    auto it = _entries.begin();
    while (it != _entries.end()) {
        auto& entry = it->second;
        // Release empty index
        if (entry.index && entry.index->empty()) {
            entry.index.reset();
        }
        // Erase the entry only when both index and monitor are gone.
        // Don't stop a monitor with active waiters — that would throw
        // abort_requested_exception to readers.
        bool monitor_idle = !entry.monitor || entry.monitor->empty();
        if (!entry.index && monitor_idle) {
            if (entry.monitor) {
                entry.monitor->stop();
            }
            it = _entries.erase(it);
        } else {
            ++it;
        }
        if (ss::need_preempt() && it != _entries.end()) {
            model::topic_id_partition next = it->first;
            co_await ss::yield();
            it = _entries.lower_bound(next);
        }
    }
    _cleanup_timer.arm(_gc_interval);
}

} // namespace cloud_topics
