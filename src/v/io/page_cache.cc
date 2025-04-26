/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "io/page_cache.h"

using reclaim_scope = seastar::memory::reclaimer_scope;

namespace experimental::io {

page_cache::page_cache(config cfg)
  : cache_(cfg, evict(this))
  , reclaimer_(
      [this](reclaimer::request request) { return reclaim(request); },
      reclaim_scope::sync) {}

void page_cache::insert(page& page) noexcept { cache_.insert(page); }

void page_cache::remove(const page& page) noexcept { cache_.remove(page); }

page_cache::evict::evict(page_cache* cache)
  : cache_(cache) {}

bool page_cache::evict::operator()(page& page) noexcept {
    cache_->evict_stats_.total++;
    if (page.may_evict()) {
        cache_->evict_stats_.granted++;
        page.clear();
        return true;
    }
    return false;
}

size_t page_cache::cost::operator()(const page& page) noexcept {
    return page.size();
}

page_cache::reclaim_result
page_cache::reclaim(reclaimer::request request) noexcept {
    if (is_memory_reclaiming()) {
        return reclaim_result::reclaimed_nothing;
    }
    batch_reclaiming_lock lock(*this);

    const auto cache_size = [this] {
        const auto stat = cache_.stat();
        return stat.small_queue_size + stat.main_queue_size;
    };

    const auto evicted_size = [init = cache_size(), cache_size] {
        return init - cache_size();
    };

    while (true) {
        const auto evicted = cache_.evict();
        if (!evicted) {
            break;
        }
        if (evicted_size() >= request.bytes_to_reclaim) {
            break;
        }
    }

    return evicted_size() == 0 ? reclaim_result::reclaimed_nothing
                               : reclaim_result::reclaimed_something;
}

uint64_t page_cache::stats::evictions_requested() const {
    return evictions_requested_;
}

uint64_t page_cache::stats::evictions_granted() const {
    return evictions_granted_;
}

uint64_t page_cache::stats::evictions_rejected() const {
    return evictions_requested_ - evictions_granted_;
}

struct page_cache::stats page_cache::stats() const noexcept {
    return {evict_stats_.total, evict_stats_.granted};
}

} // namespace experimental::io
