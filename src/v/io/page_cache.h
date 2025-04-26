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
#pragma once
#include "io/page.h"
#include "utils/s3_fifo.h"

#include <seastar/core/memory.hh>

namespace experimental::io {

/**
 * The page cache tracks pages and controls cache eviction.
 */
class page_cache {
    class evict {
    public:
        struct stats {
            uint64_t total{0};
            uint64_t granted{0};
        };

        explicit evict(page_cache*);
        bool operator()(page&) noexcept;

    private:
        page_cache* cache_;
    };

    struct cost {
        size_t operator()(const page&) noexcept;
    };

    using cache_type
      = utils::s3_fifo::cache<page, &page::cache_hook, evict, cost>;

public:
    using config = cache_type::config;

    /**
     * Initialize with the given configuration.
     */
    explicit page_cache(config cfg);

    /**
     * The seastar reclaimer callback captures `this`.
     */
    page_cache(const page_cache&) = delete;
    page_cache& operator=(const page_cache&) = delete;
    page_cache(page_cache&&) noexcept = delete;
    page_cache& operator=(page_cache&&) noexcept = delete;
    ~page_cache() = default;

    /**
     * Insert @page into the cache.
     *
     * The page must not already be stored in the cache.
     */
    void insert(page& page) noexcept;

    /**
     * Remove @page from the cache.
     *
     * The page must currently be stored in the cache.
     */
    void remove(const page&) noexcept;

    struct stats {
    public:
        [[nodiscard]] uint64_t evictions_requested() const;
        [[nodiscard]] uint64_t evictions_granted() const;
        [[nodiscard]] uint64_t evictions_rejected() const;

    private:
        friend page_cache;

        // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
        stats(uint64_t evictions_requested, uint64_t evictions_granted)
          : evictions_requested_(evictions_requested)
          , evictions_granted_(evictions_granted) {}

        uint64_t evictions_requested_;
        uint64_t evictions_granted_;
    };

    [[nodiscard]] stats stats() const noexcept;

private:
    using reclaimer = seastar::memory::reclaimer;
    using reclaim_result = seastar::memory::reclaiming_result;

    bool _is_reclaiming{false};
    struct batch_reclaiming_lock {
        explicit batch_reclaiming_lock(page_cache& cache) noexcept
          : ref(cache)
          , prev(ref._is_reclaiming) {
            ref._is_reclaiming = true;
        }
        ~batch_reclaiming_lock() noexcept { ref._is_reclaiming = prev; }
        batch_reclaiming_lock(const batch_reclaiming_lock&) = delete;
        batch_reclaiming_lock(batch_reclaiming_lock&&) = delete;
        batch_reclaiming_lock& operator=(const batch_reclaiming_lock&) = delete;
        batch_reclaiming_lock& operator=(batch_reclaiming_lock&&) = delete;

    private:
        page_cache& ref;
        bool prev;
    };
    [[nodiscard]] bool is_memory_reclaiming() const { return _is_reclaiming; }
    reclaim_result reclaim(reclaimer::request) noexcept;

    evict::stats evict_stats_;
    cache_type cache_;
    reclaimer reclaimer_;
};

} // namespace experimental::io
