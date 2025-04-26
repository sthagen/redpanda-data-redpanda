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
#include "io/page_cache.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

namespace io = experimental::io;

namespace {
auto make_page(uint64_t offset, uint64_t size) {
    return seastar::make_lw_shared<io::page>(
      offset, seastar::temporary_buffer<char>(size));
}
} // namespace

TEST(PageCache, Stats) {
    constexpr size_t page_size = 4096;

    // evictions happens _after_ size is exceeded. so, set the limit to be less
    // than the size of 2 pages so evictions start after the second page. sizes
    // are slightly different because s3 fifo expects the main fifo queue size
    // to be larger than the small fifo queue size.
    io::page_cache cache({(2 * page_size) - 1, (2 * page_size) - 2});

    EXPECT_EQ(cache.stats().evictions_requested(), 0);
    EXPECT_EQ(cache.stats().evictions_granted(), 0);
    EXPECT_EQ(cache.stats().evictions_rejected(), 0);

    std::vector<seastar::lw_shared_ptr<io::page>> pages;

    // 1 page in cache
    pages.push_back(make_page(0, page_size));
    cache.insert(*pages.back());
    EXPECT_EQ(cache.stats().evictions_requested(), 0);
    EXPECT_EQ(cache.stats().evictions_granted(), 0);
    EXPECT_EQ(cache.stats().evictions_rejected(), 0);

    // 2 page in cache
    pages.push_back(make_page(0, page_size));
    cache.insert(*pages.back());
    EXPECT_EQ(cache.stats().evictions_requested(), 0);
    EXPECT_EQ(cache.stats().evictions_granted(), 0);
    EXPECT_EQ(cache.stats().evictions_rejected(), 0);

    // 3rd page triggers a reclaim
    pages.push_back(make_page(0, page_size));
    cache.insert(*pages.back());
    EXPECT_EQ(cache.stats().evictions_requested(), 1);
    EXPECT_EQ(cache.stats().evictions_granted(), 1);
    EXPECT_EQ(cache.stats().evictions_rejected(), 0);

    // holding references to pages make them non-evictable
    auto references = pages;

    // eviction will fail on all pages
    pages.push_back(make_page(0, page_size));
    cache.insert(*pages.back());
    EXPECT_EQ(cache.stats().evictions_requested(), 3);
    EXPECT_EQ(cache.stats().evictions_granted(), 1);
    EXPECT_EQ(cache.stats().evictions_rejected(), 2);

    // the last page insertion still succeeds, and isn't contained in the
    // references, so it should be the one page that is now evictable
    auto prev_requested = cache.stats().evictions_requested();
    pages.push_back(make_page(0, page_size));
    cache.insert(*pages.back());
    auto new_requests = cache.stats().evictions_requested() - prev_requested;
    EXPECT_GE(new_requests, 1);
    EXPECT_EQ(cache.stats().evictions_granted(), 2);
    EXPECT_EQ(cache.stats().evictions_rejected(), 2 + (new_requests - 1));

    references.clear();
    for (auto& page : pages) {
        cache.remove(*page);
    }
}
