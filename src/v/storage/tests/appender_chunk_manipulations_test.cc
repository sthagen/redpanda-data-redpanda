// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "random/generators.h"
#include "storage/segment_appender.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <cstring>

using chunk = storage::segment_appender::chunk;
static constexpr storage::alignment alignment{4096};

TEST(AppenderChunkManipulationsTest, chunk_manipulation) {
    const auto b = random_generators::gen_alphanum_string(1024 * 1024);
    const auto chunk_size = config::shard_local_cfg().append_chunk_size();
    chunk c(chunk_size, alignment);

    // Unit tests should be running with default config
    assert(chunk_size == 16_KiB);

    {
        c.append(b.data(), c.space_left());
        ASSERT_TRUE(c.is_full());
        ASSERT_EQ(c.size(), chunk_size);
        c.reset();
    }
    {
        c.append(b.data(), alignment - 2);
        c.append(b.data(), 4);
        ASSERT_EQ(c.dma_size(), 8192);
        c.reset();
    }
    {
        size_t i = (alignment * 3) + 10;
        c.append(b.data(), i);
        ASSERT_EQ(c.dma_size(), alignment * 4);
        const char* dptr = c.dma_ptr();
        const char* eptr = b.data();
        ASSERT_EQ(std::memcmp(dptr, eptr, i), 0);
        c.flush();
        // same after flush
        ASSERT_EQ(c.dma_size(), alignment);
        // 10 bytes on the next page
        ASSERT_EQ(c.flushed_pos() % alignment, 10)
          << "10 bytes spill over: " << c;
        c.append(b.data() + i, alignment() + 10);
        ASSERT_TRUE(c.is_full()) << "Should be full: " << c;
        // we flushed after 3 pages. so the dma_size() should be 1 page left
        ASSERT_EQ(c.dma_size(), alignment);
        c.reset();
        ASSERT_EQ(c.dma_size(), 0);
    }
}
