/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/common/file_arena.h"
#include "config/property.h"
#include "ssx/semaphore.h"
#include "test_utils/test.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>

#include <gtest/gtest.h>

using namespace cloud_topics::l1;

static const ss::sstring test_arena_str = "test_arena";
static ss::logger test_log(test_arena_str);

struct FileArenaTestFixture : public seastar_test {
    ss::future<> set_up_file_arena(
      size_t global_scratch_space,
      double soft_limit_percent,
      size_t block_size) {
        co_await sharded_arena.start(
          ss::sharded_parameter([&]() { return &io; }),
          ss::sharded_parameter([&]() {
              return config::mock_binding<size_t>(global_scratch_space);
          }),
          ss::sharded_parameter(
            [&]() { return config::mock_binding<double>(soft_limit_percent); }),
          ss::sharded_parameter(
            [&]() { return config::mock_binding<size_t>(block_size); }),
          ss::sharded_parameter([&]() { return test_arena_str; }),
          std::ref(test_log));
        co_await sharded_arena.invoke_on_all(&file_arena::start);
    }

    ss::future<> TearDownAsync() override { co_await sharded_arena.stop(); }

    ss::future<file_arena_manager*> get_core0_manager() {
        co_return co_await sharded_arena.invoke_on(
          file_arena::manager_shard,
          [](const auto& a) { return a._core0_arena_manager.get(); });
    }

    ssx::semaphore& local_disk_bytes_reservable(file_arena& a) {
        return a._local_disk_bytes_reservable;
    }

    size_t disk_bytes_reservable_total(file_arena_manager* m) {
        return m->_disk_bytes_reservable_total;
    }

    ssx::semaphore& disk_bytes_reservable(file_arena_manager* m) {
        return m->_disk_bytes_reservable;
    }

    ss::future<> reclaim_disk_space(file_arena_manager* m) {
        return m->reclaim_disk_space();
    }

    fake_io io{};
    ss::sharded<file_arena> sharded_arena;
};

TEST_F(FileArenaTestFixture, TrackedFileMemoryManagement) {
    static constexpr auto global_scratch_space = 100;
    static constexpr auto soft_limit_percent = 80.0;
    static constexpr auto block_size = 64;
    set_up_file_arena(global_scratch_space, soft_limit_percent, block_size)
      .get();

    auto& arena = sharded_arena.local();
    auto* m = get_core0_manager().get();
    ssx::semaphore& global_sem = disk_bytes_reservable(m);
    ssx::semaphore& local_sem = local_disk_bytes_reservable(arena);
    ss::abort_source as;

    static constexpr auto file_bytes = block_size;
    auto tracked_file_res = arena.create_tmp_file(file_bytes, as).get();
    ASSERT_TRUE(tracked_file_res.has_value());
    auto tracked_file = std::move(tracked_file_res).value();

    // The global reservable total hasn't changed, but the available number of
    // units has.
    ASSERT_EQ(disk_bytes_reservable_total(m), global_scratch_space);
    ASSERT_EQ(global_sem.current(), global_scratch_space - file_bytes);
    // Though, there is no excess in the local pool yet.
    ASSERT_EQ(local_sem.current(), 0);

    // Write some data to the staging file.
    auto output_stream = tracked_file.output_stream().get();
    static const ss::sstring str = "Hello, world!";
    output_stream.write(str.data(), str.size()).get();

    // Finalize the file (indicate there will be no more future writes). The
    // excess disk reservation will be returned to the local shard pool.
    tracked_file.finalize(str.size());

    // After finalizing, the difference between the allocated block size and the
    // size of the tracked_file on disk has been returned to the local pool.
    ASSERT_EQ(local_sem.current(), block_size - str.size());
    // Global pool has not changed.
    ASSERT_EQ(global_sem.current(), global_scratch_space - file_bytes);

    tracked_file.remove().get();

    // After removing, all units have been returned to the local pool.
    ASSERT_EQ(local_sem.current(), block_size);
    // Global pool has not changed.
    ASSERT_EQ(global_sem.current(), global_scratch_space - file_bytes);

    // Though, if we force a reclaim of disk space...
    reclaim_disk_space(m).get();

    // ...Local pool has its unused memory reclaimed...
    ASSERT_EQ(local_sem.current(), 0);

    //...and global pool is full again.
    ASSERT_EQ(global_sem.current(), global_scratch_space);
}

TEST_F(FileArenaTestFixture, MultipleTrackedFiles) {
    static constexpr auto global_scratch_space = 100;
    static constexpr auto soft_limit_percent = 80.0;
    static constexpr auto block_size = 64;
    set_up_file_arena(global_scratch_space, soft_limit_percent, block_size)
      .get();

    auto& arena = sharded_arena.local();
    auto* m = get_core0_manager().get();
    ssx::semaphore& global_sem = disk_bytes_reservable(m);
    ssx::semaphore& local_sem = local_disk_bytes_reservable(arena);
    ss::abort_source as;

    static constexpr auto file_bytes = block_size;
    auto tracked_file_res = arena.create_tmp_file(file_bytes, as).get();
    ASSERT_TRUE(tracked_file_res.has_value());
    auto tracked_file = std::move(tracked_file_res).value();

    // Write some data to the staging file.
    auto output_stream = tracked_file.output_stream().get();
    static const ss::sstring str = "Hello, world, here is 36 more bytes!";
    output_stream.write(str.data(), str.size()).get();

    // Issue a request for another file. It won't be able to resolve, as there
    // isn't enough space in the global pool.
    auto next_file_fut = arena.create_tmp_file(file_bytes, as);

    // Finalize the existing file, though, and there will be just enough space
    // for the next file in the local pool.
    tracked_file.finalize(str.size());

    auto next_file_res = std::move(next_file_fut).get();
    ASSERT_TRUE(next_file_res.has_value());
    auto next_tracked_file = std::move(next_file_res).value();

    ASSERT_EQ(local_sem.current(), 0);
    ASSERT_EQ(global_sem.current(), 0);

    tracked_file.remove().get();
    next_tracked_file.remove().get();

    // All the units are returned to the local pool.
    ASSERT_EQ(local_sem.current(), global_scratch_space);
    ASSERT_EQ(global_sem.current(), 0);
}
