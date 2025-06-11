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
#include "base/units.h"
#include "test_utils/random_bytes.h"
#include "utils/file_io.h"

#include <seastar/core/file.hh>

#include <gtest/gtest.h>

#include <filesystem>

class ReadFully : public ::testing::TestWithParam<size_t> {};

TEST_P(ReadFully, RoundTrip) {
    const auto input = tests::random_iobuf(GetParam());
    write_fully("out.dat", input.copy()).get();
    const auto output = read_fully("out.dat").get();
    EXPECT_EQ(input, output);
}

TEST(MaybeRemoveFile, FileIsRemoved) {
    auto file = ss::sstring("panda_world.dat");
    ss::open_file_dma(file, ss::open_flags::create).get();
    EXPECT_TRUE(ss::file_exists(file).get());
    EXPECT_NO_THROW(maybe_remove_file(file).get());
    EXPECT_FALSE(ss::file_exists(file).get());
}

TEST(MaybeRemoveFile, MissingFileNoThrownException) {
    auto file = "panda_world.dat";
    EXPECT_FALSE(ss::file_exists(file).get());
    EXPECT_NO_THROW(maybe_remove_file(file).get());
}

TEST(MaybeRemoveFile, OtherErrorThrowsException) {
    // Using a non-empty directory removal attempt to demonstrate that any
    // exception thrown other than a missing file is propagated by
    // maybe_remove_file().
    auto outer_dir = "panda_world";
    auto dir = ss::sstring(outer_dir) + ss::sstring("/panda_city");
    ss::recursive_touch_directory(dir).get();
    EXPECT_THROW(
      maybe_remove_file(outer_dir).get(), std::filesystem::filesystem_error);
}

namespace {
std::vector<uint64_t> test_case_sizes() {
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
    auto sizes = std::vector<size_t>{
      0,
      1,
      2,
      3,
      4_KiB - 2,
      4_KiB - 1,
      4_KiB,
      4_KiB + 1,
      4_KiB + 2,
      512_KiB - 2,
      512_KiB - 1,
      512_KiB,
      512_KiB + 1,
      512_KiB + 2,
    };
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
    for (auto size : details::io_allocation_size::alloc_table) {
        sizes.push_back(size);
    }
    return sizes;
}
} // namespace

INSTANTIATE_TEST_SUITE_P(
  SizeRange, ReadFully, ::testing::ValuesIn(test_case_sizes()));
