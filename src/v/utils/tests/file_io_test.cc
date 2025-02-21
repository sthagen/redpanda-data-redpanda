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
#include "bytes/random.h"
#include "utils/file_io.h"

#include <gtest/gtest.h>

class ReadFully : public ::testing::TestWithParam<size_t> {};

TEST_P(ReadFully, RoundTrip) {
    const auto input = random_generators::make_iobuf(GetParam());
    write_fully("out.dat", input.copy()).get();
    const auto output = read_fully("out.dat").get();
    EXPECT_EQ(input, output);
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
