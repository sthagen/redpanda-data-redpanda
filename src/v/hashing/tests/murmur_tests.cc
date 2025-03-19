// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "hashing/murmur.h"

#include <seastar/core/temporary_buffer.hh>

#include <base/seastarx.h>
#include <gtest/gtest.h>

#include <array>
#include <ranges>

namespace {
// assuming an non-decreasing input, cap all elements with `limit` and discard
// repeating values after `limit` is returned first time
constexpr auto capped_with(size_t limit, std::ranges::range auto input) {
    std::vector<size_t> ret;
    bool seen_limit = false;
    for (const auto& item : input) {
        if (item < limit) {
            ret.push_back(item);
        } else {
            if (seen_limit) {
                break;
            }
            seen_limit = true;
            ret.push_back(limit);
        }
    }
    return ret;
}
} // namespace

TEST(MurmurTest, IobufEquivanetToContinuous) {
    constexpr auto total_sizes = std::to_array<size_t>(
      {0, 1, 2, 3, 4, 5, 6, 7, 8, 100, 1000, 1001, 1002, 1003});
    constexpr auto chunk_sizes = std::to_array<size_t>(
      {0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 100, 101, 102, 103, 104});

    for (auto len : total_sizes) {
        // char span
        const std::vector continuous{
          std::from_range,
          std::views::iota(size_t{0}, len)
            | std::views::transform([](char c) -> char { return c ^ 13; })};
        auto continuous_hashed = murmurhash3_x86_32(
          continuous.data(), continuous.size());

        // 1-chunk iobuf
        {
            iobuf single_chunk{std::to_array({ss::temporary_buffer<char>{
              continuous.data(), continuous.size()}})};
            auto single_hashed = murmurhash3_x86_32(single_chunk);
            ASSERT_EQ(continuous_hashed, single_hashed);
        }

        // 2-chunk iobuf
        for (auto chunk1_size : capped_with(continuous.size(), chunk_sizes)) {
            ss::temporary_buffer chunk1{continuous.data(), chunk1_size};
            ss::temporary_buffer chunk2{
              continuous.data() + chunk1_size, continuous.size() - chunk1_size};
            iobuf double_chunk{
              std::to_array({std::move(chunk1), std::move(chunk2)})};
            auto double_hashed = murmurhash3_x86_32(double_chunk);
            ASSERT_EQ(continuous_hashed, double_hashed);
        }

        // 3-chunk iobuf
        for (auto chunk1_size : capped_with(continuous.size(), chunk_sizes)) {
            for (auto chunk2_size :
                 capped_with(continuous.size() - chunk1_size, chunk_sizes)) {
                ss::temporary_buffer chunk1{continuous.data(), chunk1_size};
                ss::temporary_buffer chunk2{
                  continuous.data() + chunk1_size, chunk2_size};
                ss::temporary_buffer chunk3{
                  continuous.data() + chunk1_size + chunk2_size,
                  continuous.size() - chunk1_size - chunk2_size};
                iobuf triple_chunk{std::to_array(
                  {std::move(chunk1), std::move(chunk2), std::move(chunk3)})};
                auto triple_hashed = murmurhash3_x86_32(triple_chunk);
                ASSERT_EQ(continuous_hashed, triple_hashed);
            }
        }
    }
} // namespace
