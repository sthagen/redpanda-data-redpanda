/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/parquet/bloom_filter.h"

#include "serde/thrift/compact.h"
#include "utils/vint.h"

#include <algorithm>
#include <bit>
#include <cmath>

namespace serde::parquet {

namespace {

// From the Apache Parquet split-block Bloom filter specification.
// NOLINTNEXTLINE(*magic-number*)
constexpr uint32_t salt[8] = {
  0x47b6137bu,
  0x44974d91u,
  0x8824ad5bu,
  0xa2b7289du,
  0x705495c7u,
  0x2df1424bu,
  0x9efc4947u,
  0x5c6bfb31u,
};

void block_insert(uint32_t* block, uint32_t item_hash) {
    for (int i = 0; i < 8; ++i) {
        uint32_t bit = (item_hash * salt[i]) >> 27u;
        block[i] |= (1u << bit);
    }
}

bool block_check(const uint32_t* block, uint32_t item_hash) {
    for (int i = 0; i < 8; ++i) {
        uint32_t bit = (item_hash * salt[i]) >> 27u;
        if (!(block[i] & (1u << bit))) {
            return false;
        }
    }
    return true;
}

// Encode a Thrift union with its first variant selected as an empty struct
// (the pattern used by BloomFilterAlgorithm, BloomFilterHash,
// and BloomFilterCompression in the Parquet spec).
iobuf encode_first_variant_selected() {
    thrift::struct_encoder inner;
    inner.write_field(
      thrift::field_id(1),
      thrift::field_type::structure,
      thrift::struct_encoder::empty_struct);
    return std::move(inner).write_stop();
}

} // namespace

// static
size_t bloom_filter::num_words_for_ndv(size_t ndv) {
    // Optimal bits per item for ~1% FPP: -ln(0.01) / ln(2)^2 ≈ 9.585.
    // Round up to the nearest whole block (8 words = 256 bits = 32 bytes).
    constexpr double bits_per_item = 9.585;
    size_t num_bits = static_cast<size_t>(
      std::ceil(static_cast<double>(ndv) * bits_per_item));
    size_t num_blocks = std::max<size_t>(1, (num_bits + 255) / 256);
    return num_blocks * 8;
}

bloom_filter::bloom_filter(size_t expected_ndv)
  : _bitset(num_words_for_ndv(expected_ndv), 0) {}

void bloom_filter::insert(uint64_t hash) {
    size_t num_blocks = _bitset.size() / 8;
    auto h_lo = static_cast<uint32_t>(hash);
    auto h_hi = static_cast<uint32_t>(hash >> 32u);
    size_t block_index = (static_cast<uint64_t>(h_hi) * num_blocks) >> 32u;
    block_insert(&_bitset[block_index * 8], h_lo);
}

bool bloom_filter::check(uint64_t hash) const {
    size_t num_blocks = _bitset.size() / 8;
    auto h_lo = static_cast<uint32_t>(hash);
    auto h_hi = static_cast<uint32_t>(hash >> 32u);
    size_t block_index = (static_cast<uint64_t>(h_hi) * num_blocks) >> 32u;
    return block_check(&_bitset[block_index * 8], h_lo);
}

void bloom_filter::reset() { std::fill(_bitset.begin(), _bitset.end(), 0u); }

double bloom_filter::fill_ratio() const {
    size_t set_bits = 0;
    for (uint32_t w : _bitset) {
        set_bits += std::popcount(w);
    }
    return static_cast<double>(set_bits)
           / static_cast<double>(_bitset.size() * 32);
}

void bloom_filter::serialize(iobuf& out) const {
    // BloomFilterHeader: numBytes, algorithm=BLOCK, hash=XXHASH,
    // compression=UNCOMPRESSED.
    thrift::struct_encoder header;
    header.write_field(
      thrift::field_id(1),
      thrift::field_type::i32,
      vint::to_bytes(static_cast<int64_t>(size_bytes())));
    header.write_field(
      thrift::field_id(2),
      thrift::field_type::structure,
      encode_first_variant_selected());
    header.write_field(
      thrift::field_id(3),
      thrift::field_type::structure,
      encode_first_variant_selected());
    header.write_field(
      thrift::field_id(4),
      thrift::field_type::structure,
      encode_first_variant_selected());
    out.append(std::move(header).write_stop());

    // Raw bitset bytes. The split-block algorithm operates on little-endian
    // 32-bit words; on little-endian hosts (x86-64, aarch64 LE) this is a
    // direct copy of the in-memory representation, which is the expected
    // layout for Parquet readers.
    static_assert(
      std::endian::native == std::endian::little,
      "bloom filter serialization assumes little-endian word layout; "
      "insert byteswap per word for big-endian support");
    out.append(
      // NOLINTNEXTLINE(*reinterpret-cast*)
      reinterpret_cast<const uint8_t*>(_bitset.data()),
      _bitset.size() * sizeof(uint32_t));
}

} // namespace serde::parquet
