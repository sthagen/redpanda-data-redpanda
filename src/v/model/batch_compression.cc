// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/batch_compression.h"

#include "bytes/iobuf.h"
#include "compression/compression.h"

#include <seastar/core/coroutine.hh>

#include <stdexcept>

namespace model {

namespace {
::compression::type to_compression_type(model::compression c) {
    switch (c) {
    case model::compression::gzip:
        return ::compression::type::gzip;
    case model::compression::snappy:
        return ::compression::type::java_snappy;
    case model::compression::lz4:
        return ::compression::type::lz4;
    case model::compression::zstd:
        return ::compression::type::zstd;
    case model::compression::none:
    case model::compression::count:
    case model::compression::producer:
        break;
    }
    throw std::runtime_error(
      fmt::format("Unconvertable compression type: {}", c));
}
} // namespace

ss::future<model::record_batch> decompress_batch(const model::record_batch& b) {
    if (!b.compressed()) [[unlikely]] {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Asked to decompressed a non-compressed batch:{}",
          b.header()));
    }
    // TODO: For zstd we have a non-reactor-stall version of uncompress that is
    // async, but currently the API requires ownership of the iobuf, but at
    // least one consumer of this API uses a const model::record_batch&
    //
    // We should consider a version of async uncompress that takes a const
    // iobuf& where the caller owns the lifetime of the iobuf.
    iobuf body_buf = ::compression::compressor::uncompress(
      b.data(), to_compression_type(b.header().attrs.compression()));
    // must remove compression first!
    auto h = b.header();
    h.attrs.remove_compression();
    h.reset_size_checksum_metadata(body_buf);
    auto batch = model::record_batch(
      h, std::move(body_buf), model::record_batch::tag_ctor_ng{});
    co_return batch;
}

model::record_batch decompress_batch_sync(const model::record_batch& b) {
    if (!b.compressed()) [[unlikely]] {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Asked to decompressed a non-compressed batch:{}",
          b.header()));
    }
    iobuf body_buf = ::compression::compressor::uncompress(
      b.data(), to_compression_type(b.header().attrs.compression()));
    // must remove compression first!
    auto h = b.header();
    h.attrs.remove_compression();
    h.reset_size_checksum_metadata(body_buf);
    auto batch = model::record_batch(
      h, std::move(body_buf), model::record_batch::tag_ctor_ng{});
    return batch;
}

ss::future<model::record_batch>
compress_batch(model::compression c, model::record_batch b) {
    if (b.compressed()) [[unlikely]] {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Asked to compress a batch that is already compressed: {}",
          b.header()));
    }
    if (c == model::compression::none) [[unlikely]] {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Asked to compress a batch using compression type none"));
    }
    model::record_batch_header h = b.header();
    auto payload = co_await ::compression::stream_compressor::compress(
      std::move(b).release_data(), to_compression_type(c));
    // compression bit must be set first!
    h.attrs |= c;
    h.reset_size_checksum_metadata(payload);
    auto batch = model::record_batch(
      h, std::move(payload), model::record_batch::tag_ctor_ng{});
    co_return batch;
}

model::record_batch
compress_batch_sync(model::compression c, model::record_batch b) {
    if (b.compressed()) [[unlikely]] {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Asked to compress a batch that is already compressed: {}",
          b.header()));
    }
    if (c == model::compression::none) [[unlikely]] {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Asked to compress a batch using compression type none"));
    }
    model::record_batch_header h = b.header();
    auto payload = ::compression::compressor::compress(
      std::move(b).release_data(), to_compression_type(c));
    // compression bit must be set first!
    h.attrs |= c;
    h.reset_size_checksum_metadata(payload);
    auto batch = model::record_batch(
      h, std::move(payload), model::record_batch::tag_ctor_ng{});
    return batch;
}

} // namespace model
