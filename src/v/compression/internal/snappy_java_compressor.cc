// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/internal/snappy_java_compressor.h"

#include "base/likely.h"
#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/details/io_iterator_consumer.h"
#include "bytes/iobuf.h"
#include "compression/snappy_standard_compressor.h"
#include "snappy-sinksource.h"
#include "snappy.h"

#include <fmt/format.h>

#include <cstring>

namespace compression::internal {

size_t find_max_size_in_frags(const iobuf& x) {
    size_t ret = 0;
    for (const auto& f : x) {
        if (f.size() > ret) {
            ret = f.size();
        }
    }
    return snappy::MaxCompressedLength(ret);
}

template<typename T, typename = std::enable_if_t<std::is_integral_v<T>, T>>
void append_be(iobuf& o, T t) {
    auto x = ss::cpu_to_be(t);
    // NOLINTNEXTLINE
    o.append((const char*)&x, sizeof(x));
}
template<typename T, typename = std::enable_if_t<std::is_integral_v<T>, T>>
void append_le(iobuf& o, T t) {
    auto x = ss::cpu_to_le(t);
    // NOLINTNEXTLINE
    o.append((const char*)&x, sizeof(x));
}
iobuf snappy_java_compressor::compress(const iobuf& x) {
    iobuf ret;
    ret.append(
      snappy_magic::java_magic.data(), snappy_magic::java_magic.size());
    // versions in header are big-endian. See:
    // https://github.com/xerial/snappy-java/blob/65e1ec3de1a0d447b137c6dd6393629aa3d75b8b/src/main/java/org/xerial/snappy/SnappyCodec.java#L78-L81
    append_be(ret, snappy_magic::default_version);
    append_be(ret, snappy_magic::min_compatible_version);
    // staging buffer
    ss::temporary_buffer<char> obuf(find_max_size_in_frags(x));
    for (const auto& f : x) {
        // do compression
        size_t omax = obuf.size();
        snappy::RawCompress(f.get(), f.size(), obuf.get_write(), &omax);
        // must be int32 to be compatible && in big endian
        append_be(ret, int32_t(omax));
        ret.append(obuf.get(), omax);
    }
    return ret;
}
iobuf snappy_java_compressor::uncompress(const iobuf& x) {
    auto iter = details::io_iterator_consumer(x.cbegin(), x.cend());
    if (unlikely(x.size_bytes() < snappy_magic::header_len)) {
        return snappy_standard_compressor::uncompress(x);
    }
    std::array<uint8_t, snappy_magic::java_magic.size()> magic_compare{};
    iter.consume_to(magic_compare.size(), magic_compare.data());
    if (unlikely(snappy_magic::java_magic != magic_compare)) {
        return snappy_standard_compressor::uncompress(x);
    }
    // Previously, these version fields were erroneously written with
    // little-endian encoding. They are now corrected to be written and decoded
    // using big-endian. Additionally, there was previously a version check
    // here. It has been removed due to incorrect implementation, and because
    // most other snappy clients do not perform checks around these fields.
    [[maybe_unused]] const auto version = iter.consume_be_type<int32_t>();
    [[maybe_unused]] const auto min_version = iter.consume_be_type<int32_t>();
    // stream decoder next
    iobuf ret;
    const size_t input_bytes = x.size_bytes();
    while (iter.bytes_consumed() != input_bytes) {
        auto compressed_length = iter.consume_be_type<int32_t>();
        // iobuf doesn't have a const compatible share interface so we make a
        // copy here which is inefficient compared to a zero-copy approach.
        auto chunk = iobuf_copy(iter, compressed_length);
        auto output_size = snappy_standard_compressor::get_uncompressed_length(
          chunk);
        snappy_standard_compressor::uncompress_append(chunk, ret, output_size);
    }
    return ret;
}

} // namespace compression::internal
