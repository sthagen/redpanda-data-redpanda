/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/format_to.h"
#include "bytes/iobuf.h"

namespace compression {

enum class type : uint8_t {
    gzip,
    // NOTE: This is *NOT* standard snappy compression. It uses the java-snappy
    // framing compression which is fundamentally not compatible with upstream
    // google snappy
    java_snappy,
    lz4,
    zstd,
};

inline fmt::iterator format_to(type c, fmt::iterator out) {
    switch (c) {
    case type::gzip:
        return fmt::format_to(out, "gzip");
    case type::java_snappy:
        return fmt::format_to(out, "java_snappy");
    case type::lz4:
        return fmt::format_to(out, "lz4");
    case type::zstd:
        return fmt::format_to(out, "zstd");
    }
    return fmt::format_to(
      out, "compression::type::unknown({})", std::to_underlying(c));
}

// a very simple compressor. Exposes virtually no knobs and uses
// the defaults for all compressors. In the future, we can make these
// a virtual interface so we can instantiate them
struct compressor {
    static iobuf compress(const iobuf&, type);
    static iobuf uncompress(const iobuf&, type);
};

// A simple opinionated stream compressor.
//
// Will use stream compression when available, to defer to compressor.
struct stream_compressor {
    static ss::future<iobuf> compress(iobuf, type);
    static ss::future<iobuf> uncompress(iobuf, type);
};

} // namespace compression
