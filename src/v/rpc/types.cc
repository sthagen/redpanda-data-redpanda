// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/types.h"

#include "hashing/crc32c.h"

#include <seastar/core/byteorder.hh>

#include <ostream>

namespace rpc {
template<typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
void crc_one(crc::crc32c& crc, T t) {
    T args_le = ss::cpu_to_le(t);
    crc.extend(args_le);
}

uint32_t checksum_header_only(const header& h) {
    auto crc = crc::crc32c();
    crc_one(
      crc,
      static_cast<std::underlying_type_t<compression_type>>(h.compression));
    crc_one(crc, h.payload_size);
    crc_one(crc, h.meta);
    crc_one(crc, h.correlation_id);
    crc_one(crc, h.payload_checksum);
    return crc.value();
}

fmt::iterator header::format_to(fmt::iterator it) const {
    // NOTE: if we use the int8_t types, ostream doesn't print 0's
    // artificially cast version and compression as ints
    return fmt::format_to(
      it,
      "{{version:{}, header_checksum:{}, compression:{}, payload_size:{}, "
      "meta:{}, correlation_id:{}, payload_checksum:{}}}",
      int(version),
      header_checksum,
      static_cast<int>(compression),
      payload_size,
      meta,
      correlation_id,
      payload_checksum);
}

} // namespace rpc
