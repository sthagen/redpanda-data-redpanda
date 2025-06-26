// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_topics/types.h"
#include "model/fundamental.h"

#include <fmt/core.h>

namespace experimental::cloud_topics {

// This struct contains extent information (location of the byte slice in the
// object + the object uuid) and the kafka metadata (base offset and committed
// offset). It can be used to reference individual record batch or the continuos
// run that belongs to the same partition.
//
// Timestamps are not here because the timequery is handled in the metadata
// layer and the results of this are converted to kafka offsets.
//
// The type is generic and is supposed to work with both L0 and L1. When we will
// settle on the object name format for both L0 and L1 it will have to be
// updated to reflect this.
//
// The kafka offsets are not strictly necessary but they are used by the L0 read
// path. Without them the read path will end up being more complicated because
// the offsets will have to be written into the header after they're fetched.
// Putting this fields into the extent_meta allows data layer to put correct
// offsets into the headers before they are returned to the caller.
struct extent_meta {
    // Extent information
    object_id id;
    // TODO: the extent meta struct has to be updated
    // to match the RFC.
    first_byte_offset_t first_byte_offset;
    byte_range_size_t byte_range_size;

    // Kafka metadata
    kafka::offset base_offset;
    kafka::offset last_offset;
};

} // namespace experimental::cloud_topics

template<>
struct fmt::formatter<experimental::cloud_topics::extent_meta>
  : fmt::formatter<std::string_view> {
    template<class Context>
    constexpr auto format(
      const experimental::cloud_topics::extent_meta& o, Context& ctx) const {
        return format_to(
          ctx.out(),
          "{{id:{}, first_byte_offset:{}, byte_range_size:{}, "
          "base_offset:{}, committed_offset:{}}}",
          o.id,
          o.first_byte_offset,
          o.byte_range_size,
          o.base_offset,
          o.last_offset);
    }
};
