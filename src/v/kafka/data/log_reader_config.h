// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/format_to.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"

#include <functional>
#include <optional>

namespace kafka {

struct log_reader_config {
    kafka::offset start_offset;
    kafka::offset max_offset;
    size_t min_bytes;
    size_t max_bytes;

    /// \brief guarantees first_timestamp >= record_batch.first_timestamp
    /// it is the std::lower_bound
    std::optional<model::timestamp> first_timestamp;

    /// abort source for read operations
    model::opt_abort_source_t abort_source;

    model::opt_client_address_t client_address;

    // do not let the lower level readers go over budget even when that means
    // that the reader will return no batches.
    bool strict_max_bytes{false};

    log_reader_config(
      kafka::offset start_offset,
      kafka::offset max_offset,
      size_t min_bytes,
      size_t max_bytes,
      std::optional<model::timestamp> time,
      model::opt_abort_source_t as,
      model::opt_client_address_t client_addr = std::nullopt,
      bool strict_max_bytes = false)
      : start_offset(start_offset)
      , max_offset(max_offset)
      , min_bytes(min_bytes)
      , max_bytes(max_bytes)
      , first_timestamp(time)
      , abort_source(as)
      , client_address(std::move(client_addr))
      , strict_max_bytes(strict_max_bytes) {}

    /**
     * Read offsets [start, end].
     */
    log_reader_config(
      kafka::offset start_offset,
      kafka::offset max_offset,
      model::opt_abort_source_t as = std::nullopt,
      model::opt_client_address_t client_addr = std::nullopt)
      : log_reader_config(
          start_offset,
          max_offset,
          0,
          std::numeric_limits<size_t>::max(),
          std::nullopt,
          as,
          std::move(client_addr),
          false) {}

    fmt::iterator format_to(fmt::iterator it) const;
};

} // namespace kafka
