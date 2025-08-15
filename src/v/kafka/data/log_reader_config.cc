// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "log_reader_config.h"

#include "base/format_to.h"
#include "utils/to_string.h"

#include <ostream>

namespace kafka {

fmt::iterator log_reader_config::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "start_offset:{}, max_offset:{}, min_bytes:{}, max_bytes:{}, "
      "strict_max_bytes:{}, first_timestamp:{}, abortable:{}, "
      "client_address:{}",
      start_offset,
      max_offset,
      min_bytes,
      max_bytes,
      strict_max_bytes,
      first_timestamp,
      abort_source.has_value(),
      client_address);
}

} // namespace kafka
