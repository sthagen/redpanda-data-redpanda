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

#include "lsm/core/internal/options.h"

namespace lsm::internal {

fmt::iterator options::level_config::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{number:{},max_total_bytes:{},max_file_size:{}}}",
      number,
      max_total_bytes,
      max_file_size);
}

fmt::iterator options::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{levels:{}}}", levels);
}

} // namespace lsm::internal
