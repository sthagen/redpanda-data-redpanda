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

#include "cluster/drain_status.h"

namespace cluster {

fmt::iterator drain_status::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{finished: {}, errors: {}, partitions: {}, eligible: {}, transferring: "
      "{}, failed: {}}}",
      finished,
      errors,
      partitions,
      eligible,
      transferring,
      failed);
}

} // namespace cluster
