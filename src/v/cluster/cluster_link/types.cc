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

#include "cluster/cluster_link/types.h"

#include "base/format_to.h"

#include <fmt/ranges.h>

namespace cluster::cluster_link {

fmt::iterator topic_result::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{topic: {}, result: {}}}", topic, ec);
}
} // namespace cluster::cluster_link
