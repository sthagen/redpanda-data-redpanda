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

#include "cluster_link/model/sr_context_mapping.h"

#include <algorithm>

namespace cluster_link::model {

bool filter_selects_source_context(
  const schema_registry_sync_config::source_filter& filter,
  const pandaproxy::schema_registry::context& source_context) {
    namespace ppsr = pandaproxy::schema_registry;
    // An empty filter selects every source context.
    if (filter.contexts.empty() && filter.subjects.empty()) {
        return true;
    }
    if (std::ranges::contains(filter.contexts, source_context())) {
        return true;
    }
    return std::ranges::any_of(
      filter.subjects, [&source_context](const auto& subject) {
          const auto parsed = ppsr::context_subject::from_string(
            subject, ppsr::qualified_subjects_enabled::yes);
          return parsed.ctx == source_context;
      });
}

} // namespace cluster_link::model
