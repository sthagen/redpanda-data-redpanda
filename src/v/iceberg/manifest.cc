/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/manifest.h"

namespace iceberg {

manifest_metadata manifest_metadata::copy() const {
    return manifest_metadata{
      .schema = schema.copy(),
      .partition_spec = partition_spec.copy(),
      .format_version = format_version,
      .manifest_content_type = manifest_content_type,
    };
}

manifest manifest::copy() const {
    manifest ret{
      .metadata = metadata.copy(),
    };
    ret.entries.reserve(entries.size());
    for (const auto& e : entries) {
        ret.entries.push_back(e.copy());
    }
    return ret;
}

} // namespace iceberg
