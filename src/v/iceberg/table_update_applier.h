/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "iceberg/table_metadata.h"
#include "iceberg/table_update.h"

namespace iceberg::table_update {

enum class outcome {
    success = 0,

    // Something went wrong, e.g. some ID was missing that we expected to
    // exist. The table arg is left in an unspecified state.
    unexpected_state,
};

// Applies the given update to the given table metadata.
//
// NOTE: this is a deterministic update to the logical, in-memory metadata for
// actions that have occurred and now need to be reflected in the table. More
// complex operations (e.g. that require IO, like appending files and rewriting
// manifests) do not belong here.
outcome apply(const update&, table_metadata&);

} // namespace iceberg::table_update
