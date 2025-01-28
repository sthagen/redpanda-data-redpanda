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

#include "iceberg/transform.h"
#include "iceberg/values.h"

namespace iceberg {

// Transforms the given value to its appropriate Iceberg value based on the
// input transform.
//
// TODO: this is only implemented for the hourly transform on timestamp values!
// This will throw if used for anything else!
value apply_transform(const value&, const transform&);

} // namespace iceberg
