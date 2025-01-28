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

#include "base/seastarx.h"
#include "iceberg/transform.h"

#include <seastar/core/sstring.hh>

namespace iceberg {

// NOTE: while there are no complex JSON types here, the transforms are
// expected to be serialized as a part of JSON files (e.g. table metadata).
ss::sstring transform_to_str(const transform&);
transform transform_from_str(std::string_view);

} // namespace iceberg
