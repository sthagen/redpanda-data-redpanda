/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/ir_json.h"

namespace iceberg {

/// Converts given JSON conversion IR to iceberg struct type. If a top
/// level JSON type is different than JSON object the method will return a
/// struct type with single field being a top level type.
conversion_outcome<iceberg::struct_type>
type_to_iceberg(const json_conversion_ir&);

} // namespace iceberg
