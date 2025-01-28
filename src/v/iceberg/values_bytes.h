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

#include "bytes/bytes.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"

namespace iceberg {

bytes value_to_bytes(const value&);
value value_from_bytes(const field_type& type, const bytes&);

} // namespace iceberg
