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

#include "base/outcome.h"
#include "iceberg/unresolved_partition_spec.h"

namespace datalake {

// Parse unresolved_partition_spec from a spark-like DDL expression string
// (ex.: "(hour(redpanda.timestamp), other_field)").
checked<iceberg::unresolved_partition_spec, ss::sstring>
parse_partition_spec(const std::string_view&);

} // namespace datalake
