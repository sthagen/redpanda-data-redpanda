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

#include "iceberg/datatypes.h"

namespace iceberg {

// Expected type from test_nested_schema_json_str.
field_type test_nested_schema_type();

// Contains additional fields that are currently supported by avro:
// decimals, fixed and UUID
field_type test_nested_schema_type_avro();

// Nested schema taken from
// https://github.com/apache/iceberg-go/blob/704a6e78c13ea63f1ff4bb387f7d4b365b5f0f82/schema_test.go#L644
extern const char* test_nested_schema_json_str;

} // namespace iceberg
