/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/json_writer.h"
#include "iceberg/schema.h"
#include "iceberg/schema_json.h"
#include "iceberg/tests/test_schemas.h"
#include "json/document.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(SchemaJsonSerde, TestNestedSchema) {
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_nested_schema_json_str);
    auto parsed_schema = parse_schema(parsed_orig_json);

    field_type expected_type = test_nested_schema_type();
    ASSERT_EQ(
      parsed_schema.schema_struct, std::get<struct_type>(expected_type));
    ASSERT_EQ(parsed_schema.schema_id(), 1);
    ASSERT_EQ(parsed_schema.identifier_field_ids.size(), 1);
    ASSERT_EQ((*parsed_schema.identifier_field_ids.begin())(), 1);

    const ss::sstring parsed_orig_as_str = to_json_str(parsed_schema);
    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    auto parsed_roundtrip_schema = parse_schema(parsed_roundtrip_json);

    ASSERT_EQ(parsed_roundtrip_schema, parsed_schema);
}
