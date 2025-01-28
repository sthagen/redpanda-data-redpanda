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

#include <seastar/util/bool_class.hh>

#include <avro/CustomAttributes.hh>
#include <avro/LogicalType.hh>
#include <avro/Schema.hh>

namespace iceberg {

// Translates the given field/type to the corresponding Avro schema.
// The resulting schema is annotated with Iceberg attributes (e.g. 'field-id',
// 'element-id').
avro::Schema field_to_avro(const nested_field& field);
avro::Schema struct_type_to_avro(const struct_type&, std::string_view name);

// Translates the given Avro schema into its corresponding field/type, throwing
// an exception if the schema is not a valid Iceberg schema (e.g. missing
// attributes).
nested_field_ptr
child_field_from_avro(const avro::NodePtr& parent, size_t child_idx);
using with_field_ids = ss::bool_class<struct field_id_tag>;
field_type type_from_avro(
  const avro::NodePtr&, with_field_ids with_ids = with_field_ids::yes);

} // namespace iceberg
