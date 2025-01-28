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
#include "iceberg/values.h"

#include <avro/Node.hh>

namespace avro {
class GenericDatum;
} // namespace avro

namespace iceberg {

// Serializes the given struct value with the given struct type as an Avro
// iobuf. The given struct name will be included in the Avro schema, and the
// given metadata will be included in the Avro header.
//
// XXX: only use this for Iceberg manifest metadata! Not all Avro types are
// implemented yet.
avro::GenericDatum
struct_to_avro(const struct_value& v, const avro::NodePtr& avro_schema);

// Parses the givn Avro datum and returns the corresponding value.
std::optional<value> val_from_avro(
  const avro::GenericDatum& d,
  const field_type& expected_type,
  field_required required);

} // namespace iceberg
