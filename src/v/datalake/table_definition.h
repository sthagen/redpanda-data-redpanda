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

#include "datalake/schema_descriptor.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"

namespace datalake {

// A single canonical descriptor set used for:
//   - build_value() compile-time name/arity checking
//   - total_fields() / index_of<> field-index lookups
//   - rp_base_next_field_id

/// Canonical header key/value descriptor (binary value — used for
/// value construction; type is overridden at runtime when building the schema).
using header_kv_desc = struct_desc<
  field_desc<"key", iceberg::string_type>,
  field_desc<"value", iceberg::binary_type>>;

/// Canonical redpanda system struct descriptor.
using rp_desc = struct_desc<
  field_desc<"partition", iceberg::int_type>,
  field_desc<"offset", iceberg::long_type>,
  field_desc<"timestamp", iceberg::timestamptz_type>,
  field_desc<"headers", list_desc<header_kv_desc>>,
  field_desc<"key", iceberg::binary_type>,
  field_desc<"timestamp_type", iceberg::int_type>>;

/// Canonical top-level base row descriptor: the "redpanda" system fields.
/// Translators extend this by appending value/schema columns.
using rp_base_desc = struct_desc<field_desc<"redpanda", rp_desc>>;

inline constexpr std::string_view rp_struct_name = "redpanda";

/// Next available pre-assignment field ID after the base row descriptor.
/// Translators that add fields should start IDs from here.
inline const int rp_base_next_field_id = rp_base_desc::total_fields();

/// Build the base row struct_type, applying headers_config overrides
/// (e.g. promote header values from binary to string).
iceberg::struct_type
rp_base_struct_type(model::iceberg_mode::headers_config headers_cfg);

/// Build the redpanda system struct_value. Single definition used
/// by all translators. `key` is the already-decoded key value: binary_value
/// for raw-bytes mode, or a schema-decoded value for schema-mode keys.
std::unique_ptr<iceberg::struct_value> build_rp_struct(
  model::partition_id pid,
  kafka::offset o,
  std::optional<iceberg::value> key,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers,
  model::iceberg_mode::headers_config);

/// Get the redpanda struct_type from a base row struct_type.
inline iceberg::struct_type& rp_struct_type(iceberg::struct_type& row) {
    return std::get<iceberg::struct_type>(
      type_field<rp_base_desc, "redpanda">(row).type);
}

/// Get the redpanda struct_value from a data row.
inline iceberg::struct_value& rp_struct_value(iceberg::struct_value& row) {
    return *std::get<std::unique_ptr<iceberg::struct_value>>(
      value_field<rp_base_desc, "redpanda">(row).value());
}

} // namespace datalake
