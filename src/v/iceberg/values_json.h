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
#include "iceberg/json_writer.h"
#include "iceberg/values.h"
#include "json/document.h"

/**
 * values_json.h - Implements JSON single-value serialization for Iceberg values
 * As specified in:
 *   https://iceberg.apache.org/spec/#json-single-value-serialization
 *
 * This is needed processing default values for nested field in Iceberg schemas,
 * to/from their representation in table metadata.
 *
 */

namespace iceberg {

/**
 * value_from_json - Take a parsed json::Value (rapidjson) and produce an
 * Iceberg value subject to the provided field_type.
 *
 * @param json::Value    - The source JSON
 * @param field_type     - The expected iceberg type
 * @param field_required - If the JSON is null, either return nullopt
 *                         or raise an exception, depending on requiredness
 * @return The resulting iceberg::value. Throw invalid_argument on semantic
 *         errors during conversion (e.g. type mismatch)
 */
std::optional<value>
value_from_json(const json::Value&, const field_type&, field_required);

/**
 * value_to_json - Write the provided iceberg::value to JSON subject to the
 * provided field_type.
 *
 * @param json_writer - JSON writer sink
 * @param value       - The value to serialize
 * @param field_type  - The ostensible type of the value
 *
 * Throws invalid_argument on semantic errors during serialization (e.g. type
 * mismatch)
 */
void value_to_json(
  iceberg::json_writer&, const iceberg::value&, const iceberg::field_type&);
} // namespace iceberg
