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

#include "iceberg/json_writer.h"
#include "iceberg/snapshot.h"
#include "json/document.h"

namespace iceberg {

snapshot parse_snapshot(const json::Value&);
snapshot_reference parse_snapshot_ref(const json::Value&);

} // namespace iceberg

namespace json {

void rjson_serialize(iceberg::json_writer& w, const iceberg::snapshot& s);
void rjson_serialize(
  iceberg::json_writer& w, const iceberg::snapshot_reference& s);

// serializes snapshot reference properties without writing start and end object
// markers
void serialize_snapshot_reference_properties(
  iceberg::json_writer& w, const iceberg::snapshot_reference& s);

} // namespace json
