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
#include "iceberg/table_metadata.h"
#include "json/document.h"

namespace iceberg {

sort_field parse_sort_field(const json::Value&);
sort_order parse_sort_order(const json::Value&);
table_metadata parse_table_meta(const json::Value&);

} // namespace iceberg

namespace json {

void rjson_serialize(iceberg::json_writer& w, const iceberg::sort_field& m);
void rjson_serialize(iceberg::json_writer& w, const iceberg::sort_order& m);
void rjson_serialize(iceberg::json_writer& w, const iceberg::table_metadata& m);

} // namespace json
