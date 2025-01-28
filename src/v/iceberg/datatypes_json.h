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
#include "json/document.h"

namespace iceberg {

struct_type parse_struct(const json::Value&);
list_type parse_list(const json::Value&);
map_type parse_map(const json::Value&);
field_type parse_type(const json::Value&);
nested_field_ptr parse_field(const json::Value&);

} // namespace iceberg

namespace json {

void rjson_serialize(iceberg::json_writer& w, const iceberg::nested_field& f);
void rjson_serialize(iceberg::json_writer& w, const iceberg::primitive_type& t);
void rjson_serialize(iceberg::json_writer& w, const iceberg::struct_type& t);
void rjson_serialize(iceberg::json_writer& w, const iceberg::list_type& t);
void rjson_serialize(iceberg::json_writer& w, const iceberg::map_type& t);
void rjson_serialize(iceberg::json_writer& w, const iceberg::field_type& t);

} // namespace json
