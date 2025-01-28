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
#include "iceberg/schema.h"
#include "json/document.h"

namespace iceberg {

schema parse_schema(const json::Value&);

} // namespace iceberg

namespace json {

void rjson_serialize(iceberg::json_writer& w, const iceberg::schema& s);

} // namespace json
