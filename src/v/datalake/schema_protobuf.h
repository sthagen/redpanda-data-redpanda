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

#include "datalake/conversion_outcome.h"
#include "google/protobuf/descriptor.h"
#include "iceberg/datatypes.h"

namespace datalake {

// convert a protobuf message schema to iceberg schema
conversion_outcome<iceberg::struct_type>
type_to_iceberg(const google::protobuf::Descriptor& pool);

} // namespace datalake
