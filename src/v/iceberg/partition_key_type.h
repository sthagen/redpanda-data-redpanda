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

namespace iceberg {

struct partition_spec;
struct schema;

// The struct type transformed from an original schema by the transforms
// defined in a given partition spec. The high level relationship between the
// partition key type and related abstractions is as follows:
//
//   partition_spec  +  schema   =>  partition_key_type
//   (target fields)    (fields)     (transformed fields)
//   ( + transforms)
//
// The partition_key_type is expected to be serialized as the Avro schema of
// the r102 field of the manifest_entry.
struct partition_key_type {
    // NOTE: in accordance with the Iceberg spec, this type is not deeply
    // nested and is comprised of a few primitive types.
    struct_type type;

    // Constructs the appropriate partition key type from the given partition
    // spec and schema.
    static partition_key_type create(const partition_spec&, const schema&);

    partition_key_type copy() const;
};

} // namespace iceberg
