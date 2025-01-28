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

#include "bytes/iobuf.h"
#include "iceberg/manifest.h"
#include "iceberg/partition_key_type.h"

namespace iceberg {

iobuf serialize_avro(const manifest&);
manifest parse_manifest(const partition_key_type&, iobuf);

} // namespace iceberg
