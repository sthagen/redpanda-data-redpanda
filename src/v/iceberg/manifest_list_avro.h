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
#include "iceberg/manifest_list.h"

namespace iceberg {

iobuf serialize_avro(const manifest_list&);
manifest_list parse_manifest_list(iobuf);

} // namespace iceberg
