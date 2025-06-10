/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "model/fundamental.h"
#include "model/record.h"
#include "panda_link/model/types.h"

namespace cluster::panda_link::testing {
model::record_batch
  create_upsert_command(model::offset, ::panda_link::model::metadata);
model::record_batch create_remove_command(::panda_link::model::name_t);
} // namespace cluster::panda_link::testing
