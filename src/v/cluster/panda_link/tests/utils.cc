/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/panda_link/tests/utils.h"

#include "cluster/commands.h"

namespace cluster::panda_link::testing {

using ::panda_link::model::metadata;
using ::panda_link::model::name_t;

model::record_batch create_upsert_command(model::offset offset, metadata link) {
    cluster::panda_link_upsert_cmd cmd(0, std::move(link));
    auto batch = cluster::serde_serialize_cmd(std::move(cmd));
    batch.header().base_offset = offset;
    return batch;
}

model::record_batch create_remove_command(name_t name) {
    cluster::panda_link_remove_cmd cmd(std::move(name), 0);
    return cluster::serde_serialize_cmd(std::move(cmd));
}
} // namespace cluster::panda_link::testing
