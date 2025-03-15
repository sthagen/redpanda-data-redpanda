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

#include "iceberg/action.h"
#include "iceberg/table_metadata.h"
#include "iceberg/unresolved_partition_spec.h"
#include "utils/prefix_logger.h"

namespace iceberg {

// Action that adds a new partition spec to the table and sets it as a default
// spec.
class update_partition_spec_action : public action {
public:
    update_partition_spec_action(
      const table_metadata& table, unresolved_partition_spec new_spec);

protected:
    ss::future<action_outcome> build_updates() && final;

private:
    const table_metadata& table_;
    const unresolved_partition_spec new_spec_;
    prefix_logger logger_;
};

} // namespace iceberg
