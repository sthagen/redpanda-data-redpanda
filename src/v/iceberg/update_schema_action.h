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

#include "iceberg/action.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

// Action for updating the current schema to the given schema.
// TODO: only adding new columns works. Handle removing or altering columns.
class update_schema_action : public action {
public:
    update_schema_action(const table_metadata& table, schema new_schema)
      : table_(table)
      , new_schema_(std::move(new_schema)) {}

protected:
    ss::future<action_outcome> build_updates() && final;

private:
    const table_metadata& table_;
    schema new_schema_;
};

} // namespace iceberg
