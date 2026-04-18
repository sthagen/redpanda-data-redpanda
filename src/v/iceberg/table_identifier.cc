/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/table_identifier.h"

namespace iceberg {
fmt::iterator table_identifier::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ns: {}, table: {}}}", fmt::join(ns, "/"), table);
}
} // namespace iceberg
