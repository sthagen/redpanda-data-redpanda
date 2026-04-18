/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/inventory/types.h"

#include "model/metadata.h"

namespace {
constexpr auto supported_backends = {model::cloud_storage_backend::aws};
}

namespace cloud_storage::inventory {

bool validate_backend_supported_for_inventory_scrub(
  model::cloud_storage_backend backend) {
    return std::ranges::find(supported_backends, backend)
           != supported_backends.end();
}

} // namespace cloud_storage::inventory
