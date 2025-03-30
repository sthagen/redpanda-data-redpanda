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

#include "base/seastarx.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/sstring.hh>

namespace iceberg::rest_client {

struct catalog_config {
    // Properties that should be used as default configuration; applied before
    // client configuration.
    chunked_hash_map<ss::sstring, ss::sstring> defaults;

    // Properties that should be used to override client configuration; applied
    // after defaults and client configuration.
    chunked_hash_map<ss::sstring, ss::sstring> overrides;

    // TODO: optional: endpoints
};

} // namespace iceberg::rest_client
