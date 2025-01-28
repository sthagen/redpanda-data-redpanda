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
#include "base/seastarx.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/sstring.hh>

namespace iceberg {
struct storage_credentials {
    ss::sstring prefix;
    chunked_hash_map<ss::sstring, ss::sstring> config;
};

} // namespace iceberg
