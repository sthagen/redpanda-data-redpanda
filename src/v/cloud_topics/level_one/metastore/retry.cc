/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/retry.h"

#include "config/configuration.h"

#include <seastar/core/lowres_clock.hh>

namespace cloud_topics::l1 {

retry_chain_node make_default_metastore_rtc(ss::abort_source& as) {
    return {
      as,
      ss::lowres_clock::now()
        + config::shard_local_cfg().cloud_topics_metastore_retry_timeout_ms(),
      default_metastore_retry_backoff};
}

} // namespace cloud_topics::l1
