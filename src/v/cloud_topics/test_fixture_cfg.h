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

namespace cloud_topics {

// Configuration for cloud topics test fixtures.
struct test_fixture_cfg {
    bool use_lsm_metastore{true};
    bool skip_flush_loop{false};
    bool skip_level_zero_gc{false};
};

} // namespace cloud_topics
