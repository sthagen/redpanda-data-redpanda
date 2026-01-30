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

#include "metrics/metrics.h"

namespace cloud_topics::l1 {

class file_arena;

class file_arena_probe {
public:
    file_arena_probe(file_arena* arena, std::string_view ctx)
      : _arena(arena)
      , _ctx(ctx) {}

    void setup_metrics();

private:
    metrics::internal_metric_groups _metrics;

    file_arena* _arena;
    ss::sstring _ctx;
};

} // namespace cloud_topics::l1
