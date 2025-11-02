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

#include "cloud_topics/level_zero/pipeline/read_pipeline.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

namespace cloud_topics::l0 {

/// Read fanout stage coverts vectorized read requests into multiple
/// parallel requests. The client is allowed to send wide read requests
/// that target multiple extents that may be stored in different L0
/// objects. The read fanout stage splits the requests into multiple
/// smaller requests that are then processed in parallel by the
/// fetch request handler stage. The fetch handler can't run individual
/// requests in parallel due to its own limitations. This stage allows
/// to regain some parallelism.
///
/// For requests that target single extent the stage simply forwards
/// them to the next stage without any modifications.
class read_fanout {
public:
    explicit read_fanout(l0::read_pipeline<>::stage);

    struct stats {
        size_t requests_in{0};
        size_t requests_out{0};
        size_t requests_fail{0};
    };

    ss::future<> start();
    ss::future<> stop();

    stats get_stats() const noexcept;

private:
    ss::future<> bg_process();

    ss::future<> process_single_request(l0::read_request<>* req);

    ss::gate _gate;
    l0::read_pipeline<>::stage _pipeline_stage;
    stats _stats;
};
} // namespace cloud_topics::l0
