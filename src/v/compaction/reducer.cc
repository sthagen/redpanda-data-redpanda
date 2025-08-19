// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction/reducer.h"

#include <seastar/core/coroutine.hh>

namespace compaction {

ss::future<> compaction::sliding_window_reducer::run() && {
    // Step 0: Initialize source
    co_await _src->initialize();

    // Step 1: Perform backward pass
    co_await ss::repeat([this]() { return _src->backward_pass_iteration(); });

    // Step 2: Perform forward pass.
    co_await ss::repeat(
      [this]() { return _src->forward_pass_iteration(*_sink); });

    // Done!
    co_await _sink->finalize();
}

} // namespace compaction
