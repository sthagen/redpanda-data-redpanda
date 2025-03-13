/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/seastarx.h"

#include <seastar/core/future.hh>

#include <fmt/format.h>

#include <memory>

struct stress_config {
    // Number of ticks to increment between each scheduling point.
    // If set, "delay" variants should not be set.
    std::optional<int> min_spins_per_scheduling_point;
    std::optional<int> max_spins_per_scheduling_point;
    // The (additional) stack depth the spin function should
    // have, which is useful for stressing the CPU profiler.
    std::optional<int> stack_depth;

    // Time in milliseconds to spin for between each scheduling point.
    // If set, "spins" variants should not be set.
    std::optional<int> min_ms_per_scheduling_point;
    std::optional<int> max_ms_per_scheduling_point;

    size_t num_fibers;
};

template<>
struct fmt::formatter<stress_config> : fmt::formatter<std::string_view> {
    auto format(const stress_config& c, auto& ctx) const {
        return fmt::format_to(
          ctx.out(),
          "min/max_spin: {}/{} min/max_ms: {}/{} stack_depth {}",
          c.min_spins_per_scheduling_point,
          c.max_spins_per_scheduling_point,
          c.min_ms_per_scheduling_point,
          c.max_ms_per_scheduling_point,
          c.stack_depth);
    }
};

class stress_payload;

// Manages a single stress payload.
class stress_fiber_manager {
public:
    explicit stress_fiber_manager();
    ~stress_fiber_manager();

    // Starts stress fibers with the given config and returns true, or returns
    // false if the config is invalid or if stress fibers are already running.
    bool start(stress_config);

    // Stops any existing stress fibers.
    ss::future<> stop();

private:
    std::unique_ptr<stress_payload> _stress;
};
