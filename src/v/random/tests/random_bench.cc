/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "random/generators.h"

#include <seastar/testing/perf_tests.hh>

#include <array>
#include <cstddef>
#include <random>

static constexpr size_t inner_iters = 1000;

struct random_bench {
    random_generators::rng rng;
};

template<typename F>
size_t do_generate(F&& f) {
    std::array<int, inner_iters> output;

    for (size_t i = 0; i < inner_iters; ++i) {
        output[i] = f();
    }

    perf_tests::do_not_optimize(output);

    return inner_iters;
}

PERF_TEST_F(random_bench, get_int_standalone) {
    return do_generate([] { return random_generators::get_int<int>(); });
}

PERF_TEST_F(random_bench, get_int_global) {
    return do_generate(
      [] { return random_generators::global().get_int<int>(); });
}

PERF_TEST_F(random_bench, get_int_state) {
    return do_generate([&] { return rng.get_int<int>(); });
}

// uses ~3x more instructions than the local single-use dist object
// in the next test, belying the conventional wisdom that
// reusing a dist object is more efficient.
PERF_TEST_F(random_bench, std_engine_long_dist) {
    random_generators::rng::engine_type std_engine;
    std::uniform_int_distribution<int> dist;
    return do_generate([&] { return dist(std_engine); });
}

PERF_TEST_F(random_bench, std_engine_temp_dist) {
    random_generators::rng::engine_type std_engine;
    return do_generate([&] {
        std::uniform_int_distribution<int> dist;
        return dist(std_engine);
    });
}
