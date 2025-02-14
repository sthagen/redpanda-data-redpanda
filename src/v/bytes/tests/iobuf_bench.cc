// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "bytes/iobuf.h"
#include "random/generators.h"

#include <seastar/testing/perf_tests.hh>

namespace {
static constexpr size_t inner_iters = 1000;

template<size_t Size>
size_t move_bench() {
    iobuf buffer = iobuf::from(random_generators::gen_alphanum_string(Size));
    perf_tests::start_measuring_time();
    for (auto i = inner_iters; i--;) {
        iobuf moved = std::move(buffer);
        perf_tests::do_not_optimize(moved);
        buffer = std::move(moved);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * 2;
}
} // namespace

PERF_TEST(iobuf, move_bench_small) { return move_bench<71>(); }
PERF_TEST(iobuf, move_bench_medium) { return move_bench<300_KiB>(); }
PERF_TEST(iobuf, move_bench_large) { return move_bench<965_KiB>(); }
