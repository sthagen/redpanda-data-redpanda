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
#include "bytes/iobuf_parser.h"
#include "bytes/random.h"
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

// This microbench mocks common pattern in Redpanda where smaller iobufs will be
// parsed out of a larger iobuf then appendded together. This pattern is heavily
// used in the datalake impl.
template<int Bufs, size_t BufSize>
size_t append_bench() {
    iobuf_parser parser(random_generators::make_iobuf(Bufs * BufSize));

    std::vector<iobuf> buffers;
    buffers.reserve(Bufs);
    for (int i = 0; i < Bufs; i++) {
        buffers.push_back(parser.share(BufSize));
    }

    iobuf res;
    perf_tests::start_measuring_time();
    for (int i = 0; i < Bufs; i++) {
        res.append(std::move(buffers[i]));
    }
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(res);

    return Bufs;
}
} // namespace

PERF_TEST(iobuf, move_bench_small) { return move_bench<71>(); }
PERF_TEST(iobuf, move_bench_medium) { return move_bench<300_KiB>(); }
PERF_TEST(iobuf, move_bench_large) { return move_bench<965_KiB>(); }

PERF_TEST(iobuf, append_bench_small) { return append_bench<1'000, 4>(); }
PERF_TEST(iobuf, append_bench_medium) { return append_bench<1'000, 40_KiB>(); }
PERF_TEST(iobuf, append_bench_large) { return append_bench<1'000, 400_KiB>(); }
