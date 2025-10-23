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

#include <seastar/testing/perf_tests.hh>

namespace {
static constexpr size_t inner_iters = 1000;

// Make a deterministic iobuf for consistent benchmarks instead of randomized
// data.
iobuf make_iobuf(size_t size, bool reverse = false) {
    auto gen = std::views::iota('a', 'z');
    std::string generated;
    generated.reserve(size);
    while (generated.size() < size) {
        for (char c : gen) {
            generated.push_back(c);
            if (generated.size() == size) {
                break;
            }
        }
    }
    if (reverse) {
        std::ranges::reverse(generated);
    }
    return iobuf::from(generated);
}

template<size_t Size>
size_t move_bench() {
    iobuf buffer = make_iobuf(Size);
    perf_tests::start_measuring_time();
    for (auto i = inner_iters; i--;) {
        iobuf moved = std::move(buffer);
        perf_tests::do_not_optimize(moved);
        buffer = std::move(moved);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * 2;
}

template<size_t Size, typename cmp_fn, bool same>
size_t cmp_bench() {
    iobuf a = make_iobuf(Size);
    iobuf a_copy = a.copy();
    iobuf b = make_iobuf(Size, !same);
    iobuf b_copy;
    for (const auto& frag : b) {
        for (char c : std::string_view(frag.get(), frag.size())) {
            b_copy.append(&c, 1);
        }
    }
    perf_tests::start_measuring_time();
    for (auto i = inner_iters; i--;) {
        perf_tests::do_not_optimize(cmp_fn{}(a, b));
        perf_tests::do_not_optimize(cmp_fn{}(a_copy, b_copy));
    }
    perf_tests::stop_measuring_time();
    return inner_iters * 2;
}

// This microbench mocks common pattern in Redpanda where smaller iobufs will be
// parsed out of a larger iobuf then appendded together. This pattern is heavily
// used in the datalake impl.
template<int Bufs, size_t BufSize>
size_t append_bench() {
    iobuf_parser parser(make_iobuf(Bufs * BufSize));

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

// clang-format off
PERF_TEST(iobuf, cmp_bench_0000) { return cmp_bench<   0, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0001) { return cmp_bench<   1, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0002) { return cmp_bench<   2, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0003) { return cmp_bench<   3, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0004) { return cmp_bench<   4, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0016) { return cmp_bench<  16, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0064) { return cmp_bench<  64, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0128) { return cmp_bench< 128, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0256) { return cmp_bench< 256, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0512) { return cmp_bench< 512, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_1024) { return cmp_bench<1024, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_2048) { return cmp_bench<2048, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0000_same) { return cmp_bench<   0, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0001_same) { return cmp_bench<   1, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0002_same) { return cmp_bench<   2, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0003_same) { return cmp_bench<   3, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0004_same) { return cmp_bench<   4, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0016_same) { return cmp_bench<  16, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0064_same) { return cmp_bench<  64, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0128_same) { return cmp_bench< 128, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0256_same) { return cmp_bench< 256, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0512_same) { return cmp_bench< 512, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_1024_same) { return cmp_bench<1024, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_2048_same) { return cmp_bench<2048, std::less<>, true>(); }
// clang-format on

PERF_TEST(iobuf, move_bench_small) { return move_bench<71>(); }
PERF_TEST(iobuf, move_bench_medium) { return move_bench<300_KiB>(); }
PERF_TEST(iobuf, move_bench_large) { return move_bench<965_KiB>(); }

PERF_TEST(iobuf, eq_bench_small) {
    return cmp_bench<71, std::equal_to<>, false>();
}
PERF_TEST(iobuf, eq_bench_small_same) {
    return cmp_bench<71, std::equal_to<>, true>();
}
PERF_TEST(iobuf, eq_bench_medium) {
    return cmp_bench<300_KiB, std::equal_to<>, false>();
}
PERF_TEST(iobuf, eq_bench_medium_same) {
    return cmp_bench<300_KiB, std::equal_to<>, true>();
}
PERF_TEST(iobuf, eq_bench_large) {
    return cmp_bench<965_KiB, std::equal_to<>, false>();
}
PERF_TEST(iobuf, eq_bench_large_same) {
    return cmp_bench<965_KiB, std::equal_to<>, true>();
}

PERF_TEST(iobuf, append_bench_small) { return append_bench<1'000, 4>(); }
PERF_TEST(iobuf, append_bench_medium) { return append_bench<1'000, 40_KiB>(); }
PERF_TEST(iobuf, append_bench_large) { return append_bench<1'000, 400_KiB>(); }
