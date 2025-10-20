// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/chunked_vector.h"
#include "container/priority_queue.h"
#include "container/tests/bench_utils.h"
#include "random/generators.h"

#include <seastar/core/map_reduce.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <cstddef>
#include <queue>
#include <type_traits>

template<typename PriorityQueue, size_t Size>
struct PriorityQueueBenchTest {
    using value_type = typename PriorityQueue::value_type;

    static auto make_random_value() {
        if constexpr (std::is_integral_v<value_type>) {
            return random_generators::get_int<value_type>();
        } else if constexpr (std::same_as<value_type, ss::sstring>) {
            return random_generators::gen_alphanum_string(32);
        } else if constexpr (std::same_as<value_type, large_struct>) {
            constexpr size_t max_str_len = 64;
            constexpr size_t max_num_cardinality = 16;
            return large_struct{
              .foo = max_num_cardinality / 3,
              .bar = max_num_cardinality - 2,
              .qux = std::string(max_str_len / 8, 'x'),
              .baz = random_generators::gen_alphanum_string(max_str_len / 4),
              .more = random_generators::gen_alphanum_string(max_str_len / 2),
              .data = random_generators::gen_alphanum_string(max_str_len / 1),
              .okdone = 4242,
            };
        }
    }

    static auto make_test_data() {
        chunked_vector<value_type> vec;
        vec.reserve(Size);
        for (size_t j = 0; j < Size; ++j) {
            vec.emplace_back(make_random_value());
        }
        return vec;
    }

    chunked_vector<value_type> test_data = make_test_data();

    PriorityQueue make_filled() {
        PriorityQueue pq;
        for (auto val : test_data) {
            pq.push(val);
        }
        return pq;
    }

    PriorityQueue make_range_filled() {
        PriorityQueue pq;
        pq.push_range(test_data);
        return pq;
    }

    [[gnu::noinline]]
    void run_push_test() {
        PriorityQueue pq = make_filled();
        perf_tests::do_not_optimize(pq);
    }

    [[gnu::noinline]]
    void run_push_range_test() {
        PriorityQueue pq = make_range_filled();
        perf_tests::do_not_optimize(pq);
    }

    PriorityQueue filled_pop = make_range_filled();

    [[gnu::noinline]]
    void run_pop_test() {
        while (!filled_pop.empty()) {
            if constexpr (!std::is_void_v<decltype(filled_pop.pop())>) {
                // bounded_priority_queue::pop() returns a value
                auto x = filled_pop.pop();
                perf_tests::do_not_optimize(x);
            } else {
                // std::priority_queue::pop() returns void
                auto x = filled_pop.top();
                filled_pop.pop();
                perf_tests::do_not_optimize(x);
            }
        }
    }

    PriorityQueue filled_extract_sorted = make_range_filled();

    [[gnu::noinline]]
    void run_extract_sorted_test() {
        auto x = std::move(filled_extract_sorted).extract_sorted();
        perf_tests::do_not_optimize(x);
    }

    PriorityQueue filled_async_extract_sorted = make_range_filled();

    [[gnu::noinline]]
    ss::future<> run_async_extract_sorted_test() {
        auto x = co_await std::move(filled_async_extract_sorted)
                   .async_extract_sorted()
                   .get();
        perf_tests::do_not_optimize(x);
    }

    PriorityQueue filled_sort_extract_sorted = make_range_filled();

    [[gnu::noinline]]
    void run_sort_extract_heap_test() {
        auto x = std::move(filled_sort_extract_sorted).extract_heap();
        std::ranges::sort(x);
        perf_tests::do_not_optimize(x);
    }
};

// NOLINTBEGIN(*-macro-*)
#define PRIORITY_QUEUE_PERF_TEST(container, element, size)                     \
    class PriorityQueueBenchTest_##container##_##element##_##size              \
      : public PriorityQueueBenchTest<container<element>, size> {};            \
    PERF_TEST_F(                                                               \
      PriorityQueueBenchTest_##container##_##element##_##size, Push) {         \
        run_push_test();                                                       \
    }                                                                          \
    PERF_TEST_F(                                                               \
      PriorityQueueBenchTest_##container##_##element##_##size, PushRange) {    \
        run_push_range_test();                                                 \
    }                                                                          \
    PERF_TEST_F(                                                               \
      PriorityQueueBenchTest_##container##_##element##_##size, Pop) {          \
        run_pop_test();                                                        \
    }

#define BOUNDED_PRIORITY_QUEUE_PERF_TEST(container, element, size)             \
    class BoundedPriorityQueueBenchTest_##container##_##element##_##size       \
      : public PriorityQueueBenchTest<container<element>, size> {};            \
    PERF_TEST_F(                                                               \
      BoundedPriorityQueueBenchTest_##container##_##element##_##size, Push) {  \
        run_push_test();                                                       \
    }                                                                          \
    PERF_TEST_F(                                                               \
      BoundedPriorityQueueBenchTest_##container##_##element##_##size,          \
      PushRange) {                                                             \
        run_push_range_test();                                                 \
    }                                                                          \
    PERF_TEST_F(                                                               \
      BoundedPriorityQueueBenchTest_##container##_##element##_##size,          \
      ExtractSorted) {                                                         \
        run_extract_sorted_test();                                             \
    }                                                                          \
    PERF_TEST_F(                                                               \
      BoundedPriorityQueueBenchTest_##container##_##element##_##size,          \
      SortExtractHeap) {                                                       \
        run_sort_extract_heap_test();                                          \
    }
// NOLINTEND(*-macro-*)

template<typename T>
using std_priority_queue = std::priority_queue<T>;

PRIORITY_QUEUE_PERF_TEST(std_priority_queue, int64_t, 64)
PRIORITY_QUEUE_PERF_TEST(priority_queue, int64_t, 64)

PRIORITY_QUEUE_PERF_TEST(std_priority_queue, int64_t, 1024)
PRIORITY_QUEUE_PERF_TEST(priority_queue, int64_t, 1024)

PRIORITY_QUEUE_PERF_TEST(std_priority_queue, sstring, 1024)
PRIORITY_QUEUE_PERF_TEST(priority_queue, sstring, 1024)

PRIORITY_QUEUE_PERF_TEST(std_priority_queue, large_struct, 1024)
PRIORITY_QUEUE_PERF_TEST(priority_queue, large_struct, 1024)

PRIORITY_QUEUE_PERF_TEST(std_priority_queue, int64_t, 1048576)
PRIORITY_QUEUE_PERF_TEST(priority_queue, int64_t, 1048576)

// BOUNDED_PRIORITY_QUEUE_PERF_TEST(priority_queue, int64_t, 64)
BOUNDED_PRIORITY_QUEUE_PERF_TEST(priority_queue, int64_t, 1024)
BOUNDED_PRIORITY_QUEUE_PERF_TEST(priority_queue, sstring, 1024)
BOUNDED_PRIORITY_QUEUE_PERF_TEST(priority_queue, large_struct, 1024)
BOUNDED_PRIORITY_QUEUE_PERF_TEST(priority_queue, int64_t, 1048576)
