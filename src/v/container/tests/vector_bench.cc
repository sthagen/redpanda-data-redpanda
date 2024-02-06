/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "container/fragmented_vector.h"
#include "random/generators.h"
#include "utils/functional.h"
#include "utils/type_traits.h"

#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <iterator>
#include <vector>

struct large_struct {
    size_t foo;
    size_t bar;
    std::string qux;
    std::string baz;
    std::string more;
    std::string data;
    size_t okdone;

    bool operator==(const large_struct&) const = default;
    auto operator<=>(const large_struct&) const = default;
};

template<typename Vector, size_t Size>
class VectorBenchTest {
    static auto make_value() {
        if constexpr (std::is_same_v<typename Vector::value_type, int64_t>) {
            return static_cast<int64_t>(random_generators::get_int<int64_t>());

        } else if constexpr (std::is_same_v<
                               typename Vector::value_type,
                               ss::sstring>) {
            constexpr size_t max_str_len = 64;
            return random_generators::gen_alphanum_string(
              random_generators::get_int<size_t>(0, max_str_len));
        } else if constexpr (std::is_same_v<
                               typename Vector::value_type,
                               large_struct>) {
            constexpr size_t max_str_len = 64;
            constexpr size_t max_num_cardinality = 16;
            return large_struct{
              .foo = random_generators::get_int<size_t>(0, max_num_cardinality),
              .bar = random_generators::get_int<size_t>(0, max_num_cardinality),
              .qux = random_generators::gen_alphanum_string(
                random_generators::get_int<size_t>(0, max_str_len)),
              .baz = random_generators::gen_alphanum_string(
                random_generators::get_int<size_t>(0, max_str_len)),
              .more = random_generators::gen_alphanum_string(
                random_generators::get_int<size_t>(0, max_str_len)),
              .data = random_generators::gen_alphanum_string(
                random_generators::get_int<size_t>(0, max_str_len)),
              .okdone = random_generators::get_int<size_t>(),
            };
        } else {
            static_assert(
              utils::unsupported_type<typename Vector::value_type>::value,
              "unsupported");
        }
    }
    static auto make_filled() {
        Vector v;
        std::generate_n(std::back_inserter(v), Size, make_value);
        return v;
    }

public:
    void run_sort_test() {
        auto v = make_filled();
        perf_tests::start_measuring_time();
        std::sort(v.begin(), v.end());
        perf_tests::stop_measuring_time();
    }

    void run_fifo_test() {
        Vector v;
        auto val = make_value();
        perf_tests::start_measuring_time();
        for (size_t i = 0; i < Size; ++i) {
            v.push_back(val);
        }
        for (const auto& e : v) {
            perf_tests::do_not_optimize(e);
        }
        perf_tests::stop_measuring_time();
    }

    void run_lifo_test() {
        Vector v;
        auto val = make_value();
        perf_tests::start_measuring_time();
        for (size_t i = 0; i < Size; ++i) {
            v.push_back(val);
        }
        while (!v.empty()) {
            v.pop_back();
        }
        perf_tests::stop_measuring_time();
    }

    void run_fill_test() {
        Vector v;
        auto val = make_value();
        perf_tests::start_measuring_time();
        std::fill_n(std::back_inserter(v), Size, val);
        perf_tests::stop_measuring_time();
    }

    void run_random_access_test() {
        auto v = make_filled();
        std::vector<size_t> indexes;
        std::generate_n(std::back_inserter(indexes), 1000, [&v]() {
            return random_generators::get_int<size_t>(0, v.size() - 1);
        });
        perf_tests::start_measuring_time();
        perf_tests::do_not_optimize(v.front());
        perf_tests::do_not_optimize(v.back());
        for (size_t index : indexes) {
            perf_tests::do_not_optimize(v[index]);
        }
        perf_tests::do_not_optimize(v.front());
        perf_tests::do_not_optimize(v.back());
        perf_tests::stop_measuring_time();
    }
};

// NOLINTBEGIN(*-macro-*)
#define VECTOR_PERF_TEST(container, element, size)                             \
    class VectorBenchTest_##container##_##element##_##size                     \
      : public VectorBenchTest<container<element>, size> {};                   \
    PERF_TEST_F(VectorBenchTest_##container##_##element##_##size, Sort) {      \
        run_sort_test();                                                       \
    }                                                                          \
    PERF_TEST_F(VectorBenchTest_##container##_##element##_##size, Fifo) {      \
        run_fifo_test();                                                       \
    }                                                                          \
    PERF_TEST_F(VectorBenchTest_##container##_##element##_##size, Lifo) {      \
        run_lifo_test();                                                       \
    }                                                                          \
    PERF_TEST_F(VectorBenchTest_##container##_##element##_##size, Fill) {      \
        run_fill_test();                                                       \
    }                                                                          \
    PERF_TEST_F(                                                               \
      VectorBenchTest_##container##_##element##_##size, RandomAccess) {        \
        run_random_access_test();                                              \
    }
// NOLINTEND(*-macro-*)

template<typename T>
using std_vector = std::vector<T>;
using ss::sstring;

VECTOR_PERF_TEST(std_vector, int64_t, 64)
VECTOR_PERF_TEST(fragmented_vector, int64_t, 64)
VECTOR_PERF_TEST(chunked_vector, int64_t, 64)

VECTOR_PERF_TEST(std_vector, sstring, 64)
VECTOR_PERF_TEST(fragmented_vector, sstring, 64)
VECTOR_PERF_TEST(chunked_vector, sstring, 64)

VECTOR_PERF_TEST(std_vector, large_struct, 64)
VECTOR_PERF_TEST(fragmented_vector, large_struct, 64)
VECTOR_PERF_TEST(chunked_vector, large_struct, 64)

VECTOR_PERF_TEST(std_vector, int64_t, 10000)
VECTOR_PERF_TEST(fragmented_vector, int64_t, 10000)
VECTOR_PERF_TEST(chunked_vector, int64_t, 10000)

VECTOR_PERF_TEST(std_vector, sstring, 10000)
VECTOR_PERF_TEST(fragmented_vector, sstring, 10000)
VECTOR_PERF_TEST(chunked_vector, sstring, 10000)

VECTOR_PERF_TEST(std_vector, large_struct, 10000)
VECTOR_PERF_TEST(fragmented_vector, large_struct, 10000)
VECTOR_PERF_TEST(chunked_vector, large_struct, 10000)

VECTOR_PERF_TEST(std_vector, int64_t, 1048576)
VECTOR_PERF_TEST(fragmented_vector, int64_t, 1048576)
VECTOR_PERF_TEST(chunked_vector, int64_t, 1048576)
