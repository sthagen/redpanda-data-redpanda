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

#include "container/priority_queue.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <queue>
#include <vector>

struct value {
    explicit value(int data)
      : _data(data) {}

    auto operator<=>(const value&) const = default;
    bool operator==(const value&) const = default;

    int _data;
};

struct move_only {
    explicit move_only(int data)
      : _data{data} {}

    move_only(move_only&&) = default;
    move_only& operator=(move_only&&) = default;

    move_only(const move_only&) = delete;
    move_only& operator=(const move_only&) = delete;

    ~move_only() = default;

    auto operator<=>(const move_only&) const = default;
    bool operator==(const move_only&) const = default;

    int _data;
};

template<template<typename...> class Queue, typename ValueType>
struct QueueFactory {
    // Use large capacity for bounded queues so they behave like unbounded for
    // small test data
    static constexpr size_t large_capacity = 1000;
    using value_type = ValueType;

    static auto make_queue() { return Queue<ValueType>{}; }

    static auto make_bounded_queue(size_t cap) { return Queue<ValueType>{cap}; }

    template<typename Comp>
    static auto make_queue_with_comp(Comp comp) {
        return Queue<ValueType, Comp>{comp};
    }

    template<typename Comp>
    static auto make_bounded_queue_with_comp(size_t cap, Comp comp) {
        return Queue<ValueType, Comp>{cap, comp};
    }

    static value_type make_value(int val) { return value_type{val}; }

    static chunked_vector<value_type>
    make_values(std::initializer_list<size_t> vals) {
        chunked_vector<value_type> result;
        result.reserve(vals.size());
        for (auto v : vals)
            result.push_back(make_value(v));
        return result;
    }
};

// Specific factory types using the generic template
template<typename T>
using UnboundedQueueFactory = QueueFactory<chunked_priority_queue, T>;

template<typename T>
using BoundedQueueFactory = QueueFactory<chunked_bounded_priority_queue, T>;

// Types to test - both value types and move-only types
using QueueTypes = ::testing::
  Types<UnboundedQueueFactory<int>, UnboundedQueueFactory<move_only>>;

using BoundedQueueTypes
  = ::testing::Types<BoundedQueueFactory<int>, BoundedQueueFactory<move_only>>;

// Copyable types only (for tests that require copying)
using CopyableQueueTypes
  = ::testing::Types<UnboundedQueueFactory<int>, BoundedQueueFactory<int>>;

// Types for std compatibility testing (unbounded only, copyable types)
using StdCompatibilityTypes
  = ::testing::Types<UnboundedQueueFactory<int>, UnboundedQueueFactory<value>>;

// Typed test suite for std::priority_queue compatibility - inherits common
// functionality
template<typename QueueFactory>
class StdCompatibilityTest
  : public QueueFactory
  , public ::testing::Test {};

TYPED_TEST_SUITE(StdCompatibilityTest, StdCompatibilityTypes);

// Test drop-in replacement compatibility with std::priority_queue
TYPED_TEST(StdCompatibilityTest, StdCompatibility) {
    std::priority_queue<typename TestFixture::value_type> std_pq;
    priority_queue<typename TestFixture::value_type> our_pq;

    for (int i : {3, 1, 4, 1, 5}) {
        std_pq.push(this->make_value(i));
        our_pq.push(this->make_value(i));
    }

    // Test size and empty
    EXPECT_EQ(std_pq.size(), our_pq.size());
    EXPECT_EQ(std_pq.empty(), our_pq.empty());

    // Test top and pop equivalence
    while (!std_pq.empty() && !our_pq.empty()) {
        EXPECT_EQ(std_pq.top(), our_pq.top());
        std_pq.pop();
        std::ignore = our_pq.pop();
    }

    EXPECT_EQ(std_pq.empty(), our_pq.empty());
}

// Typed test suite for common functionality
template<typename QueueFactory>
class PriorityQueueCommonTest
  : public QueueFactory
  , public ::testing::Test {};

TYPED_TEST_SUITE(PriorityQueueCommonTest, QueueTypes);

TYPED_TEST(PriorityQueueCommonTest, BasicOperations) {
    auto pq = this->make_queue();

    EXPECT_TRUE(pq.empty());
    EXPECT_EQ(pq.size(), 0);

    auto test_value = this->make_value(42);
    pq.push(std::move(test_value));
    EXPECT_FALSE(pq.empty());
    EXPECT_EQ(pq.size(), 1);

    auto expected_value = this->make_value(42);
    EXPECT_EQ(pq.top(), expected_value);

    std::ignore = pq.pop();
    EXPECT_TRUE(pq.empty());
}

TYPED_TEST(PriorityQueueCommonTest, PushRange) {
    auto pq = this->make_queue();

    // Create a vector of values of the appropriate type
    {
        std::vector<typename TestFixture::value_type> values;
        values.push_back(this->make_value(3));
        pq.push_range(std::move(values));
    }

    {
        std::vector<typename TestFixture::value_type> values;
        for (int val : {1, 4, 1, 5}) {
            values.push_back(this->make_value(val));
        }
        pq.push_range(std::move(values));
    }
    EXPECT_EQ(pq.size(), 5);

    std::vector<typename TestFixture::value_type> results;
    while (!pq.empty()) {
        results.push_back(pq.pop());
    }

    EXPECT_TRUE(std::ranges::is_sorted(results, std::ranges::greater{}));
}

TYPED_TEST(PriorityQueueCommonTest, ExtractHeap) {
    auto pq = this->make_queue();

    std::vector<typename TestFixture::value_type> values;
    for (int val : {3, 1, 4, 1, 5}) {
        values.push_back(this->make_value(val));
    }
    pq.push_range(std::move(values));

    auto heap = std::move(pq).extract_heap();
    EXPECT_TRUE(std::ranges::is_heap(heap));

    constexpr auto pop = [](auto& heap) {
        std::ranges::pop_heap(heap);
        auto val = std::move(heap.back());
        heap.pop_back();
        return val;
    };

    EXPECT_EQ(pop(heap), this->make_value(5));
    EXPECT_EQ(pop(heap), this->make_value(4));
    EXPECT_EQ(pop(heap), this->make_value(3));
    EXPECT_EQ(pop(heap), this->make_value(1));
    EXPECT_EQ(pop(heap), this->make_value(1));
}

TYPED_TEST(PriorityQueueCommonTest, ExtractSorted) {
    auto pq = this->make_queue();

    std::vector<typename TestFixture::value_type> values;
    for (int val : {3, 1, 4, 1, 5}) {
        values.push_back(this->make_value(val));
    }
    pq.push_range(std::move(values));

    auto sorted = std::move(pq).extract_sorted();

    EXPECT_EQ(sorted.size(), 5);
    EXPECT_EQ(sorted[0], this->make_value(1));
    EXPECT_EQ(sorted[1], this->make_value(1));
    EXPECT_EQ(sorted[2], this->make_value(3));
    EXPECT_EQ(sorted[3], this->make_value(4));
    EXPECT_EQ(sorted[4], this->make_value(5));
}

TYPED_TEST(PriorityQueueCommonTest, AsyncExtractSorted) {
    auto pq = this->make_queue();

    std::vector<typename TestFixture::value_type> values;
    for (int val : {3, 1, 4, 1, 5}) {
        values.push_back(this->make_value(val));
    }
    pq.async_push_range(std::move(values)).get();

    auto sorted = std::move(pq).async_extract_sorted().get();

    EXPECT_EQ(sorted.size(), 5);
    EXPECT_EQ(sorted[0], this->make_value(1));
    EXPECT_EQ(sorted[1], this->make_value(1));
    EXPECT_EQ(sorted[2], this->make_value(3));
    EXPECT_EQ(sorted[3], this->make_value(4));
    EXPECT_EQ(sorted[4], this->make_value(5));
}

TYPED_TEST(PriorityQueueCommonTest, CustomComparator) {
    auto pq = this->make_queue_with_comp(std::ranges::greater{});

    // Create a vector of values with custom comparator (min-heap behavior)
    std::vector<typename TestFixture::value_type> values;
    for (int val : {3, 1, 4, 1, 5}) {
        values.push_back(this->make_value(val));
    }

    pq.push_range(std::move(values));
    EXPECT_EQ(pq.size(), 5);

    std::vector<typename TestFixture::value_type> sorted;
    while (!pq.empty()) {
        sorted.push_back(pq.pop());
    }

    EXPECT_EQ(sorted.size(), 5);
    EXPECT_EQ(sorted[0], this->make_value(1));
    EXPECT_EQ(sorted[1], this->make_value(1));
    EXPECT_EQ(sorted[2], this->make_value(3));
    EXPECT_EQ(sorted[3], this->make_value(4));
    EXPECT_EQ(sorted[4], this->make_value(5));
}

template<typename QueueFactory>
class BoundedPriorityQueueTest
  : public QueueFactory
  , public ::testing::Test {};

TYPED_TEST_SUITE(BoundedPriorityQueueTest, BoundedQueueTypes);

TYPED_TEST(BoundedPriorityQueueTest, CapacityConstraints) {
    auto bpq = this->make_bounded_queue(2);

    // Fill to capacity
    EXPECT_TRUE(bpq.push(this->make_value(10)));
    EXPECT_TRUE(bpq.push(this->make_value(20)));
    EXPECT_TRUE(bpq.full());

    // Try to push a worse element (smaller value) - should be rejected
    EXPECT_FALSE(bpq.push(this->make_value(5)));
    EXPECT_EQ(bpq.size(), 2);

    // Try to push a better element (larger value) - should evict worst element
    // This maintains the top-K elements by comparing with the worst (minimum)
    // and replacing it if the new element is better
    EXPECT_TRUE(bpq.push(this->make_value(25)));
    EXPECT_EQ(bpq.size(), 2);

    // Verify the final contents - should have the 2 best elements
    auto results = std::move(bpq).extract_sorted();
    EXPECT_TRUE(std::ranges::is_sorted(results, std::ranges::greater{}));
    EXPECT_EQ(results.size(), 2);
    EXPECT_EQ(results[0], this->make_value(25));
    EXPECT_EQ(results[1], this->make_value(20));
}

TYPED_TEST(BoundedPriorityQueueTest, PushRange) {
    auto bpq = this->make_bounded_queue(3); // Keep top 3 elements

    auto elements = this->make_values({5, 10, 2, 15, 8, 20, 1, 12});

    bpq.push_range(elements | std::views::as_rvalue);

    // Should contain the 3 largest elements: 20, 15, 12
    EXPECT_EQ(bpq.size(), 3);

    auto results = std::move(bpq).extract_sorted();

    // Should be the top 3 elements in descending order
    auto expected = this->make_values({20, 15, 12});
    EXPECT_EQ(results, expected);
}

TYPED_TEST(BoundedPriorityQueueTest, Swap) {
    auto bpq1 = this->make_bounded_queue(2);
    auto bpq2 = this->make_bounded_queue(3);

    bpq1.push(this->make_value(10));
    bpq1.push(this->make_value(20));

    bpq2.push(this->make_value(30));
    bpq2.push(this->make_value(40));
    bpq2.push(this->make_value(50));

    bpq1.swap(bpq2);

    EXPECT_EQ(bpq1.size(), 3);
    EXPECT_EQ(bpq2.size(), 2);

    EXPECT_EQ(std::move(bpq1).extract_sorted()[0], this->make_value(50));
    EXPECT_EQ(std::move(bpq2).extract_sorted()[0], this->make_value(20));
}

TYPED_TEST(BoundedPriorityQueueTest, MoveSemantics) {
    auto bpq1 = this->make_bounded_queue(3);
    bpq1.push(this->make_value(10));
    bpq1.push(this->make_value(20));

    // Test move constructor
    auto bpq2 = std::move(bpq1);
    EXPECT_EQ(bpq2.size(), 2);
    EXPECT_EQ(std::move(bpq2).extract_sorted()[0], this->make_value(20));

    // Test move assignment
    auto bpq3 = this->make_bounded_queue(5);
    bpq3.push(this->make_value(5));
    auto bpq4 = this->make_bounded_queue(3);
    bpq4.push(this->make_value(15));
    bpq4.push(this->make_value(25));

    bpq3 = std::move(bpq4);
    EXPECT_EQ(bpq3.size(), 2);
    EXPECT_EQ(std::move(bpq3).extract_sorted()[0], this->make_value(25));
}

TYPED_TEST(BoundedPriorityQueueTest, LargeCapacity) {
    constexpr size_t large_cap = 1000;
    auto bpq = this->make_bounded_queue(large_cap);
    bpq.reserve(500);

    // Fill partially
    for (int i = 0; i < 500; ++i) {
        EXPECT_TRUE(bpq.push(this->make_value(i)));
    }

    EXPECT_EQ(bpq.size(), 500);
    EXPECT_FALSE(bpq.full());
    EXPECT_EQ(
      std::move(bpq).extract_sorted()[0],
      this->make_value(499)); // Largest element
}

TYPED_TEST(BoundedPriorityQueueTest, TopKBehavior) {
    // Demonstrate proper top-K behavior with a small capacity
    auto bpq = this->make_bounded_queue(3); // Keep top 3 elements

    // Insert elements in random order
    bpq.push_range(this->make_values({5, 10, 2, 15, 8, 20, 1, 12}));

    // Should contain the 3 largest elements: 20, 15, 12
    EXPECT_EQ(bpq.size(), 3);

    auto results = std::move(bpq).extract_sorted();

    // Should be the top 3 elements in descending order
    auto expected = this->make_values({20, 15, 12});
    EXPECT_EQ(results, expected);
}

TYPED_TEST(BoundedPriorityQueueTest, AsyncExtractSorted) {
    auto bpq = this->make_bounded_queue(3); // Keep top 3 elements

    // Insert elements in random order
    bpq.async_push_range(this->make_values({5, 10, 2, 15, 8, 20, 1, 12})).get();

    // Should contain the 3 largest elements: 20, 15, 12
    EXPECT_EQ(bpq.size(), 3);

    auto results = std::move(bpq).async_extract_sorted().get();

    // Should be the top 3 elements in descending order
    auto expected = this->make_values({20, 15, 12});
    EXPECT_EQ(results, expected);
}
