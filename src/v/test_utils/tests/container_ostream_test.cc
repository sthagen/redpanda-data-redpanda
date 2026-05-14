// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "test_utils/container_ostream.h"

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

template<typename T>
std::string stream(const T& v) {
    std::ostringstream os;
    os << v;
    return std::move(os).str();
}

TEST(ContainerOstream, EmptyVector) {
    EXPECT_EQ(stream(std::vector<int>{}), "[]");
}

TEST(ContainerOstream, VectorOfInts) {
    EXPECT_EQ(stream(std::vector<int>{1, 2, 3}), "[1, 2, 3]");
}

TEST(ContainerOstream, VectorOfStrings) {
    // fmt's range formatter wraps strings in quotes by default.
    EXPECT_EQ(stream(std::vector<std::string>{"a", "b"}), "[\"a\", \"b\"]");
}

TEST(ContainerOstream, NestedVector) {
    EXPECT_EQ(
      stream(std::vector<std::vector<int>>{{1, 2}, {3}}), "[[1, 2], [3]]");
}

TEST(ContainerOstream, EmptyUnorderedMap) {
    EXPECT_EQ(stream(std::unordered_map<int, int>{}), "{}");
}

TEST(ContainerOstream, UnorderedMapSingleEntry) {
    // Unordered iteration order is implementation-defined; pin the test to
    // one entry so it stays deterministic.
    EXPECT_EQ(
      stream(std::unordered_map<int, std::string>{{1, "one"}}), "{1: \"one\"}");
}

TEST(ContainerOstream, GTestStreamingMessage) {
    // Smoke test the actual scenario this header exists for: chaining a
    // container into a googletest assertion message.
    std::vector<int> v{42};
    testing::AssertionResult r = testing::AssertionFailure() << v;
    EXPECT_EQ(std::string{r.message()}, "[42]");
}

} // namespace
