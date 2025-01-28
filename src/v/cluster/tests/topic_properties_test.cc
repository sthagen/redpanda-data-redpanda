// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_properties.h"

#include <gtest/gtest.h>

namespace cluster {

// Avoid a common typo where a part of the string format is passed as an
// argument.
// clang-format off
// I.e fmt::format("a: {}", "b: {}", a, b) resulting in "a: b: {}"
// instead of fmt::format("a: {}, b: {}", a, b) which should result in "a: <value> b: <value>"
// clang-format on
TEST(TopicProperties, ostream) {
    topic_properties properties;
    std::ostringstream stream;
    stream << properties;
    auto result = stream.str();
    ASSERT_FALSE(result.contains("{}")) << result;
}

} // namespace cluster
