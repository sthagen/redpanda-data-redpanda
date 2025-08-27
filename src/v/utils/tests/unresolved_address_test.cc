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

#include "utils/unresolved_address.h"

#include <gtest/gtest.h>

namespace net::tests {

TEST(unresolved_address_test, from_string_valid) {
    auto result = unresolved_address::from_string("test-addr:12345");

    EXPECT_EQ(result.host(), "test-addr");
    EXPECT_EQ(result.port(), 12345);
    EXPECT_FALSE(result.family().has_value());
}

TEST(unresolved_address_test, no_colon) {
    EXPECT_THROW(
      unresolved_address::from_string("test-addr"), std::invalid_argument);
}

TEST(unresolved_address_test, empty_host) {
    EXPECT_THROW(
      unresolved_address::from_string(":12345"), std::invalid_argument);
}

TEST(unresolved_address_test, invalid_port) {
    EXPECT_THROW(
      unresolved_address::from_string("test-addr:abcd"), std::invalid_argument);
}

TEST(unresolved_address_test, invalid_port_partial) {
    EXPECT_THROW(
      unresolved_address::from_string("test-addr:123abc"),
      std::invalid_argument);
}

} // namespace net::tests
