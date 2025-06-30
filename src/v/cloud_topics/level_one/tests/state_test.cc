/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "cloud_topics/level_one/state.h"

#include <gtest/gtest.h>

using namespace experimental::cloud_topics::l1;

// Simple test that makes sure we can build serde serialization.
TEST(StateTest, TestSerde) {
    state s;
    iobuf b = serde::to_iobuf(s.copy());
    auto roundtrip_s = serde::from_iobuf<state>(std::move(b));
    ASSERT_TRUE(s == roundtrip_s);
}
