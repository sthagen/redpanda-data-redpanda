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

#include "utils/arch.h"

#include <gtest/gtest.h>

using namespace util;

#ifdef __x86_64__
constexpr auto expected_arch = arch::AMD64;
#else
constexpr auto expected_arch = arch::ARM64;
#endif

GTEST_TEST(arch, equality) { EXPECT_EQ(cpu_arch::current(), expected_arch); }
