/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

// Here we define test assertion macros which are "portable" across gtest
// and boot test frameworks. In a boost test or gtest test itself, you are
// probably better of using the "native" macros directly, but in header files
// which may be included from both test frameworks, these macros are useful.
// They are also useful in the .cc files for fixtures which are used by
// both frameworks.
//
// The decision to use the gtest macros or the boost test macros is made
// based on the IS_GTEST or IS_BTEST macros. These are defined only for
// the compilation of the test.cc file (and any headers included in that
// TU), but not for other files, e.g., test helper or fixture code in another
// .cc file. If neither macro is defined, we use a fallback implementation
// based on vassert.
//
// The emulation is not exact, for example concepts like "add fail" do not
// exist in boost or fallback vassert, so those fail immediately.

#if defined(IS_BTEST)
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#define RPTEST_FAIL(m) BOOST_FAIL(m)
#define RPTEST_ADD_FAIL(m) BOOST_FAIL(m)
#define RPTEST_FAIL_CORO(m) BOOST_FAIL(m)
#define RPTEST_REQUIRE(m) BOOST_REQUIRE(m)
#define RPTEST_REQUIRE_CORO(m) BOOST_REQUIRE(m)
#define RPTEST_REQUIRE_EQ(m, n) BOOST_REQUIRE_EQUAL(m, n)
#define RPTEST_REQUIRE_EQ_CORO(m, n) BOOST_REQUIRE_EQUAL(m, n)
#define RPTEST_REQUIRE_NE(m, n) BOOST_REQUIRE_NE(m, n)
#define RPTEST_REQUIRE_NE_CORO(m, n) BOOST_REQUIRE_NE(m, n)
#define RPTEST_EXPECT_EQ(m, n) BOOST_CHECK_EQUAL(m, n)
#define RPTEST_REQUIRE_GT(m, n) BOOST_REQUIRE_GT(m, n)
#define RPTEST_REQUIRE_GE(m, n) BOOST_REQUIRE_GE(m, n)
#define RPTEST_REQUIRE_LE(m, n) BOOST_REQUIRE_LE(m, n)
#define RPTEST_REQUIRE_LT(m, n) BOOST_REQUIRE_LT(m, n)
#define RPTEST_REQUIRE_THROW(m, e) BOOST_REQUIRE_THROW(m, e)
#define RPTEST_REQUIRE_NO_THROW(m) BOOST_REQUIRE_NO_THROW(m)
#define RPTEST_REQUIRE_CLOSE(m, n, t) BOOST_REQUIRE_CLOSE(m, n, t)
#define RPTEST_EXPECT(m) BOOST_CHECK(m)

#elif defined(IS_GTEST)

#include "test_utils/test.h"

#define RPTEST_FAIL(m) FAIL() << (m)
#define RPTEST_ADD_FAIL(m) ADD_FAILURE() << (m)
#define RPTEST_FAIL_CORO(m) ASSERT_TRUE_CORO(false) << (m)
#define RPTEST_REQUIRE(m) ASSERT_TRUE(m)
#define RPTEST_REQUIRE_CORO(m) ASSERT_TRUE_CORO(m)
#define RPTEST_REQUIRE_EQ(m, n) ASSERT_EQ(m, n)
#define RPTEST_REQUIRE_EQ_CORO(m, n) ASSERT_EQ_CORO(m, n)
#define RPTEST_REQUIRE_NE(m, n) ASSERT_NE(m, n)
#define RPTEST_REQUIRE_NE_CORO(m, n) ASSERT_NE_CORO(m, n)
#define RPTEST_EXPECT_EQ(m, n) EXPECT_EQ(m, n)
#define RPTEST_REQUIRE_GT(m, n) ASSERT_GT(m, n)
#define RPTEST_REQUIRE_GE(m, n) ASSERT_GE(m, n)
#define RPTEST_REQUIRE_LE(m, n) ASSERT_LE(m, n)
#define RPTEST_REQUIRE_LT(m, n) ASSERT_LT(m, n)
#define RPTEST_REQUIRE_THROW(m, e) ASSERT_THROW(m, e)
#define RPTEST_REQUIRE_NO_THROW(m) ASSERT_NO_THROW(m)
#define RPTEST_REQUIRE_CLOSE(m, n, t) ASSERT_NEAR(m, n, t)
#define RPTEST_EXPECT(m) EXPECT_TRUE(m)

#else
#include "base/vassert.h"

// fallback macros using vassert

// The fallback macros which accept messages only 1 object which can be
// formatted via fmt::format, unlike the other macros which can accept <<
// operator delimited strings.
#define RPTEST_FAIL(m) vunreachable("{}", m)
#define RPTEST_ADD_FAIL(m) RPTEST_FAIL(m)
#define RPTEST_FAIL_CORO(m) RPTEST_FAIL(m)
#define RPTEST_REQUIRE(m) vassert(m, "RPTEST_REQUIRE assertion failed")
#define RPTEST_REQUIRE_CORO(m) RPTEST_REQUIRE(m)
#define RPTEST_REQUIRE_EQ(m, n) RPTEST_REQUIRE(m == n)
#define RPTEST_REQUIRE_EQ_CORO(m, n) RPTEST_REQUIRE_EQ(m, n)
#define RPTEST_REQUIRE_NE(m, n) RPTEST_REQUIRE(m != n)
#define RPTEST_REQUIRE_NE_CORO(m, n) RPTEST_REQUIRE_NE(m, n)
#define RPTEST_EXPECT_EQ(m, n) RPTEST_REQUIRE_EQ(m, n)

#endif
