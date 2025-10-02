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

#include "ssx/condition_variable.h"

#include <seastar/core/manual_clock.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace ssx {

using namespace std::chrono_literals;

TEST(ConditionVariable, AbortSource) {
    ss::abort_source as;
    condition_variable cv;

    auto fut = cv.wait(as);
    EXPECT_FALSE(fut.available());
    cv.signal();
    fut.get();

    fut = cv.wait(as);
    EXPECT_FALSE(fut.available());
    as.request_abort();
    EXPECT_THROW(fut.get(), ss::abort_requested_exception);
}

TEST(ConditionVariable, AbortSourceWithCustomException) {
    class my_cool_exception : public std::exception {};
    ss::abort_source as;
    condition_variable cv;

    auto fut = cv.wait(as);
    as.request_abort_ex(my_cool_exception());
    EXPECT_THROW(fut.get(), my_cool_exception);
}

TEST(ConditionVariable, AbortSourceWithPredicate) {
    ss::abort_source as;
    condition_variable cv;

    int i = 0;
    auto fut = cv.wait(as, [&i] { return ++i == 2; });
    cv.signal();
    cv.signal();
    fut.get();

    // Test predicate with abort
    fut = cv.wait(as, [] { return false; });
    as.request_abort();
    EXPECT_THROW(fut.get(), ss::abort_requested_exception);
}

TEST(ConditionVariable, AbortSourceWithTimeout) {
    ss::abort_source as;
    condition_variable cv;

    // Test timeout fires before abort
    auto fut = cv.wait(ss::manual_clock::now() + 1s, as);
    EXPECT_FALSE(fut.available());
    ss::manual_clock::advance(2s);
    EXPECT_THROW(fut.get(), ss::condition_variable_timed_out);

    // Test abort fires before timeout - need fresh abort_source
    ss::abort_source as2;
    fut = cv.wait(ss::manual_clock::now() + 10s, as2);
    EXPECT_FALSE(fut.available());
    as2.request_abort();
    EXPECT_THROW(fut.get(), ss::abort_requested_exception);
}

TEST(ConditionVariable, AbortSourceWithTimeoutAndPredicate) {
    ss::abort_source as;
    condition_variable cv;

    int i = 0;
    auto fut = cv.wait(
      ss::manual_clock::now() + 10s, as, [&i] { return ++i == 2; });
    cv.signal();
    fut.get();

    cv.signal();
    fut = cv.wait(ss::manual_clock::now() + 1s, as, [] { return false; });
    ss::manual_clock::advance(2s);
    EXPECT_THROW(fut.get(), ss::condition_variable_timed_out);

    fut = cv.wait(ss::manual_clock::now() + 10s, as, [] { return false; });
    as.request_abort();
    EXPECT_THROW(fut.get(), ss::abort_requested_exception);
}

TEST(ConditionVariable, Signal) {
    ss::abort_source as;
    condition_variable cv;

    auto fut = cv.wait(as);
    EXPECT_FALSE(fut.available());
    cv.signal();
    fut.get();
}

TEST(ConditionVariable, Broadcast) {
    ss::abort_source as;
    condition_variable cv;

    auto fut1 = cv.wait(as);
    auto fut2 = cv.wait(as);
    EXPECT_FALSE(fut1.available());
    EXPECT_FALSE(fut2.available());
    cv.broadcast();
    fut1.get();
    fut2.get();
}

TEST(ConditionVariable, Broken) {
    ss::abort_source as;
    condition_variable cv;

    auto fut1 = cv.wait(as);
    auto fut2 = cv.wait(as);
    EXPECT_FALSE(fut1.available());
    EXPECT_FALSE(fut2.available());
    cv.broken();
    EXPECT_THROW(fut1.get(), ss::broken_condition_variable);
    EXPECT_THROW(fut2.get(), ss::broken_condition_variable);
}

TEST(ConditionVariable, BrokenWithCustomException) {
    class my_exception : public std::exception {};
    ss::abort_source as;
    condition_variable cv;

    auto fut = cv.wait(as);
    cv.broken(std::make_exception_ptr(my_exception()));
    EXPECT_THROW(fut.get(), my_exception);
}

TEST(ConditionVariable, HasWaiters) {
    ss::abort_source as;
    condition_variable cv;

    EXPECT_FALSE(cv.has_waiters());
    auto fut = cv.wait(as);
    EXPECT_TRUE(cv.has_waiters());
    cv.signal();
    fut.get();
    EXPECT_FALSE(cv.has_waiters());
}

} // namespace ssx
