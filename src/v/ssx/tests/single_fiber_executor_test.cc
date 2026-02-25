// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "ssx/single_fiber_executor.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/semaphore.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <stdexcept>

using namespace std::chrono_literals;

namespace {

using fn_t = ss::noncopyable_function<ss::future<ss::sstring>(
  ss::abort_source&) noexcept(false)>;

fn_t make_ready_coro(ss::sstring retval) {
    return {[retval](ss::abort_source&) { return ssx::now(retval); }};
}

fn_t make_slow_coro(ss::semaphore& sem, ss::sstring retval) {
    return {[&sem, retval](ss::abort_source& as) {
        return sem.wait(as).then([retval] { return retval; });
    }};
}

fn_t make_uncooperative_coro(ss::semaphore& sem, ss::sstring retval) {
    return {[&sem, retval](ss::abort_source& as) {
        return sem.wait().then([&as, retval] {
            return as.abort_requested() ? ss::sstring("tripped-on-abort-source")
                                        : retval;
        });
    }};
}

fn_t make_side_effect_coro(
  ss::semaphore& sem, ss::sstring retval, int& counter) {
    return {[&sem, retval, &counter](ss::abort_source& as) {
        return sem.wait().then([&as, retval, &counter] {
            ++counter;
            return as.abort_requested() ? ss::sstring("tripped-on-abort-source")
                                        : retval;
        });
    }};
}

using sfe = ssx::single_fiber_executor<fn_t>;

// depending on Seastar scheduling, the coro may or may not complete
MATCHER_P(InterruptedOr, expected_value, "") {
    return arg ? arg.value() == expected_value
               : arg.error() == sfe::errc::interrupted;
}

MATCHER_P(CompletedAnd, expected_value, "") {
    return arg && arg.value() == expected_value;
}

constexpr std::expected<ss::sstring, sfe::errc> interrupted{
  std::unexpect, sfe::errc::interrupted};

void assure_new_tasks_are_bounced(ssx::single_fiber_executor<fn_t>& executor) {
    auto f = executor.submit(make_ready_coro("na"));
    ASSERT_EQ(f.get(), interrupted);
}

} // namespace

TEST(SingleFiberExecutor, no_destruction_test) {
    ssx::single_fiber_executor<fn_t> executor;

    { // superseded ones may or may not complete
        auto f1 = executor.submit(make_ready_coro("ret1"));
        auto f2 = executor.submit(make_ready_coro("ret2"));
        auto f3 = executor.submit(make_ready_coro("ret3"));
        ASSERT_THAT(f1.get(), InterruptedOr("ret1"));
        ASSERT_THAT(f2.get(), InterruptedOr("ret2"));
        ASSERT_THAT(f3.get(), CompletedAnd("ret3"));
    }

    { // running, superseded and queued -- all get aborted
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto f2 = executor.submit(make_slow_coro(sem, "ret2"));
        auto f3 = executor.submit(make_slow_coro(sem, "ret3"));
        executor.request_abort();
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_EQ(f2.get(), interrupted);
        ASSERT_EQ(f3.get(), interrupted);
    }

    { // the second is scheduled before the first one completes
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto f2 = executor.submit(make_ready_coro("ret2"));
        sem.signal();
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_THAT(f2.get(), CompletedAnd("ret2"));
    }

    { // the result of the interrupted one is discarded
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_uncooperative_coro(sem, "ret1"));
        auto f2 = executor.submit(make_ready_coro("ret2"));
        sem.signal();
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_THAT(f2.get(), CompletedAnd("ret2"));
    }

    { // aborted yields to next scheduled
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        executor.request_abort();
        auto f2 = executor.submit(make_ready_coro("ret2"));
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_THAT(f2.get(), CompletedAnd("ret2"));
    }

    { // latest scheduled runs to completion
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto f2 = executor.submit(make_slow_coro(sem, "ret2"));
        auto f3 = executor.submit(make_slow_coro(sem, "ret3"));
        sem.signal(3);
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_EQ(f2.get(), interrupted);
        ASSERT_THAT(f3.get(), CompletedAnd("ret3"));
    }

    { // exception gets propagated
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        sem.broken();
        ASSERT_THROW(f1.get(), ss::broken_semaphore);
    }

    { // exception gets propagated for the pending one as well
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto f2 = executor.submit(make_slow_coro(sem, "ret2"));
        sem.broken();
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_THROW(f2.get(), ss::broken_semaphore);
    }

    { // superseding wins over exception
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto f2 = executor.submit(make_ready_coro("ret2"));
        sem.broken();
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_THAT(f2.get(), CompletedAnd("ret2"));
    }
}

TEST(SingleFiberExecutor, drain_test) {
    { // drain while idle
        ssx::single_fiber_executor<fn_t> executor;
        auto drain_f = executor.drain();
        assure_new_tasks_are_bounced(executor);
        drain_f.get();
        assure_new_tasks_are_bounced(executor);
    }

    { // drain while running
        ssx::single_fiber_executor<fn_t> executor;
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto drain_f = executor.drain();
        ASSERT_EQ(f1.get(), interrupted); // it got canceled by drain
        assure_new_tasks_are_bounced(executor);
        drain_f.get();
        assure_new_tasks_are_bounced(executor);
    }

    { // drain while running and queued
        ssx::single_fiber_executor<fn_t> executor;
        ss::semaphore sem{0};
        int cnt_f1_completed = 0;
        auto f1 = executor.submit(
          make_side_effect_coro(sem, "ret1", cnt_f1_completed));
        auto f2 = executor.submit(make_ready_coro("ret2"));
        auto drain_f = executor.drain();
        ASSERT_EQ(f1.get(), interrupted); // it got canceled by drain
        ASSERT_EQ(f2.get(), interrupted); // and this one never even got run
        ASSERT_EQ(cnt_f1_completed, 0);   // however, f1 hasn't completed
        assure_new_tasks_are_bounced(executor);
        ASSERT_FALSE(drain_f.available()); // can't complete because f1 is stuck
        sem.signal();                      // let f1 complete
        drain_f.get();
        ASSERT_EQ(cnt_f1_completed, 1); // it has completed now
        assure_new_tasks_are_bounced(executor);
    }
    return;
}

TEST(SingleFiberExecutor, empty_return_test) {
    using fn_t = ss::noncopyable_function<ss::future<>(
      ss::abort_source&) noexcept(false)>;
    using sfe = ssx::single_fiber_executor<fn_t>;
    auto make_ready_coro = [] -> fn_t {
        return {[](ss::abort_source&) -> ss::future<> { return ss::now(); }};
    };
    constexpr std::expected<void, sfe::errc> completed{};

    ssx::single_fiber_executor<fn_t> executor;
    { // superseded ones may or may not complete
        auto f1 = executor.submit(make_ready_coro());
        auto f2 = executor.submit(make_ready_coro());
        auto f3 = executor.submit(make_ready_coro());
        f1.get();
        f2.get();
        ASSERT_EQ(f3.get(), completed);
    }
    auto drain_f = executor.drain();
    drain_f.get();
}

TEST(SingleFiberExecutor, lvalue_test) {
    using sfe = ssx::single_fiber_executor<fn_t&>;
    constexpr std::expected<ss::sstring, sfe::errc> interrupted{
      std::unexpect, sfe::errc::interrupted};

    ssx::single_fiber_executor<fn_t&> executor;
    fn_t f = make_ready_coro("ret1");
    { // superseded ones may or may not complete
        auto f1 = executor.submit(f);
        auto f2 = executor.submit(f);
        auto f3 = executor.submit(f);
        ASSERT_THAT(f1.get(), InterruptedOr("ret1"));
        ASSERT_THAT(f2.get(), InterruptedOr("ret1"));
        ASSERT_THAT(f3.get(), CompletedAnd("ret1"));
    }
    auto drain_f = executor.drain();
    drain_f.get();
}

namespace {
constexpr auto make_fn(int retval) {
    return [retval](ss::abort_source&) { return ssx::now(retval); };
}
} // namespace

TEST(SingleFiberExecutor, constexpr_test) {
    // this test demonstrates how to use it with a lambda factory without a
    // type-erasing wrapper

    // rvalue
    ssx::single_fiber_executor<decltype(make_fn(0))> executor_r;
    auto f1 = executor_r.submit(make_fn(1));
    ASSERT_EQ(f1.get(), 1);

    // lvalue
    ssx::single_fiber_executor<decltype(make_fn(0))&> executor_l;
    auto fn = make_fn(5);
    auto f2 = executor_l.submit(fn);
    ASSERT_EQ(f2.get(), 5);
}

using manual_rcn = basic_retry_chain_node<ss::manual_clock>;

template<ssx::abortable_async_fn Func, typename StopCondition>
using manual_repeater_with_rcn
  = ssx::basic_repeater_with_rcn<Func, StopCondition, manual_rcn>;

namespace {

ss::future<> advance_clock(ss::manual_clock::duration dur) {
    ss::manual_clock::advance(dur);
    co_await tests::drain_task_queue();
}

ss::future<>
run_to_completion(ss::future<int>& f, ss::manual_clock::duration step) {
    while (!f.available()) {
        co_await advance_clock(step);
    }
}
} // namespace

TEST_CORO(RepeaterWithRcn, basic_test) {
    ss::abort_source as;
    manual_rcn parent_rcn{as, 1min, 1ms};

    int counter = 0;
    auto func = [&counter](ss::abort_source&) -> ss::future<int> {
        return ssx::now(++counter);
    };
    auto stop = [](int v) {
        return v == 3 ? ss::stop_iteration::yes : ss::stop_iteration::no;
    };
    manual_repeater_with_rcn repeater(func, stop, &parent_rcn);

    auto f = repeater(as);
    co_await run_to_completion(f, 1ms);
    ASSERT_EQ_CORO(co_await std::move(f), 3);
}

TEST_CORO(RepeaterWithRcn, slow_retry_test) {
    ss::abort_source as;
    int counter = 0;
    auto func = [&counter](ss::abort_source&) -> ss::future<int> {
        return ssx::now(++counter);
    };
    auto stop = [](int) { return ss::stop_iteration::no; };
    manual_repeater_with_rcn repeater(
      std::move(func), // rvalues supported too
      std::move(stop), // rvalues supported too
      as,
      1000ms,
      100ms,
      retry_strategy::polling);

    auto f = repeater(as);
    co_await run_to_completion(f, 10ms);
    auto cnt_called = co_await std::move(f);

    // each delay is between 100 and 200ms due to jitter
    ASSERT_GE_CORO(cnt_called, 5);
    ASSERT_LE_CORO(cnt_called, 10);
}

TEST_CORO(RepeaterWithRcn, slow_execution_test) {
    ss::abort_source as;
    manual_rcn parent_rcn{as, 350ms, 1ms};
    int counter = 0;
    auto func = [&counter](ss::abort_source& as) -> ss::future<int> {
        return ss::sleep_abortable<ss::manual_clock>(100ms, as).then_wrapped(
          [&counter](auto&& f) {
              if (f.failed()) {
                  // will not happen: RCN won't trigger abort source
                  counter += 100;
              } else {
                  ++counter;
              }
              f.ignore_ready_future();
              return ssx::now(counter);
          });
    };
    auto stop = [](int) { return ss::stop_iteration::no; };
    manual_repeater_with_rcn repeater(func, stop, &parent_rcn);

    auto f = repeater(as);
    co_await run_to_completion(f, 1ms);
    ASSERT_EQ_CORO(co_await std::move(f), 4);
}

TEST_CORO(RepeaterWithRcn, aborted_during_delay_test) {
    ss::abort_source as;
    manual_repeater_with_rcn repeater(
      [](ss::abort_source&) { return ssx::now(0); },
      [](int) { return ss::stop_iteration::no; },
      as,
      1000ms,
      100ms,
      retry_strategy::polling);

    auto f = repeater(as);
    co_await advance_clock(50ms);
    as.request_abort();
    ASSERT_THROW_CORO(co_await std::move(f), ss::sleep_aborted);
}

TEST_CORO(RepeaterWithRcn, aborted_during_execution_test) {
    ss::abort_source as;
    constexpr auto rcn_timeout = 350ms;
    constexpr auto func_duration = 100ms;
    constexpr auto step = 10ms;
    manual_rcn parent_rcn{as, rcn_timeout, 1ms};
    int counter = 0;
    auto func = [&counter,
                 func_duration](ss::abort_source& as) -> ss::future<int> {
        return ss::sleep_abortable<ss::manual_clock>(func_duration, as)
          .then_wrapped([&counter](auto&& f) {
              if (f.failed()) {
                  counter += 100;
              } else {
                  ++counter;
              }
              f.ignore_ready_future();
              return ssx::now(counter);
          });
    };
    auto stop = [](int r) {
        // stopping if execution aborted, as otherwise RCN retry check throws
        return r >= 100 ? ss::stop_iteration::yes : ss::stop_iteration::no;
    };
    manual_repeater_with_rcn repeater(func, stop, &parent_rcn);

    auto f = repeater(as);
    // advance in small steps so cascading timers fire incrementally:
    // 3 func executions complete (~300ms), 4th starts and is in progress
    for (auto elapsed = 0ms; elapsed < rcn_timeout; elapsed += step) {
        co_await advance_clock(step);
    }
    ASSERT_FALSE_CORO(f.available());
    as.request_abort();
    co_await tests::drain_task_queue();
    ASSERT_EQ_CORO(co_await std::move(f), 103);
}

TEST_CORO(RepeaterWithRcn, disallowed_retries_test) {
    ss::abort_source as;
    int counter = 0;
    manual_repeater_with_rcn repeater(
      [&counter](ss::abort_source&) { return ssx::now(++counter); },
      [](int) { return ss::stop_iteration::no; },
      as,
      1min,
      1s,
      retry_strategy::disallow);

    auto f = repeater(as);
    ASSERT_EQ_CORO(
      co_await std::move(f), 1); // run once, then no retries allowed
}

TEST_CORO(RepeaterWithRcn, originally_expired_rcn_test) {
    ss::abort_source as;
    manual_repeater_with_rcn repeater(
      [](ss::abort_source&) -> ss::future<int> {
          throw std::runtime_error("should not be called");
      },
      [](int) -> ss::stop_iteration {
          throw std::runtime_error("should not be called");
      },
      as,
      ss::manual_clock::now() - 1min, // deadline in the past
      65s);

    auto f = repeater(as);
    ASSERT_EQ_CORO(co_await std::move(f), 0);
}
