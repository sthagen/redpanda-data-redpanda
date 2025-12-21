/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/pipeline/write_pipeline.h"
#include "container/chunked_circular_buffer.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <iterator>
#include <limits>
#include <tuple>

using namespace std::chrono_literals;

static cloud_topics::cluster_epoch min_epoch{3840};

namespace cloud_topics::l0 {
struct write_pipeline_accessor {
    // Returns true if the write request is in the `_pending` collection
    bool write_requests_pending(size_t n) {
        return pipeline->get_pending().size() == n;
    }

    write_pipeline<ss::manual_clock>* pipeline;
};
} // namespace cloud_topics::l0

// Simulate sleep of certain duration and wait until the condition is met
template<class Fn>
ss::future<>
sleep_until(std::chrono::milliseconds delta, Fn&& fn, int retry_limit = 100) {
    ss::manual_clock::advance(delta);
    for (int i = 0; i < retry_limit; i++) {
        co_await ss::yield();
        if (fn()) {
            co_return;
        }
    }
    GTEST_MESSAGE_("Test stalled", ::testing::TestPartResult::kFatalFailure);
}

TEST_CORO(write_pipeline_test, single_write_request) {
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };
    // Expect single upload to be made

    auto stage = pipeline.register_write_pipeline_stage();

    const auto timeout = ss::manual_clock::now() + 1s;
    auto fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, {}, timeout);

    // Make sure the write request is in the _pending list
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });

    auto res = stage.pull_write_requests(1);
    ASSERT_TRUE_CORO(res.complete);
    ASSERT_TRUE_CORO(res.requests.size() == 1);

    res.requests.front().set_value(chunked_vector<cloud_topics::extent_meta>{});

    auto write_res = co_await std::move(fut);
    ASSERT_TRUE_CORO(write_res.has_value());
}

TEST_CORO(batcher_test, expired_write_request) {
    // The test starts two write request but one of which is expected to
    // timeout.
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage = pipeline.register_write_pipeline_stage();

    static constexpr auto timeout = 1s;
    auto deadline = ss::manual_clock::now() + 1s;
    auto expect_fail_fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, {}, deadline);

    // Expire first request
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });
    ss::manual_clock::advance(timeout);

    deadline = ss::manual_clock::now() + 1s;
    auto expect_pass_fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, {}, deadline);

    // Make sure that both write requests are pending
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(2); });

    auto res = stage.pull_write_requests(1);

    // One req has already expired at this point
    ASSERT_EQ_CORO(res.requests.size(), 1);
    res.requests.back().set_value(chunked_vector<cloud_topics::extent_meta>{});

    auto [pass_result, fail_result] = co_await ss::when_all_succeed(
      std::move(expect_pass_fut), std::move(expect_fail_fut));

    ASSERT_TRUE_CORO(!fail_result.has_value());

    ASSERT_TRUE_CORO(pass_result.has_value());
}

TEST_CORO(write_pipeline_test, stage_bytes_accounting) {
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage = pipeline.register_write_pipeline_stage();

    ASSERT_EQ_CORO(pipeline.stage_bytes(stage.id()), 0);

    const auto timeout = ss::manual_clock::now() + 1s;
    auto test_data = co_await model::test::make_random_batches(
      {.count = 1, .records = 5});
    chunked_vector<model::record_batch> batches;
    std::ranges::move(std::move(test_data), std::back_inserter(batches));

    auto fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, std::move(batches), timeout);

    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });

    // Bytes in the stage.
    ASSERT_GT_CORO(pipeline.stage_bytes(stage.id()), 0);

    // Pull requests -- bytes leave the stage.
    auto res = stage.pull_write_requests(std::numeric_limits<size_t>::max());
    ASSERT_EQ_CORO(pipeline.stage_bytes(stage.id()), 0);

    res.requests.front().set_value(chunked_vector<cloud_topics::extent_meta>{});
    auto write_res = co_await std::move(fut);
    ASSERT_TRUE_CORO(write_res.has_value());
}

TEST_CORO(write_pipeline_test, stage_bytes_accounting_on_timeout) {
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage = pipeline.register_write_pipeline_stage();

    ASSERT_EQ_CORO(pipeline.stage_bytes(stage.id()), 0);

    static constexpr auto timeout_duration = 1s;
    const auto timeout = ss::manual_clock::now() + timeout_duration;
    auto test_data = co_await model::test::make_random_batches(
      {.count = 1, .records = 5});
    chunked_vector<model::record_batch> batches;
    std::ranges::move(std::move(test_data), std::back_inserter(batches));

    auto fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, std::move(batches), timeout);

    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });

    // Bytes in the stage.
    ASSERT_GT_CORO(pipeline.stage_bytes(stage.id()), 0);

    // "Wait" until the requests time out.
    ss::manual_clock::advance(timeout_duration);

    // Trigger timeout detection by pulling requests.
    auto res = stage.pull_write_requests(std::numeric_limits<size_t>::max());
    ASSERT_EQ_CORO(res.requests.size(), 0);

    // Stage bytes are released when the future resolves.
    auto write_res = co_await std::move(fut);
    ASSERT_FALSE_CORO(write_res.has_value());
    ASSERT_EQ_CORO(pipeline.stage_bytes(stage.id()), 0);
}
