/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/single_flight.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/coroutine/as_future.hh>

#include <gtest/gtest.h>

#include <stdexcept>

using namespace cloud_topics::l1;

namespace {

// outcome constructors: success carries no value, failure carries an errc.
single_flight::outcome ok() { return {}; }
single_flight::outcome fail(io::errc e) { return std::unexpected(e); }

ss::future<single_flight::outcome> ready(single_flight::outcome o) {
    return ss::make_ready_future<single_flight::outcome>(std::move(o));
}

// Work that fails the test if invoked. Used as the merger's work
// argument: the merger must never run its own work.
ss::future<single_flight::outcome> work_never_runs() {
    ADD_FAILURE() << "merger work invoked";
    return ready(fail(io::errc::file_io_error));
}

} // namespace

TEST_CORO(SingleFlightTest, ColdRunBecomesLeader) {
    single_flight map;
    ASSERT_EQ_CORO(map.in_flight(), 0u);

    ss::abort_source as;
    bool work_ran = false;
    auto r = co_await map.run("/a", as, [&] {
        work_ran = true;
        // Entry exists for the duration of work().
        EXPECT_EQ(map.in_flight(), 1u);
        return ready(ok());
    });

    ASSERT_TRUE_CORO(work_ran);
    ASSERT_TRUE_CORO(r.has_value());
    ASSERT_FALSE_CORO(*r); // ran work, did not merge
    ASSERT_EQ_CORO(map.in_flight(), 0u);
}

// An already-aborted caller short-circuits to cloud_op_timeout without
// running work or inserting an entry (so it never becomes a leader).
TEST_CORO(SingleFlightTest, AbortedBeforeRunShortCircuits) {
    single_flight map;
    ss::abort_source as;
    as.request_abort();

    bool work_ran = false;
    auto r = co_await map.run("/a", as, [&] {
        work_ran = true;
        return ready(ok());
    });

    ASSERT_FALSE_CORO(work_ran);
    ASSERT_FALSE_CORO(r.has_value());
    ASSERT_EQ_CORO(r.error(), io::errc::cloud_op_timeout);
    ASSERT_EQ_CORO(map.in_flight(), 0u);
}

TEST_CORO(SingleFlightTest, ConcurrentRunsLeaderPublishesSuccess) {
    single_flight map;
    ss::promise<single_flight::outcome> leader_p;

    ss::abort_source leader_as;
    auto leader_fut = map.run(
      "/a", leader_as, [&] { return leader_p.get_future(); });
    // Leader has inserted its entry and suspended on the work future.
    ASSERT_EQ_CORO(map.in_flight(), 1u);

    ss::abort_source merger_as;
    auto merger_fut = map.run("/a", merger_as, work_never_runs);
    // Merger joined the existing leader; no second entry.
    ASSERT_EQ_CORO(map.in_flight(), 1u);

    leader_p.set_value(ok());

    auto leader_r = co_await std::move(leader_fut);
    auto merger_r = co_await std::move(merger_fut);

    ASSERT_TRUE_CORO(leader_r.has_value());
    ASSERT_FALSE_CORO(*leader_r);

    ASSERT_TRUE_CORO(merger_r.has_value());
    ASSERT_TRUE_CORO(*merger_r); // merged

    ASSERT_EQ_CORO(map.in_flight(), 0u);
}

TEST_CORO(SingleFlightTest, MergerInheritsLeaderErrc) {
    single_flight map;
    ss::promise<single_flight::outcome> leader_p;

    ss::abort_source leader_as;
    auto leader_fut = map.run(
      "/a", leader_as, [&] { return leader_p.get_future(); });

    ss::abort_source merger_as;
    auto merger_fut = map.run("/a", merger_as, work_never_runs);

    leader_p.set_value(fail(io::errc::cloud_op_error));

    auto leader_r = co_await std::move(leader_fut);
    auto merger_r = co_await std::move(merger_fut);

    ASSERT_FALSE_CORO(leader_r.has_value());
    ASSERT_EQ_CORO(leader_r.error(), io::errc::cloud_op_error);

    ASSERT_FALSE_CORO(merger_r.has_value());
    ASSERT_EQ_CORO(merger_r.error(), io::errc::cloud_op_error);
}

// The merger's own abort_source fires while it waits. This manifests as a
// cloud_op_timeout in the merger's result.
TEST_CORO(SingleFlightTest, MergerAbortSurfacesAsTimeout) {
    single_flight map;
    ss::promise<single_flight::outcome> leader_p;

    ss::abort_source leader_as;
    auto leader_fut = map.run(
      "/a", leader_as, [&] { return leader_p.get_future(); });

    ss::abort_source merger_as;
    auto merger_fut = map.run("/a", merger_as, work_never_runs);

    merger_as.request_abort();

    auto merger_r = co_await std::move(merger_fut);
    ASSERT_FALSE_CORO(merger_r.has_value());
    ASSERT_EQ_CORO(merger_r.error(), io::errc::cloud_op_timeout);

    // Leader still has to publish; assert it completes cleanly as a
    // (non-merged) leader success.
    leader_p.set_value(ok());
    auto leader_r = co_await std::move(leader_fut);
    ASSERT_TRUE_CORO(leader_r.has_value());
    ASSERT_FALSE_CORO(*leader_r); // led, did not merge
}

// If the leader's work raises, run() never rethrows: it maps the
// exception to file_io_error for the leader and any mergers, and
// releases the entry.
TEST_CORO(SingleFlightTest, LeaderWorkThrowsReportsFileIoError) {
    single_flight map;
    ss::promise<single_flight::outcome> leader_p;

    ss::abort_source leader_as;
    auto leader_fut = map.run(
      "/a", leader_as, [&](this auto) -> ss::future<single_flight::outcome> {
          co_await leader_p.get_future();
          throw std::runtime_error("boom");
      });

    ss::abort_source merger_as;
    auto merger_fut = map.run("/a", merger_as, work_never_runs);

    leader_p.set_value(ok());

    auto leader_r = co_await std::move(leader_fut);
    ASSERT_FALSE_CORO(leader_r.has_value());
    ASSERT_EQ_CORO(leader_r.error(), io::errc::file_io_error);

    auto merger_r = co_await std::move(merger_fut);
    ASSERT_FALSE_CORO(merger_r.has_value());
    ASSERT_EQ_CORO(merger_r.error(), io::errc::file_io_error);

    ASSERT_EQ_CORO(map.in_flight(), 0u);
}

// At max_entries, additional callers run work uncoordinated. They
// observe merged=false and do not insert into the map.
TEST_CORO(SingleFlightTest, AtCapacityRunsUncoordinated) {
    single_flight map(/*max_entries=*/2);
    ss::promise<single_flight::outcome> p_a;
    ss::promise<single_flight::outcome> p_b;

    ss::abort_source as_a;
    ss::abort_source as_b;
    auto fut_a = map.run("/a", as_a, [&] { return p_a.get_future(); });
    auto fut_b = map.run("/b", as_b, [&] { return p_b.get_future(); });
    ASSERT_EQ_CORO(map.in_flight(), 2u);

    // Two concurrent callers for the same key /c, both held open. At
    // capacity each runs its own work rather than one joining the other.
    ss::promise<single_flight::outcome> p_c1;
    ss::promise<single_flight::outcome> p_c2;
    bool c1_ran = false;
    bool c2_ran = false;
    ss::abort_source as_c1;
    ss::abort_source as_c2;
    auto fut_c1 = map.run("/c", as_c1, [&] {
        c1_ran = true;
        return p_c1.get_future();
    });
    auto fut_c2 = map.run("/c", as_c2, [&] {
        c2_ran = true;
        return p_c2.get_future();
    });
    // Both ran their own work; neither registered (map still full).
    ASSERT_TRUE_CORO(c1_ran);
    ASSERT_TRUE_CORO(c2_ran);
    ASSERT_EQ_CORO(map.in_flight(), 2u);

    p_c1.set_value(ok());
    p_c2.set_value(ok());
    auto r_c1 = co_await std::move(fut_c1);
    auto r_c2 = co_await std::move(fut_c2);
    ASSERT_TRUE_CORO(r_c1.has_value());
    ASSERT_TRUE_CORO(r_c2.has_value());
    ASSERT_FALSE_CORO(*r_c1); // ran uncoordinated, did not merge
    ASSERT_FALSE_CORO(*r_c2); // ran uncoordinated, did not merge

    p_a.set_value(ok());
    p_b.set_value(ok());
    co_await std::move(fut_a);
    co_await std::move(fut_b);
    ASSERT_EQ_CORO(map.in_flight(), 0u);
}

// After a leader releases, the slot it held is free for a new key.
TEST_CORO(SingleFlightTest, SlotReusableAfterLeaderRelease) {
    single_flight map(/*max_entries=*/1);

    {
        ss::abort_source as;
        co_await map.run("/a", as, [] { return ready(ok()); });
    }
    ASSERT_EQ_CORO(map.in_flight(), 0u);

    // /b takes the freed slot as a leader and holds it open.
    ss::promise<single_flight::outcome> p_b;
    ss::abort_source leader_as;
    auto leader_fut = map.run(
      "/b", leader_as, [&] { return p_b.get_future(); });
    ASSERT_EQ_CORO(map.in_flight(), 1u);

    // A concurrent /b joins the leader rather than running its own work,
    // proving the slot is live and /b is joinable.
    ss::abort_source merger_as;
    auto merger_fut = map.run("/b", merger_as, work_never_runs);
    ASSERT_EQ_CORO(map.in_flight(), 1u);

    p_b.set_value(ok());
    auto leader_r = co_await std::move(leader_fut);
    auto merger_r = co_await std::move(merger_fut);
    ASSERT_TRUE_CORO(leader_r.has_value());
    ASSERT_FALSE_CORO(*leader_r); // led
    ASSERT_TRUE_CORO(merger_r.has_value());
    ASSERT_TRUE_CORO(*merger_r); // merged -> /b was joinable
}
