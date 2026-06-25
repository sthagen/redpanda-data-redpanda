/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/reservation_policy.h"
#include "cloud_io/scheduler_types.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/later.hh>

#include <array>
#include <chrono>
#include <vector>

namespace {

using namespace std::chrono_literals;

template<typename T>
ss::future<T> with_test_timeout(ss::future<T> fut) {
    return ss::with_timeout(ss::lowres_clock::now() + 5s, std::move(fut));
}

ss::future<> tick(ss::manual_clock::duration d) {
    ss::manual_clock::advance(d);
    co_await ss::yield();
}

// Apply target_reserved + capacity post-construction.
void configure(
  cloud_io::reservation_policy<ss::manual_clock>& policy,
  size_t total_slots = 20,
  std::array<uint32_t, cloud_io::num_group_ids> target_reserved = {0, 0, 0}) {
    // Reset reservations first so set_total_slots doesn't fight a
    // shrinking capacity vs an already-configured floor sum.
    for (size_t i = 0; i < cloud_io::num_group_ids; ++i) {
        policy.set_target_reserved(static_cast<cloud_io::group_id>(i), 0);
    }
    policy.set_total_slots(total_slots);
    for (size_t i = 0; i < cloud_io::num_group_ids; ++i) {
        policy.set_target_reserved(
          static_cast<cloud_io::group_id>(i), target_reserved[i]);
    }
}

} // namespace

TEST_CORO(ReservationPolicyTest, BasicAdmitReleaseAndAccounting) {
    cloud_io::reservation_policy<ss::manual_clock> policy{4};
    configure(policy, /*total_slots=*/4);

    EXPECT_EQ(policy.total_capacity(), 4u);
    EXPECT_EQ(policy.available_slots(), 4u);
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::consumer_fetch), 0u);

    ss::abort_source as;

    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::consumer_fetch), 1u);
    EXPECT_EQ(policy.available_slots(), 3u);

    // Resize up: 1 in-flight + 3 free, new capacity 6 → 5 free.
    policy.set_total_slots(6);
    EXPECT_EQ(policy.total_capacity(), 6u);
    EXPECT_EQ(policy.available_slots(), 5u);

    // Resize down: 1 in-flight + 5 free, new capacity 3 → 2 free.
    policy.set_total_slots(3);
    EXPECT_EQ(policy.total_capacity(), 3u);
    EXPECT_EQ(policy.available_slots(), 2u);

    policy.release(cloud_io::group_id::consumer_fetch);
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::consumer_fetch), 0u);
    EXPECT_EQ(policy.available_slots(), 3u);
}

TEST_CORO(ReservationPolicyTest, SaturationCausesQueueing) {
    cloud_io::reservation_policy<ss::manual_clock> policy{2};
    configure(policy, 2);
    ss::abort_source as;

    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::consumer_fetch, as));
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(policy.available_slots(), 0u);

    auto fut = policy.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::yield();
    EXPECT_FALSE(fut.available());
    EXPECT_EQ(policy.waiters(cloud_io::group_id::consumer_fetch), 1u);

    policy.release(cloud_io::group_id::consumer_fetch);
    co_await with_test_timeout(std::move(fut));
    EXPECT_EQ(policy.waiters(cloud_io::group_id::consumer_fetch), 0u);
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::consumer_fetch), 2u);

    policy.release(cloud_io::group_id::consumer_fetch);
    policy.release(cloud_io::group_id::consumer_fetch);
}

TEST_CORO(ReservationPolicyTest, AbortCancelsQueuedWait) {
    cloud_io::reservation_policy<ss::manual_clock> policy{1};
    configure(policy, 1);
    ss::abort_source as;

    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::consumer_fetch, as));
    auto queued = policy.admit(cloud_io::group_id::consumer_fetch, as);

    co_await ss::yield();
    EXPECT_FALSE(queued.available());

    as.request_abort();
    auto r = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(queued)));
    EXPECT_TRUE(r.failed());
    r.ignore_ready_future();

    policy.release(cloud_io::group_id::consumer_fetch);
}

TEST_CORO(ReservationPolicyTest, AlreadyAbortedSourceRejectsSlowPath) {
    cloud_io::reservation_policy<ss::manual_clock> policy{1};
    configure(policy, 1);
    ss::abort_source as;

    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(policy.available_slots(), 0u);

    as.request_abort();

    auto queued = policy.admit(cloud_io::group_id::consumer_fetch, as);
    auto r = co_await ss::coroutine::as_future(
      with_test_timeout(std::move(queued)));
    EXPECT_TRUE(r.failed());
    r.ignore_ready_future();

    policy.release(cloud_io::group_id::consumer_fetch);
}

TEST_CORO(ReservationPolicyTest, CrossGroupDispatchWakesQueuedWaiter) {
    cloud_io::reservation_policy<ss::manual_clock> policy{1};
    configure(policy, 1);
    ss::abort_source as;

    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(policy.available_slots(), 0u);

    auto b_fut = policy.admit(cloud_io::group_id::default_group, as);
    co_await ss::yield();
    EXPECT_FALSE(b_fut.available());
    EXPECT_EQ(policy.waiters(cloud_io::group_id::default_group), 1u);

    policy.release(cloud_io::group_id::consumer_fetch);
    co_await with_test_timeout(std::move(b_fut));
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::default_group), 1u);
    EXPECT_EQ(policy.waiters(cloud_io::group_id::default_group), 0u);

    policy.release(cloud_io::group_id::default_group);
}

TEST_CORO(ReservationPolicyTest, MultiGroupFillsCapacity) {
    cloud_io::reservation_policy<ss::manual_clock> policy{4};
    configure(policy, /*total_slots=*/4);
    ss::abort_source as;

    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::consumer_fetch, as));
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::producer_upload, as));
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::consumer_fetch, as));
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::default_group, as));

    EXPECT_EQ(policy.in_flight(cloud_io::group_id::consumer_fetch), 2u);
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::producer_upload), 1u);
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::default_group), 1u);
    EXPECT_EQ(policy.available_slots(), 0u);

    policy.release(cloud_io::group_id::consumer_fetch);
    policy.release(cloud_io::group_id::consumer_fetch);
    policy.release(cloud_io::group_id::producer_upload);
    policy.release(cloud_io::group_id::default_group);
    EXPECT_EQ(policy.available_slots(), 4u);
}

TEST_CORO(ReservationPolicyTest, ReservationSemantics) {
    // capacity=6, pu_min=2 -> _shared=4, _reserved[pu]=2.
    // Once pu is active, verify cf cannot consume pu's reserved slots.
    // Then queue a cf waiter and verify the first pu releases (reserved)
    // do NOT dispatch cf; the slot returns to pu's reserved pool. Only
    // the shared release dispatches cf.
    cloud_io::reservation_policy<ss::manual_clock> policy{6};
    configure(policy, /*total_slots=*/6, /*target_reserved=*/{2, 0, 0});
    ss::abort_source as;

    // Warm pu so it becomes non-idle and its reservation is protected
    // from reclaim. The first admit drains pu's untouched reservation
    // through the common pool; the matching releases refill the
    // reservation back to target via the under-target refill path.
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::producer_upload, as));
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::producer_upload, as));
    policy.release(cloud_io::group_id::producer_upload);
    policy.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 2u);

    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(policy.try_admit(cloud_io::group_id::consumer_fetch));
    }
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::consumer_fetch), 4u);
    EXPECT_EQ(policy.available_slots(), 2u);
    EXPECT_FALSE(policy.try_admit(cloud_io::group_id::consumer_fetch));

    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::producer_upload, as));
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::producer_upload, as));
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::producer_upload), 2u);
    EXPECT_EQ(policy.available_slots(), 0u);

    auto cf5 = policy.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::yield();
    EXPECT_EQ(policy.waiters(cloud_io::group_id::consumer_fetch), 1u);

    policy.release(cloud_io::group_id::producer_upload);
    co_await ss::yield();
    EXPECT_EQ(policy.waiters(cloud_io::group_id::consumer_fetch), 1u);
    EXPECT_FALSE(cf5.available());

    policy.release(cloud_io::group_id::producer_upload);
    co_await ss::yield();
    EXPECT_EQ(policy.waiters(cloud_io::group_id::consumer_fetch), 1u);
    EXPECT_FALSE(cf5.available());

    policy.release(cloud_io::group_id::consumer_fetch);
    co_await with_test_timeout(std::move(cf5));

    policy.release(cloud_io::group_id::consumer_fetch);
    policy.release(cloud_io::group_id::consumer_fetch);
    policy.release(cloud_io::group_id::consumer_fetch);
}

// One group consumes from both pools (its reservation plus slots
// from the common pool), then releases them all. Verifies the
// release routing decrements the right counters in the right order
// (reservation-sourced in-flight is returned to the reservation
// first, then excess releases flow back to the common pool).
TEST_CORO(ReservationPolicyTest, ReservedAndSharedSlotsCoexist) {
    cloud_io::reservation_policy<ss::manual_clock> policy{6};
    configure(policy, /*total_slots=*/6, /*target_reserved=*/{2, 0, 0});
    ss::abort_source as;

    for (int i = 0; i < 4; ++i) {
        co_await with_test_timeout(
          policy.admit(cloud_io::group_id::producer_upload, as));
    }
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::producer_upload), 4u);
    EXPECT_EQ(policy.available_slots(), 2u);

    policy.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(policy.available_slots(), 3u);
    policy.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(policy.available_slots(), 4u);
    policy.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(policy.available_slots(), 5u);
    policy.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(policy.available_slots(), 6u);
}

TEST_CORO(ReservationPolicyTest, ReclaimsIdleReservationToCommonPool) {
    cloud_io::reservation_policy<ss::manual_clock> policy{6};
    configure(policy, /*total_slots=*/6, /*target_reserved=*/{2, 0, 0});

    ss::abort_source as;

    // Warm pu so its reservation is restored to target via the refill
    // path (first admit drains the untouched reservation through the
    // common pool; each release refills 1 slot back via the
    // under-target rule).
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::producer_upload, as));
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::producer_upload, as));
    policy.release(cloud_io::group_id::producer_upload);
    policy.release(cloud_io::group_id::producer_upload);
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 2u);

    co_await tick(cloud_io::default_dwell_duration + 1s);
    EXPECT_TRUE(policy.try_admit(cloud_io::group_id::default_group));

    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 0u);
    EXPECT_EQ(policy.available_slots(), 5u);

    policy.release(cloud_io::group_id::default_group);
}

TEST_CORO(ReservationPolicyTest, ReclaimSparesNonIdleGroups) {
    cloud_io::reservation_policy<ss::manual_clock> policy{6};
    configure(policy, /*total_slots=*/6, /*target_reserved=*/{2, 0, 0});
    ss::abort_source as;
    constexpr auto pu = cloud_io::group_id::producer_upload;

    co_await with_test_timeout(policy.admit(pu, as));
    EXPECT_EQ(policy.state(pu), cloud_io::group_state::active);

    // Timer fire under `active` — reservation must persist.
    co_await tick(2s);
    EXPECT_EQ(policy.current_reserved(pu), 2u);

    policy.release(pu);
    EXPECT_EQ(policy.state(pu), cloud_io::group_state::dwelling);

    // Timer fire within dwell window — reservation must persist.
    co_await tick(2s);
    EXPECT_EQ(policy.current_reserved(pu), 2u);

    // Past dwell — next timer drains.
    co_await tick(4s);
    EXPECT_EQ(policy.state(pu), cloud_io::group_state::idle);
    EXPECT_EQ(policy.current_reserved(pu), 0u);
}

TEST_CORO(
  ReservationPolicyTest,
  InactiveReservationLentAndReclaimedByUndertargetWaiter) {
    // A never-active group's seeded reservation is reclaim-eligible, so
    // an active peer can saturate the pool by drawing on those slots
    // when its own share is full. When the inactive group becomes
    // active and queues, under-target dispatch routes the next peer
    // release to its waiter before any peer waiter or refill — the
    // inactive group reclaims its capacity on demand.
    cloud_io::reservation_policy<ss::manual_clock> policy{6};
    configure(policy, /*total_slots=*/6, /*target_reserved=*/{2, 0, 0});
    ss::abort_source as;

    // Let the reclaim timer drain pu's untouched reservation.
    co_await tick(2s);

    // cf saturates the pool now that pu's slots are in the common pool.
    for (int i = 0; i < 6; ++i) {
        EXPECT_TRUE(policy.try_admit(cloud_io::group_id::consumer_fetch));
    }
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::consumer_fetch), 6u);
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 0u);
    EXPECT_EQ(policy.available_slots(), 0u);

    // pu queues; it's under-target (in_flight=0 < target=2).
    auto pu_admit = policy.admit(cloud_io::group_id::producer_upload, as);
    co_await ss::yield();
    EXPECT_FALSE(pu_admit.available());
    EXPECT_EQ(policy.waiters(cloud_io::group_id::producer_upload), 1u);

    // First cf release routes to pu via under-target dispatch.
    policy.release(cloud_io::group_id::consumer_fetch);
    co_await with_test_timeout(std::move(pu_admit));
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::producer_upload), 1u);
    EXPECT_EQ(policy.waiters(cloud_io::group_id::producer_upload), 0u);

    policy.release(cloud_io::group_id::producer_upload);
    for (int i = 0; i < 5; ++i) {
        policy.release(cloud_io::group_id::consumer_fetch);
    }
}

// Contrast to InactiveReservationLentAndReclaimedByUndertargetWaiter above:
// there the saturating group (cf) has NO reservation, so its release flows
// through dispatch_next and the under-target peer reclaims its slot. Here cf
// DOES have a reservation (the production default is consumer_fetch:2). A
// group that holds common slots ABOVE its reservation must still release
// them to an under-target peer; it must not hoard them just because it has
// reserved slots in flight. This is the Run-6 reconciler-stall root cause:
// consumer_fetch saturates the pool while default_group (the reconciler's
// lane) sits under its reservation target, queued, and never gets dispatched.
TEST_CORO(
  ReservationPolicyTest, ReservedSaturatedGroupDoesNotStarveUndertargetPeer) {
    // capacity=6; cf=2, default=2, pu=mc=0 -> _shared=2.
    cloud_io::reservation_policy<ss::manual_clock> policy{6};
    configure(policy, /*total_slots=*/6, /*target_reserved=*/{0, 2, 0, 2});
    ss::abort_source as;

    // cf grabs and holds both reserved slots; staying active protects its
    // reservation across the reclaim tick below.
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::consumer_fetch, as));
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::consumer_fetch, as));
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::consumer_fetch), 2u);
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::consumer_fetch), 2u);

    // default never activates; the idle sweep reclaims its seeded
    // reservation to the common pool, so it must win its reservation back
    // via dispatch on a peer release.
    co_await tick(cloud_io::default_dwell_duration + 1s);
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::default_group), 0u);

    // cf floods the common pool and queues a waiter.
    while (policy.try_admit(cloud_io::group_id::consumer_fetch)) {
    }
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::consumer_fetch), 6u);
    EXPECT_EQ(policy.available_slots(), 0u);
    auto cf_waiter = policy.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::yield();
    EXPECT_FALSE(cf_waiter.available());

    // default queues: under its reserved target (in_flight 0 < target 2).
    auto default_waiter = policy.admit(cloud_io::group_id::default_group, as);
    co_await ss::yield();
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::default_group), 0u);
    EXPECT_EQ(policy.waiters(cloud_io::group_id::default_group), 1u);

    // Releasing a cf slot frees a COMMON slot (cf holds 6, reserved only 2),
    // so it must route to the under-target default via dispatch_next, not be
    // re-grabbed by cf.
    policy.release(cloud_io::group_id::consumer_fetch);
    co_await ss::yield();
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::default_group), 1u);
    EXPECT_EQ(policy.waiters(cloud_io::group_id::default_group), 0u);

    // Cleanup: cancel queued waiters, then drain all in-flight.
    as.request_abort();
    {
        auto r = co_await ss::coroutine::as_future(
          with_test_timeout(std::move(cf_waiter)));
        r.ignore_ready_future();
    }
    {
        auto r = co_await ss::coroutine::as_future(
          with_test_timeout(std::move(default_waiter)));
        r.ignore_ready_future();
    }
    for (auto g : cloud_io::all_group_ids) {
        while (policy.in_flight(g) > 0) {
            policy.release(g);
        }
    }
}

TEST_CORO(ReservationPolicyTest, WorkConservingCycleReclaimThenRefill) {
    cloud_io::reservation_policy<ss::manual_clock> policy{6};
    configure(policy, /*total_slots=*/6, /*target_reserved=*/{2, 0, 0});

    ss::abort_source as;

    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::producer_upload, as));
    policy.release(cloud_io::group_id::producer_upload);
    co_await tick(cloud_io::default_dwell_duration + 1s);
    EXPECT_TRUE(policy.try_admit(cloud_io::group_id::default_group));
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 0u);

    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(policy.try_admit(cloud_io::group_id::default_group));
    }
    EXPECT_EQ(policy.available_slots(), 0u);

    auto pu_wake = policy.admit(cloud_io::group_id::producer_upload, as);
    co_await ss::yield();
    EXPECT_FALSE(pu_wake.available());
    EXPECT_EQ(policy.waiters(cloud_io::group_id::producer_upload), 1u);

    policy.release(cloud_io::group_id::default_group);
    co_await with_test_timeout(std::move(pu_wake));
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::producer_upload), 1u);

    policy.release(cloud_io::group_id::default_group);
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 1u);
    policy.release(cloud_io::group_id::default_group);
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 2u);

    policy.release(cloud_io::group_id::default_group);
    policy.release(cloud_io::group_id::producer_upload);
    policy.release(cloud_io::group_id::default_group);
    policy.release(cloud_io::group_id::default_group);

    // Refill is capped at target_reserved: the trailing releases never
    // push producer_upload's reservation above 2.
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 2u);
}

// When a group is queued and under target, dispatch_next has priority
// over refill: a freed common-pool slot dispatches the waiter (in-flight
// goes up) rather than growing the group's reservation. Only once there
// are no queued waiters do subsequent releases refill the reservation.
TEST_CORO(ReservationPolicyTest, RefillOnlyFiresAfterDispatchNext) {
    cloud_io::reservation_policy<ss::manual_clock> policy{4};
    configure(policy, /*total_slots=*/4, /*target_reserved=*/{1, 0, 0});

    ss::abort_source as;

    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::producer_upload, as));
    policy.release(cloud_io::group_id::producer_upload);
    co_await tick(cloud_io::default_dwell_duration + 1s);
    EXPECT_TRUE(policy.try_admit(cloud_io::group_id::default_group));
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 0u);

    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(policy.try_admit(cloud_io::group_id::default_group));
    }

    auto pu_q = policy.admit(cloud_io::group_id::producer_upload, as);
    co_await ss::yield();
    EXPECT_EQ(policy.waiters(cloud_io::group_id::producer_upload), 1u);

    policy.release(cloud_io::group_id::default_group);
    co_await with_test_timeout(std::move(pu_q));
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::producer_upload), 1u);
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 0u);

    policy.release(cloud_io::group_id::producer_upload);
    policy.release(cloud_io::group_id::default_group);
    policy.release(cloud_io::group_id::default_group);
    policy.release(cloud_io::group_id::default_group);
}

// Two groups with reservations queue waiters at the same time; both
// are under target. dispatch_next picks them in FIFO order. Once
// both are dispatched, subsequent releases refill the under-target
// groups — ratio drives the choice between them.
TEST_CORO(ReservationPolicyTest, DispatchAndRefillAcrossUnderTargetGroups) {
    cloud_io::reservation_policy<ss::manual_clock> policy{6};
    configure(policy, /*total_slots=*/6, /*target_reserved=*/{2, 2, 0});

    ss::abort_source as;

    // Touch pu and cf so they stamp inactive_since, then age past
    // dwell so reclaim is eligible for both groups.
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::producer_upload, as));
    policy.release(cloud_io::group_id::producer_upload);
    co_await with_test_timeout(
      policy.admit(cloud_io::group_id::consumer_fetch, as));
    policy.release(cloud_io::group_id::consumer_fetch);

    co_await tick(cloud_io::default_dwell_duration + 1s);
    EXPECT_TRUE(policy.try_admit(cloud_io::group_id::default_group));
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 0u);
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::consumer_fetch), 0u);

    // Drain remaining shared with default_group.
    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(policy.try_admit(cloud_io::group_id::default_group));
    }
    EXPECT_EQ(policy.available_slots(), 0u);

    // Queue pu, then cf. Both have target=2, in_flight=0 → under target.
    auto pu_fut = policy.admit(cloud_io::group_id::producer_upload, as);
    auto cf_fut = policy.admit(cloud_io::group_id::consumer_fetch, as);
    co_await ss::yield();
    EXPECT_EQ(policy.waiters(cloud_io::group_id::producer_upload), 1u);
    EXPECT_EQ(policy.waiters(cloud_io::group_id::consumer_fetch), 1u);

    // dispatch_next picks pu first (older seq, both under target).
    policy.release(cloud_io::group_id::default_group);
    co_await with_test_timeout(std::move(pu_fut));
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::producer_upload), 1u);
    EXPECT_EQ(policy.waiters(cloud_io::group_id::consumer_fetch), 1u);

    // dispatch_next picks cf.
    policy.release(cloud_io::group_id::default_group);
    co_await with_test_timeout(std::move(cf_fut));
    EXPECT_EQ(policy.in_flight(cloud_io::group_id::consumer_fetch), 1u);

    // No more waiters. Refill takes over.
    // pu and cf both at reserved=0, ratio=0 — tied, iteration order picks pu.
    policy.release(cloud_io::group_id::default_group);
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::producer_upload), 1u);
    // pu=500, cf=0 — cf wins (lower ratio).
    policy.release(cloud_io::group_id::default_group);
    EXPECT_EQ(policy.current_reserved(cloud_io::group_id::consumer_fetch), 1u);

    policy.release(cloud_io::group_id::default_group);
    policy.release(cloud_io::group_id::default_group);
    policy.release(cloud_io::group_id::producer_upload);
    policy.release(cloud_io::group_id::consumer_fetch);
}

TEST_CORO(ReservationPolicyTest, GroupStateTransitionsThroughLifecycle) {
    cloud_io::reservation_policy<ss::manual_clock> policy{4};
    configure(policy, /*total_slots=*/4);
    ss::abort_source as;
    constexpr auto g = cloud_io::group_id::consumer_fetch;

    // Construction → idle.
    EXPECT_EQ(policy.state(g), cloud_io::group_state::idle);

    // Admit → active.
    co_await with_test_timeout(policy.admit(g, as));
    EXPECT_EQ(policy.state(g), cloud_io::group_state::active);

    // Release last in-flight → dwelling.
    policy.release(g);
    EXPECT_EQ(policy.state(g), cloud_io::group_state::dwelling);

    // Clock advances past dwell → idle (virtual transition, no trigger).
    ss::manual_clock::advance(cloud_io::default_dwell_duration + 1s);
    EXPECT_EQ(policy.state(g), cloud_io::group_state::idle);
}
