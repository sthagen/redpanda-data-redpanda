/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "base/vassert.h"
#include "cloud_io/admission_control_types.h"
#include "container/intrusive_list_helpers.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/util/optimized_optional.hh>

#include <chrono>
#include <cstdint>
#include <exception>
#include <optional>
#include <string_view>
#include <utility>

namespace cloud_io {

/// Duration of the `dwelling` state before a group ages out to `idle`.
inline constexpr std::chrono::seconds default_dwell_duration{5};

/// Lifecycle of one queued admit() call.
///   - `enqueued`: waiting in the group's queue.
///   - `dispatched`: granted a slot; future resolves with value.
///   - `canceled`: aborted; future resolves with exception.
enum class waiter_state : uint8_t {
    enqueued,
    dispatched,
    canceled,
};

constexpr std::string_view to_string_view(waiter_state s) {
    switch (s) {
    case waiter_state::enqueued:
        return "enqueued";
    case waiter_state::dispatched:
        return "dispatched";
    case waiter_state::canceled:
        return "canceled";
    }
    std::unreachable();
}

inline fmt::iterator format_to(waiter_state s, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(s));
}

/// Lifecycle of a reservation group.
///   - `active`: in-flight ops or queued waiters present.
///   - `dwelling`: recently active, within `default_dwell_duration`.
///   - `idle`: otherwise. Reclaim-eligible.
enum class group_state : uint8_t {
    idle,
    active,
    dwelling,
};

constexpr std::string_view to_string_view(group_state s) {
    switch (s) {
    case group_state::idle:
        return "idle";
    case group_state::active:
        return "active";
    case group_state::dwelling:
        return "dwelling";
    }
    std::unreachable();
}

inline fmt::iterator format_to(group_state s, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(s));
}

template<class Clock>
struct reservation_group_state;

/// One queued admit() call. Lifecycle (`waiter_state`):
///
///                       ┌── dispatch() ──► dispatched
///   enqueued ───────────┤
///                       └── cancel() ────► canceled
///
/// The ctor enqueues onto the group's waiter list and subscribes to
/// the caller's abort_source. The dtor calls cancel() as a backstop;
/// no-op if the waiter is already dispatched or canceled.
template<class Clock>
struct reservation_waiter {
    reservation_waiter(
      reservation_group_state<Clock>& gs, ss::abort_source& as) noexcept;
    ~reservation_waiter() noexcept;

    reservation_waiter(const reservation_waiter&) = delete;
    reservation_waiter& operator=(const reservation_waiter&) = delete;
    reservation_waiter(reservation_waiter&&) = delete;
    reservation_waiter& operator=(reservation_waiter&&) = delete;

    /// For the group's waiter list.
    intrusive_list_hook link;

    /// Insertion sequence captured from the per-shard counter.
    /// Used for FIFO ordering across groups in dispatch.
    uint64_t seq;

    /// Transition `enqueued` → `dispatched`; resolves the future with
    /// a value.
    void dispatch() noexcept;

    /// Transition `enqueued` → `canceled`; resolves the future with
    /// `abort_requested_exception`. No-op if already dispatched or
    /// canceled.
    void cancel() noexcept;

    ss::future<> fut() noexcept { return p.get_future(); }

    fmt::iterator format_to(fmt::iterator out) const {
        return fmt::format_to(
          out, "waiter{{seq={}, state={}}}", seq, to_string_view(state));
    }

private:
    ss::promise<> p;
    waiter_state state{waiter_state::enqueued};
    reservation_group_state<Clock>* _gs;
    ss::optimized_optional<ss::abort_source::subscription> _sub;

    /// Per-shard monotonic counter to enforce FIFO on waiters queued on
    /// different groups.
    static inline thread_local uint64_t _seq_counter{0};
};

/// Per-group admission state.
///
/// Invariant: the sum of `target_reserved` across all groups is bounded
/// by the policy's `capacity` (asserted at construction in
/// `reservation_policy::reservation_policy`).
///
/// The policy has two kinds of slot storage:
///
///   - A per-group reservation (this struct's `reserved_available`
///     count). Slots in a group's reservation can only be claimed by
///     that group; releases stay here unless the policy reclaims them.
///
///   - A single common pool of slots (`reservation_policy::_shared`)
///     that any group can claim from. Releases go back here unless
///     refill diverts them into a group's reservation.
///
/// Capacity moves between the two pools by two one-way policy actions:
///
///   - refill (common pool -> reservation): a slot heading back to
///     the common pool is diverted into an under-target group's
///     reservation, one at a time.
///
///   - reclaim (reservation -> common pool): a periodic sweep drains
///     `reserved_available` (not in-flight) from `idle` groups.
///
/// Lifecycle (`group_state`):
///
///                          ┌─────────── admit ───────────┐
///                          ▼                             │
///   ┌──────┐   admit   ┌────────┐  release/cancel   ┌────┴─────┐
///   │ idle ├──────────►│ active ├──────────────────►│ dwelling │
///   └──▲───┘           └────────┘                   └────┬─────┘
///      │            dwell expires (no trigger)           │
///      └─────────────────────────────────────────────────┘
///
/// A newly-constructed group is `idle`, so any seeded reservation
/// drains on the first reclaim sweep. `active` and `dwelling` groups
/// retain their reservation; refill grows it toward `target_reserved`
/// while the group is non-idle.
template<class Clock>
struct reservation_group_state {
    using time_point = typename Clock::time_point;

    explicit reservation_group_state(group_id id) noexcept
      : id(id) {}

    reservation_group_state(const reservation_group_state&) = delete;
    reservation_group_state& operator=(const reservation_group_state&) = delete;
    reservation_group_state(reservation_group_state&&) = delete;
    reservation_group_state& operator=(reservation_group_state&&) = delete;
    ~reservation_group_state() noexcept {
        vassert(
          waiters.empty(),
          "reservation_group_state destroyed with active waiters: {}",
          *this);
    }

    /// The group_id this state belongs to.
    const group_id id;

    /// The group's reservation: slots available for the owning group's
    /// exclusive use. Mutated via try_take_reserved_slot,
    /// grant_reserved_slot, set_target_reserved, and maybe_reclaim_idle.
    size_t reserved_available = 0;

    /// Configured cap on the group's reservation.
    size_t target_reserved = 0;

    /// Ops holding a slot from this group, across both the group's
    /// reservation and the common pool.
    size_t in_flight = 0;

    /// Of in_flight, how many came from the group's reservation.
    /// Counted into current_reserved().
    ///
    /// Invariants at admit/release boundaries:
    ///   reserved_in_flight <= in_flight
    ///   reserved_in_flight <= target_reserved
    size_t reserved_in_flight = 0;

    intrusive_list<reservation_waiter<Clock>, &reservation_waiter<Clock>::link>
      waiters;

    /// Lifetime counters surfaced by metrics & diagnostics.
    uint64_t admit_total = 0;
    uint64_t admit_immediate_total = 0;

    /// Lifetime count of waiters that aborted while queued (incl. the
    /// shutdown sweep in stop()). Bumped once per waiter in
    /// reservation_waiter::cancel().
    uint64_t canceled_total = 0;

    /// Timestamp of the most recent `active` → `dwelling` transition.
    /// nullopt if the group has never been `active`.
    std::optional<time_point> inactive_since;

    /// Current lifecycle state, derived from in_flight/waiters and
    /// inactive_since.
    group_state state() const noexcept {
        if (in_flight > 0 || !waiters.empty()) {
            return group_state::active;
        }
        if (
          inactive_since.has_value()
          && Clock::now() - *inactive_since < default_dwell_duration) {
            return group_state::dwelling;
        }
        return group_state::idle;
    }

    /// Criteria for refill:
    ///   - target_reserved floor configured
    ///   - current_reserved below target
    ///   - group is not idle (active or within dwell window)
    bool is_refill_eligible() const noexcept {
        return target_reserved > 0 && current_reserved() < target_reserved
               && !is_idle();
    }

    /// True when in-flight count is below `target_reserved`. Dispatch
    /// uses this to prefer under-target groups.
    bool has_reservation_headroom() const noexcept {
        return in_flight < target_reserved;
    }

    /// Slots in this group's reservation that are currently available
    /// (not in-flight).
    size_t available_slots() const noexcept { return reserved_available; }

    /// The reservation's runtime size: slots currently held by this
    /// group (available + in-flight).
    size_t current_reserved() const noexcept {
        return available_slots() + reserved_in_flight;
    }

    /// Currently-queued waiters. O(n); intrusive_list::size() is not
    /// constant-time, callers should not assume otherwise.
    size_t waiter_count() const noexcept { return waiters.size(); }

    /// True if the group has any queued waiters.
    bool has_waiters() const noexcept { return !waiters.empty(); }

    /// Score for picking a refill target among eligible groups: smaller
    /// is more under-target. Precondition: target_reserved > 0.
    size_t refill_priority_ratio() const noexcept {
        vassert(
          target_reserved > 0,
          "refill_priority_ratio: target_reserved is zero for {}",
          *this);
        return current_reserved() * 1000 / target_reserved;
    }

    /// Record that this admit took the fast path (no queue).
    void on_immediate_admit() noexcept { ++admit_immediate_total; }

    /// Record one slot admit on this group: Caller passes from_reserved per
    /// where the slot came from.
    void admit_one(bool from_reserved) noexcept {
        ++in_flight;
        if (from_reserved) {
            ++reserved_in_flight;
        }
        ++admit_total;
    }

    /// Record one slot release on this group. If the slot was from
    /// the group's own reservation (i.e. not the common pool), route
    /// it back there, either by dispatching a queued same-group waiter or
    /// incrementing reserved_available.
    ///
    /// Returns whether the released slot went to the reservation so the caller
    /// can decide whether to apply the policy-global dispatch strategy.
    [[nodiscard]] bool release_one() noexcept {
        const bool from_reserved = all_in_flight_reserved();
        vassert(in_flight > 0, "release_one: in_flight underflow on {}", *this);
        --in_flight;
        maybe_mark_inactive();
        if (from_reserved) {
            vassert(
              reserved_in_flight > 0,
              "release_one: reserved_in_flight underflow on {}",
              *this);
            --reserved_in_flight;
            if (!waiters.empty()) {
                dispatch_one(/*from_reserved=*/true);
            } else {
                ++reserved_available;
            }
        }
        return from_reserved;
    }

    /// Try to claim a slot from this group's reservation.
    [[nodiscard]] bool try_take_reserved_slot() noexcept {
        if (target_reserved == 0 || reserved_available == 0) {
            return false;
        }
        --reserved_available;
        return true;
    }

    /// Add a slot to this group's reservation. Called by refill.
    void grant_reserved_slot() noexcept { ++reserved_available; }

    /// Set the `target_reserved` cap for this group and reconcile the
    /// `reserved_available` count against the supplied common pool.
    /// Growing the target pulls slots from `shared`; shrinking returns
    /// them. Asserts that the shrink delta does not exceed
    /// `reserved_available`, which can happen if the group has more
    /// reserved slots in flight than the new target permits.
    void set_target_reserved(size_t value, size_t& shared) noexcept {
        const size_t cur = current_reserved();
        if (value > cur) {
            const size_t delta = value - cur;
            vassert(
              shared >= delta,
              "set_target_reserved({}, {}): would underflow shared pool "
              "(shared={}, delta={})",
              to_string_view(id),
              value,
              shared,
              delta);
            shared -= delta;
            reserved_available += delta;
        } else if (value < cur) {
            const size_t delta = cur - value;
            vassert(
              reserved_available >= delta,
              "set_target_reserved({}, {}): would underflow "
              "reserved_available (reserved_available={}, in_flight={}, "
              "delta={})",
              to_string_view(id),
              value,
              reserved_available,
              reserved_in_flight,
              delta);
            reserved_available -= delta;
            shared += delta;
        }
        target_reserved = value;
    }

    /// Cancel all queued waiters.
    void stop() noexcept {
        while (!waiters.empty()) {
            waiters.front().cancel();
        }
    }

    uint64_t front_seq() const noexcept {
        vassert(!waiters.empty(), "front_seq: queue empty for {}", *this);
        return waiters.front().seq;
    }

    /// Hand a slot to one queued waiter. Precondition: queue non-empty. Caller
    /// passes from_reserved per where the slot came from.
    void dispatch_one(bool from_reserved) noexcept {
        vassert(!waiters.empty(), "dispatch_one: queue empty for {}", *this);
        admit_one(from_reserved);
        waiters.front().dispatch();
    }

    /// If the group is idle, drain its reserved slots and return the
    /// count to the caller. Returns 0 if not idle.
    [[nodiscard]] size_t maybe_reclaim_idle() noexcept {
        if (is_idle()) {
            return std::exchange(reserved_available, size_t{0});
        }
        return 0;
    }

    fmt::iterator format_to(fmt::iterator out) const {
        return fmt::format_to(
          out,
          "{}{{state={}, in_flight={}[reserved_in_flight={}], "
          "target_reserved={}, current_reserved={}, waiter_count={}}}",
          to_string_view(id),
          to_string_view(state()),
          in_flight,
          reserved_in_flight,
          target_reserved,
          current_reserved(),
          waiter_count());
    }

private:
    friend struct reservation_waiter<Clock>;

    /// True when every in-flight slot is from the group's reservation
    /// (no common-pool slots in flight).
    bool all_in_flight_reserved() const noexcept {
        return in_flight > 0 && in_flight == reserved_in_flight;
    }

    bool is_active() const noexcept { return state() == group_state::active; }

    bool is_idle() const noexcept { return state() == group_state::idle; }

    void maybe_mark_inactive() noexcept {
        if (!is_active()) {
            inactive_since = Clock::now();
        }
    }
};

template<class Clock>
reservation_waiter<Clock>::reservation_waiter(
  reservation_group_state<Clock>& gs, ss::abort_source& as) noexcept
  : seq(_seq_counter++)
  , _gs(&gs)
  , _sub(
      as.subscribe([this](const std::optional<std::exception_ptr>&) noexcept {
          cancel();
      })) {
    _gs->waiters.push_back(*this);
    if (!_sub) {
        // Already aborted
        cancel();
    }
}

template<class Clock>
reservation_waiter<Clock>::~reservation_waiter() noexcept {
    cancel();
    vassert(
      state != waiter_state::enqueued,
      "~reservation_waiter: waiter left in enqueued state (seq={})",
      seq);
}

template<class Clock>
void reservation_waiter<Clock>::cancel() noexcept {
    if (link.is_linked()) {
        link.unlink();
        vassert(
          state == waiter_state::enqueued,
          "reservation_waiter::cancel: linked waiter in non-enqueued state");
        state = waiter_state::canceled;
        ++_gs->canceled_total;
        p.set_exception(
          std::make_exception_ptr(ss::abort_requested_exception{}));
        _gs->maybe_mark_inactive();
    }
}

template<class Clock>
void reservation_waiter<Clock>::dispatch() noexcept {
    vassert(
      link.is_linked(),
      "reservation_waiter::dispatch: waiter not linked (seq={})",
      seq);
    vassert(
      state == waiter_state::enqueued,
      "reservation_waiter::dispatch: expected enqueued (seq={})",
      seq);
    link.unlink();
    state = waiter_state::dispatched;
    p.set_value();
}

} // namespace cloud_io
