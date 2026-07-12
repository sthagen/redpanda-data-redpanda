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

#include "base/seastarx.h"
#include "cloud_io/admission_control_policy.h"
#include "cloud_io/admission_control_types.h"
#include "cloud_io/reservation_policy_types.h"
#include "metrics/metrics.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/timer.hh>

#include <cstdint>
#include <memory>
#include <optional>

namespace cloud_io {

/// \brief Reservation-based admission policy.
///
/// Bounds simultaneous in-flight cloud_io ops to the configured capacity.
/// Slots live in one of two places: a group's private reservation (held
/// by `reserved_available` inside `reservation_group_state`) or the
/// common pool (`_shared`, below). Reservations ebb and flow with
/// demand: refill builds a group's reservation back up toward
/// `target_reserved` while the group is active; the policy reclaims
/// idle reservations to the common pool after the dwell window. See
/// `reservation_group_state` for the mechanism details.
template<class Clock = ss::lowres_clock>
class reservation_policy final : public admission_control_policy {
public:
    using time_point = typename Clock::time_point;

    explicit reservation_policy(
      size_t capacity, reservation_policy_config = {});
    reservation_policy(const reservation_policy&) = delete;
    reservation_policy& operator=(const reservation_policy&) = delete;
    reservation_policy(reservation_policy&&) = delete;
    reservation_policy& operator=(reservation_policy&&) = delete;
    ~reservation_policy() noexcept override = default;

    ss::future<> admit(group_id g, ss::abort_source& as) override;
    [[nodiscard]] bool try_admit(group_id g) noexcept override;
    void release(group_id g) noexcept override;
    ss::future<> stop() override;

    size_t in_flight(group_id) const noexcept override;
    size_t waiters(group_id) const noexcept override;
    size_t available_slots() const noexcept override;
    size_t total_capacity() const noexcept override;

    // ---- reservation_policy-specific public API ----

    /// Sets the target_reserved for a group. Construction installs
    /// initial targets via reservation_policy_config; tests may also
    /// mutate targets between scenarios. Not yet safe for live
    /// reconfiguration with traffic in flight: shrinking the target
    /// below current reserved usage will trip a vassert because
    /// reserved slots may be held by in-flight ops. A future
    /// runtime-reconfig path will need to defer the shrink until those
    /// slots return.
    void set_target_reserved(group_id, size_t);

    /// Current target_reserved floor for a group.
    size_t target_reserved(group_id) const noexcept;

    /// Runtime reservation size for a group, derived from the group's
    /// reservation semaphore and reserved_in_flight count. See
    /// reservation_group_state::current_reserved for semantics.
    size_t current_reserved(group_id) const noexcept;

    // ---- test-only public API ----

    /// Runtime capacity mutator. Production cluster config can't change
    /// capacity at runtime. Exists so tests can grow or shrink the
    /// common pool without rebuilding the policy.
    void set_total_slots(size_t);

    // ---- observability accessors ----

    /// Current FSM state for a group. See `group_state` in
    /// reservation_policy_types.h.
    group_state state(group_id) const noexcept;

    /// Lifetime admit() count for a group.
    uint64_t admit_total(group_id) const noexcept;

    /// Lifetime fast-path-admit count for a group.
    uint64_t admit_immediate_total(group_id) const noexcept;

    /// Lifetime count of waiters that aborted while queued for a group.
    uint64_t canceled_total(group_id) const noexcept;

    /// Total queued waiters across all groups.
    size_t total_waiters() const noexcept;

    /// Lifetime count of canceled waiters across all groups.
    uint64_t total_canceled() const noexcept;

private:
    /// Dispatch the next queued waiter on behalf of a release on
    /// `releasing_group`. Three-tier choice:
    ///   0. Local recycle (via `release_one`): if the released slot
    ///      was from the releasing_group's own reservation, route
    ///      back to a same-group waiter or bank in reserved_available.
    ///   1. Under-target preference: among groups with in_flight <
    ///      target_reserved AND queued waiters, the oldest seq wins.
    ///   2. FIFO fallback: if no group is under target, the global
    ///      oldest-seq across all groups with waiters wins.
    /// Returns false only when no local recycle happened AND no group
    /// has waiters. The caller then tries refill or returns the slot
    /// to the common pool.
    bool dispatch_next(group_id releasing_group) noexcept;

    /// Walk groups and reclaim the reservation of any inactive group
    /// whose dwell window has elapsed. Reclaimed slots return to the
    /// common pool. O(N) where N is num_group_ids.
    void reclaim_idle_reservations();

    /// Pick the group that should receive a common-pool slot as a refill.
    /// Eligibility: not idle AND current_reserved < target_reserved.
    /// Among eligible groups, the one most below its target wins.
    /// Returns nullopt if no group is eligible; the slot then returns
    /// to the common pool.
    std::optional<group_id> pick_refill_candidate() noexcept;

    [[nodiscard]] bool try_take_common_slot() noexcept;

    void put_common_slots(size_t n) noexcept;

    void setup_metrics();

    void setup_public_metrics();

    size_t _current_total_capacity{0};
    /// The common pool of slots. Any group can claim from it; releases
    /// go back here unless refill diverts them into a group's
    /// reservation.
    size_t _shared{0};

    using GroupState = reservation_group_state<Clock>;

    /// Per-group state including the group's reservation. See
    /// reservation_group_state for the layout and invariants.
    per_group<GroupState> _groups;

    ss::gate _admit_gate;

    static constexpr
      typename Clock::duration reclaim_interval = std::chrono::seconds{1};
    ss::timer<Clock> _reclaim_timer;

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

extern template class reservation_policy<ss::lowres_clock>;
extern template class reservation_policy<ss::manual_clock>;

} // namespace cloud_io
