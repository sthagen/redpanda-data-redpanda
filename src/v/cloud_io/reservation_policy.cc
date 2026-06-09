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

#include "base/vassert.h"
#include "base/vlog.h"
#include "cloud_io/logger.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/util/log.hh>

#include <fmt/ranges.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <ranges>
#include <utility>

namespace cloud_io {

namespace {

template<class Clock, size_t... Is>
per_group<reservation_group_state<Clock>>
make_group_states(std::index_sequence<Is...>) {
    return {{reservation_group_state<Clock>{static_cast<group_id>(Is)}...}};
}

} // namespace

template<class Clock>
reservation_policy<Clock>::reservation_policy(
  size_t capacity, reservation_policy_config cfg)
  : scheduler_policy(capacity)
  , _current_total_capacity(capacity)
  , _shared(capacity)
  , _groups(make_group_states<Clock>(std::make_index_sequence<num_group_ids>{}))
  , _reclaim_timer([this]() noexcept {
      reclaim_idle_reservations();
      _reclaim_timer.arm(reclaim_interval);
  }) {
    const size_t target_sum = std::ranges::fold_left(
      cfg.target_reserved, size_t{0}, std::plus{});
    vassert(
      target_sum <= capacity,
      "reservation_policy: target_reserved sum ({}) exceeds capacity ({})",
      target_sum,
      capacity);

    for (const auto g : all_group_ids) {
        set_target_reserved(g, cfg.target_reserved[g]);
    }

    _reclaim_timer.arm(reclaim_interval);

    vlog(
      log.info,
      "reservation_policy initialized: capacity={} dwell={}s "
      "target_reserved={}",
      _current_total_capacity,
      default_dwell_duration.count(),
      cfg.target_reserved.data);
}

template<class Clock>
ss::future<> reservation_policy<Clock>::stop() {
    _reclaim_timer.cancel();
    for (auto& gs : _groups) {
        gs.stop();
    }
    co_await _admit_gate.close();
}

template<class Clock>
size_t reservation_policy<Clock>::in_flight(group_id g) const noexcept {
    return _groups[g].in_flight;
}

template<class Clock>
size_t reservation_policy<Clock>::waiters(group_id g) const noexcept {
    return _groups[g].waiter_count();
}

template<class Clock>
size_t reservation_policy<Clock>::available_slots() const noexcept {
    return std::ranges::fold_left(
      _groups, _shared, [](size_t acc, const auto& gs) {
          return acc + gs.available_slots();
      });
}

template<class Clock>
size_t reservation_policy<Clock>::total_capacity() const noexcept {
    return _current_total_capacity;
}

template<class Clock>
ss::future<>
reservation_policy<Clock>::admit(group_id g, ss::abort_source& as) {
    // Fast path.
    if (try_admit(g)) {
        co_return;
    }

    // Slow path.
    auto holder = _admit_gate.hold();
    auto w = std::make_unique<reservation_waiter<Clock>>(_groups[g], as);
    co_await w->fut();
    co_return;
}

template<class Clock>
bool reservation_policy<Clock>::try_admit(group_id g) noexcept {
    auto& gs = _groups[g];

    if (
      bool from_reserved = gs.try_take_reserved_slot();
      from_reserved || try_take_common_slot()) {
        gs.admit_one(from_reserved);
        gs.on_immediate_admit();
        return true;
    }

    return false;
}

template<class Clock>
void reservation_policy<Clock>::release(group_id g) noexcept {
    if (dispatch_next(g)) {
        return;
    }
    if (const auto target = pick_refill_candidate(); target.has_value()) {
        _groups[*target].grant_reserved_slot();
    } else {
        put_common_slots(1);
    }
}

template<class Clock>
bool reservation_policy<Clock>::dispatch_next(
  group_id releasing_group) noexcept {
    if (_groups[releasing_group].release_one()) {
        return true;
    }
    auto has_waiters = _groups | std::views::filter(&GroupState::has_waiters);

    auto under_target = has_waiters
                        | std::views::filter(
                          &GroupState::has_reservation_headroom);

    auto pick_oldest = [](auto&& range) -> std::optional<group_id> {
        auto it = std::ranges::min_element(range, {}, &GroupState::front_seq);
        if (it == std::ranges::end(range)) {
            return std::nullopt;
        }
        return it->id;
    };

    auto pick = pick_oldest(under_target);
    if (!pick.has_value()) {
        pick = pick_oldest(has_waiters);
    }
    if (!pick.has_value()) {
        return false;
    }

    auto& gs = _groups[*pick];
    gs.dispatch_one(/*from_reserved=*/false);

    thread_local static ss::logger::rate_limit dispatch_log_rate{
      std::chrono::seconds(60)};
    log.log(
      ss::log_level::debug,
      dispatch_log_rate,
      "reservation_policy: dispatch picked={} | {}",
      to_string_view(gs.id),
      fmt::join(_groups, " "));

    return true;
}

template<class Clock>
void reservation_policy<Clock>::set_total_slots(size_t desired) {
    if (desired == _current_total_capacity) {
        return;
    }
    if (desired > _current_total_capacity) {
        _shared += desired - _current_total_capacity;
    } else {
        const size_t delta = _current_total_capacity - desired;
        vassert(
          _shared >= delta,
          "set_total_slots({}): would underflow _shared (current={}, "
          "delta={})",
          desired,
          _shared,
          delta);
        _shared -= delta;
    }
    vlog(
      log.info,
      "cloud_io reservation_policy total slots: {} -> {}",
      _current_total_capacity,
      desired);
    _current_total_capacity = desired;
}

template<class Clock>
void reservation_policy<Clock>::set_target_reserved(group_id g, size_t value) {
    _groups[g].set_target_reserved(value, _shared);
}

template<class Clock>
size_t reservation_policy<Clock>::target_reserved(group_id g) const noexcept {
    return _groups[g].target_reserved;
}

template<class Clock>
size_t reservation_policy<Clock>::current_reserved(group_id g) const noexcept {
    return _groups[g].current_reserved();
}

template<class Clock>
group_state reservation_policy<Clock>::state(group_id g) const noexcept {
    return _groups[g].state();
}

template<class Clock>
uint64_t reservation_policy<Clock>::admit_total(group_id g) const noexcept {
    return _groups[g].admit_total;
}

template<class Clock>
uint64_t
reservation_policy<Clock>::admit_immediate_total(group_id g) const noexcept {
    return _groups[g].admit_immediate_total;
}

template<class Clock>
size_t reservation_policy<Clock>::total_waiters() const noexcept {
    return std::ranges::fold_left(
      _groups, size_t{0}, [](size_t acc, const auto& gs) {
          return acc + gs.waiter_count();
      });
}

template<class Clock>
void reservation_policy<Clock>::reclaim_idle_reservations() {
    for (auto& gs : _groups) {
        put_common_slots(gs.maybe_reclaim_idle());
    }
}

template<class Clock>
std::optional<group_id>
reservation_policy<Clock>::pick_refill_candidate() noexcept {
    auto eligible = _groups
                    | std::views::filter(&GroupState::is_refill_eligible);
    auto it = std::ranges::min_element(
      eligible, {}, &GroupState::refill_priority_ratio);
    if (it == std::ranges::end(eligible)) {
        return std::nullopt;
    }
    return it->id;
}

template<class Clock>
bool reservation_policy<Clock>::try_take_common_slot() noexcept {
    if (_shared == 0) {
        return false;
    }
    --_shared;
    return true;
}

template<class Clock>
void reservation_policy<Clock>::put_common_slots(size_t n) noexcept {
    _shared += n;
}

template class reservation_policy<ss::lowres_clock>;
template class reservation_policy<ss::manual_clock>;

} // namespace cloud_io
