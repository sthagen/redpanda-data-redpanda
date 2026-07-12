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
#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/util/log.hh>

#include <fmt/ranges.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <ranges>
#include <utility>
#include <vector>

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
  : admission_control_policy(capacity)
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

    setup_metrics();
    setup_public_metrics();

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
    _metrics.clear();
    _public_metrics.clear();
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
uint64_t reservation_policy<Clock>::canceled_total(group_id g) const noexcept {
    return _groups[g].canceled_total;
}

template<class Clock>
size_t reservation_policy<Clock>::total_waiters() const noexcept {
    return std::ranges::fold_left(
      _groups, size_t{0}, [](size_t acc, const auto& gs) {
          return acc + gs.waiter_count();
      });
}

template<class Clock>
uint64_t reservation_policy<Clock>::total_canceled() const noexcept {
    return std::ranges::fold_left(
      _groups, uint64_t{0}, [](uint64_t acc, const auto& gs) {
          return acc + gs.canceled_total;
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

template<class Clock>
void reservation_policy<Clock>::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    const auto group_name = prometheus_sanitize::metrics_name(
      "cloud_io_admission_control");
    constexpr auto group_label_key = "group_id";

    _metrics.add_group(
      group_name,
      {
        sm::make_gauge(
          "available_slots",
          [this] { return available_slots(); },
          sm::description(
            "Total slots currently available (shared + all reserved).")),
        sm::make_gauge(
          "total_capacity",
          [this] { return total_capacity(); },
          sm::description("Configured total slot capacity.")),
        sm::make_gauge(
          "total_waiters",
          [this] { return total_waiters(); },
          sm::description("Total fibers queued across all groups.")),
        sm::make_counter(
          "total_waiters_canceled",
          [this] { return total_canceled(); },
          sm::description(
            "Total waiters that aborted while queued across all groups.")),
      });

    for (auto g : all_group_ids) {
        const std::vector<sm::label_instance> labels{
          sm::label(group_label_key)(ssx::sformat("{}", g))};

        _metrics.add_group(
          group_name,
          {
            sm::make_gauge(
              "in_flight",
              [this, g] { return in_flight(g); },
              sm::description("Concurrent ops currently holding a slot."),
              labels),
            sm::make_gauge(
              "waiters",
              [this, g] { return waiters(g); },
              sm::description("Fibers queued on this group."),
              labels),
            sm::make_counter(
              "admit_total",
              [this, g] { return admit_total(g); },
              sm::description("Total admit() calls completed for this group."),
              labels),
            sm::make_counter(
              "admit_immediate_total",
              [this, g] { return admit_immediate_total(g); },
              sm::description(
                "admit() calls that took the fast path (no queue)."),
              labels),
            sm::make_counter(
              "canceled_total",
              [this, g] { return canceled_total(g); },
              sm::description(
                "Waiters that aborted while queued on this group."),
              labels),
            sm::make_gauge(
              "current_reserved",
              [this, g] { return current_reserved(g); },
              sm::description(
                "Runtime reservation size. Starts at target_reserved; "
                "reclaimed when idle past dwell; rebuilt via refill."),
              labels),
          });
    }
}

template<class Clock>
void reservation_policy<Clock>::setup_public_metrics() {
    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    const auto group_name = prometheus_sanitize::metrics_name(
      "cloud_io_admission_control");
    constexpr auto group_label_key = "group_id";
    const auto aggregate_labels = std::vector<sm::label>{sm::shard_label};

    _public_metrics.add_group(
      group_name,
      {
        sm::make_gauge(
          "available_slots",
          [this] { return available_slots(); },
          sm::description(
            "Total slots currently available (shared + all reserved)."))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "total_capacity",
          [this] { return total_capacity(); },
          sm::description("Configured total slot capacity."))
          .aggregate(aggregate_labels),
      });

    for (auto g : all_group_ids) {
        const std::vector<sm::label_instance> labels{
          sm::label(group_label_key)(ssx::sformat("{}", g))};

        _public_metrics.add_group(
          group_name,
          {
            sm::make_gauge(
              "in_flight",
              [this, g] { return in_flight(g); },
              sm::description("Concurrent ops currently holding a slot."),
              labels)
              .aggregate(aggregate_labels),
            sm::make_gauge(
              "waiters",
              [this, g] { return waiters(g); },
              sm::description("Fibers queued on this group."),
              labels)
              .aggregate(aggregate_labels),
          });
    }
}

template class reservation_policy<ss::lowres_clock>;
template class reservation_policy<ss::manual_clock>;

} // namespace cloud_io
