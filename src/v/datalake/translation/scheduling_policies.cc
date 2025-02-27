/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/scheduling_policies.h"

#include "datalake/logger.h"
#include "random/generators.h"

#include <seastar/core/sleep.hh>

#include <queue>

using namespace std::chrono_literals;

static constexpr auto polling_interval = 1s;

namespace datalake::translation::scheduling {
simple_fcfs_scheduling_policy::simple_fcfs_scheduling_policy(
  size_t max_concurrent_translators, clock::duration translation_time_quota)
  : _max_concurrent_translations(max_concurrent_translators)
  , _translation_time_quota(translation_time_quota) {
    vlog(
      datalake_log.info,
      "created simple_fcfs_scheduling_policy policy with {} translators "
      "and {} time quota",
      max_concurrent_translators,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        translation_time_quota));
}

ss::future<> simple_fcfs_scheduling_policy::schedule_one_translation(
  executor& executor, const reservations_tracker& mem_tracker) {
    // check the # of running translators
    while (!executor.as.abort_requested() && !executor.waiting.empty()
           && !mem_tracker.memory_exhausted()
           && executor.running.size() >= _max_concurrent_translations) {
        co_await ss::sleep_abortable(polling_interval, executor.as);
    }
    if (executor.as.abort_requested() || mem_tracker.memory_exhausted()) {
        co_return;
    }
    // pick the first queued translator.
    if (executor.waiting.empty()) {
        co_return;
    }
    executor.start_translation(
      *executor.waiting.begin(), _translation_time_quota);
}

ss::future<> simple_fcfs_scheduling_policy::on_resource_exhaustion(
  executor& executor, const reservations_tracker& mem_tracker) {
    while (mem_tracker.memory_exhausted() && !executor.as.abort_requested()) {
        // pick the earliest scheduled translator and force a flush.
        if (!executor.running.empty()) {
            executor.stop_translation(*executor.running.begin());
        }
        co_await ss::sleep_abortable(5s, executor.as);
    }
}

fair_scheduling_policy::fair_scheduling_policy(
  size_t max_concurrent_translators, clock::duration translation_time_quota)
  : _max_concurrent_translations(max_concurrent_translators)
  , _translation_time_quota(translation_time_quota) {
    vlog(
      datalake_log.info,
      "created fair_scheduling_policy policy with {} translators "
      "and {} time quota",
      max_concurrent_translators,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        translation_time_quota));
    initialize_group_shares();
}

std::ostream&
operator<<(std::ostream& os, fair_scheduling_policy::translator_group group) {
    switch (group) {
    case fair_scheduling_policy::translator_group::other:
        return os << "translator_group::other";
    case fair_scheduling_policy::translator_group::unfulfilled_quota:
        return os << "translator_group::unfulfilled_quota";
    case fair_scheduling_policy::translator_group::about_to_expire:
        return os << "translator_group::about_to_expire";
    case fair_scheduling_policy::translator_group::expired:
        return os << "translator_group::expired";
    }
}

fair_scheduling_policy::group_shares_t fair_scheduling_policy::_group_to_shares;
fair_scheduling_policy::group_intervals_t
  fair_scheduling_policy::_group_intervals;

void fair_scheduling_policy::initialize_group_shares() {
    _group_to_shares = {
      {other, fair_scheduling_policy::default_other_shares},
      {unfulfilled_quota,
       fair_scheduling_policy::default_unfulfilled_group_shares},
      {about_to_expire,
       fair_scheduling_policy::default_about_to_expire_group_shares},
      {expired, fair_scheduling_policy::default_expired_group_shares}};

    const double total_shares = std::accumulate(
      std::begin(_group_to_shares),
      std::end(_group_to_shares),
      0,
      [](const double so_far, const auto& p) { return so_far + p.second; });

    vassert(
      total_shares > 0,
      "invalid share assignment for translation groups, total shares should be "
      "> 0");

    double previous_end = 0.0;
    for (const auto& [group, shares] : _group_to_shares) {
        double end = previous_end + shares / total_shares;
        _group_intervals[group] = std::make_pair(previous_end, end);
        previous_end = end;
    }
}

fair_scheduling_policy::translator_group
fair_scheduling_policy::choose_random_translator_group() const {
    auto random = random_generators::get_real<double>(0, 1);
    auto it = std::ranges::find_if(
      _group_intervals, [&random](const auto& entry) {
          auto interval_begin = entry.second.first;
          auto interval_end = entry.second.second;
          return interval_begin <= random && random <= interval_end;
      });
    vassert(
      it != _group_intervals.end(),
      "Invalid share assignment for translation groups, no group found for {}",
      random);
    return it->first;
}

ss::future<> fair_scheduling_policy::schedule_one_translation(
  executor& executor, const reservations_tracker& mem_tracker) {
    // Wait until an empty slot frees up.
    while (!executor.as.abort_requested() && !executor.waiting.empty()
           && !mem_tracker.memory_exhausted()
           && executor.running.size() >= _max_concurrent_translations) {
        co_await ss::sleep_abortable(polling_interval, executor.as);
    }

    if (executor.as.abort_requested() || mem_tracker.memory_exhausted()) {
        co_return;
    }

    auto prioritized_group = choose_random_translator_group();
    vlog(
      datalake_log.trace,
      "prioritizing translator group of type: {}",
      prioritized_group);

    struct entry {
        translator_id id;
        translator_group group;
        // weight within the group, used to sort entries within
        // a single group.
        long weight{0};
    };
    // A comparator to sort all the {@link entry} entries based on two
    // dimensions
    // 1. group - if the group is prioritized, it is bubbled to the top relative
    //  to other groups (since we want to pick the prioritized group among all
    //  the available translators)
    // 2. weight - 2nd dimension of sorting among all translators within a group

    // This scheme allows us to always pick a translator if a prioritized group
    // is unavailable. In such a case pick a group with highest shares (and
    // weight).
    auto max_heap_cmp = [&](const entry& lhs, const entry& rhs) {
        auto both_prioritized = lhs.group == prioritized_group
                                && rhs.group == prioritized_group;
        if (both_prioritized) {
            return lhs.weight < rhs.weight;
        }
        // Either one of them is prioritized
        if (lhs.group == prioritized_group || rhs.group == prioritized_group) {
            return rhs.group == prioritized_group;
        }
        // neither of them are prioritized, just sort based on
        // shares if they are different, else weight within the same share
        // group.
        auto lhs_shares = _group_to_shares[lhs.group];
        auto rhs_shares = _group_to_shares[rhs.group];
        return lhs_shares == rhs_shares ? lhs.weight < rhs.weight
                                        : lhs_shares < rhs_shares;
    };

    // Make a snapshot of state to be scheduled without any scheduling points.
    // The expectation is that the waiting queue is small enough to not
    // cause any reactor stalls.

    // This loop classifies each translator into a group {@link
    // translator_group} depending on the current status of the group. A
    // snapshot of all the classified translators is then sorted using the
    // comparator prioritizing the group that is randomly picked above..

    chunked_vector<entry> candidates;
    for (const auto& translator : executor.waiting) {
        auto status = translator.status();
        auto duration_to_expire = status.next_checkpoint_deadline
                                  - clock::now();
        auto& id = translator.translator_ptr()->id();
        if (duration_to_expire < clock::duration{0}) {
            // Translator with the largest expiration time (most expired) is
            // given the highest weight.
            candidates.push_back(
              {.id = id,
               .group = translator_group::expired,
               .weight = -duration_to_expire.count()});
        } else if (
          duration_to_expire < about_to_expire_window * status.target_lag) {
            // Translator with the nearest expiry time is prioritized higher.
            candidates.push_back(
              {.id = id,
               .group = translator_group::about_to_expire,
               .weight = -duration_to_expire.count()});
        } else {
            auto total_time = clock::now() - translator.start_time();
            auto total_running_time = translator.total_running_time();
            if (total_running_time < minimum_allotment_coeff * total_time) {
                // within all unfulfilled quota, we want to order order them
                // by least alloted time first .
                candidates.push_back(
                  {.id = id,
                   .group = translator_group::unfulfilled_quota,
                   .weight = -total_running_time.count()});
            } else {
                // todo: perhaps we can order by pending_bytes_to_translate
                candidates.push_back(
                  {.id = id,
                   .group = translator_group::other,
                   .weight = random_generators::get_int<long>()});
            }
        }
        const auto& back = candidates.back();
        vlog(
          datalake_log.trace,
          "scheduling candidate: {},  group: {}, weight: {}",
          translator,
          back.group,
          back.weight);
    }

    co_await ss::maybe_yield();

    // Reactor friendly sort using the comparator
    std::priority_queue<entry, chunked_vector<entry>, decltype(max_heap_cmp)>
      prioritized(max_heap_cmp);
    for (auto& entry : candidates) {
        executor.as.check();
        auto it = executor.translators.find(entry.id);
        if (
          it == executor.translators.end()
          || !it->second._waiting_hook.is_linked()) {
            continue;
        }
        prioritized.push(entry);
        co_await ss::maybe_yield();
    }

    executor.as.check();

    while (!prioritized.empty()) {
        auto& entry = prioritized.top();
        auto it = executor.translators.find(entry.id);
        if (
          it != executor.translators.end()
          && it->second._waiting_hook.is_linked()) {
            vlog(
              datalake_log.trace,
              "[{}] chosen translator to run, group: {}, weight: {}",
              it->second,
              entry.group,
              entry.weight);
            executor.start_translation(it->second, _translation_time_quota);
            co_return;
        }
        prioritized.pop();
        co_await ss::maybe_yield();
    }
}

ss::future<> fair_scheduling_policy::on_resource_exhaustion(
  executor& executor, const reservations_tracker& mem_tracker) {
    if (!mem_tracker.memory_exhausted() || executor.running.empty()) {
        co_return;
    }
    // stop the translator with highest memory usage first.
    // note: the size of this list is super small (low single
    // digits)
    executor.running.sort(
      [](const translator_executable& a, const translator_executable& b) {
          return a.status().memory_bytes_reserved
                 > b.status().memory_bytes_reserved;
      });

    auto num_running = executor.running.size();
    // pick the earliest scheduled translator and force a flush.
    vlog(
      datalake_log.debug,
      "[{}] stopping translator due to memory exhaustion",
      *executor.running.begin());
    executor.stop_translation(*executor.running.begin());

    while (mem_tracker.memory_exhausted() && !executor.as.abort_requested()
           && executor.running.size() == num_running) {
        co_await ss::sleep_abortable(polling_interval, executor.as);
    }
}

} // namespace datalake::translation::scheduling
