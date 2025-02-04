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

} // namespace datalake::translation::scheduling
