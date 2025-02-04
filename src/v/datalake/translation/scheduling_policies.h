/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "datalake/translation/scheduling.h"

namespace datalake::translation::scheduling {

/**
 * A very basic scheduling policy that schedules on a fcfs basis, used for
 * testing.
 */
class simple_fcfs_scheduling_policy : public scheduling_policy {
public:
    explicit simple_fcfs_scheduling_policy(
      size_t max_concurrent_translators,
      clock::duration translation_time_quota);

    ss::future<> schedule_one_translation(
      executor& executor, const reservations_tracker& mem_tracker) override;

    ss::future<> on_resource_exhaustion(
      executor& executor, const reservations_tracker& mem_tracker) override;

private:
    size_t _max_concurrent_translations;
    clock::duration _translation_time_quota;
};

} // namespace datalake::translation::scheduling
