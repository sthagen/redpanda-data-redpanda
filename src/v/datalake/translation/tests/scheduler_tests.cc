/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/logger.h"
#include "datalake/translation/tests/scheduler_fixture.h"
#include "test_utils/randoms.h"

#include <seastar/core/sleep.hh>

using namespace datalake::translation::scheduling;

class many_concurrent_translators : public scheduler_fixture {
public:
    std::unique_ptr<scheduling_policy> make_scheduling_policy() override {
        return scheduling_policy::make_default(
          config::mock_binding(max_concurrent_translators), task_time_quota);
    }

protected:
    static constexpr size_t max_concurrent_translators = 30;
};

TEST_F_CORO(many_concurrent_translators, test_concurrent_add_remove) {
    std::vector<translator_id> added;
    auto add_one = [this, &added]() {
        auto translator = make_random_translator();
        auto ntp = translator->id();
        return _scheduler->add_translator(std::move(translator))
          .then([&added, ntp = std::move(ntp), this](bool success) {
              if (!success) {
                  return _scheduler->remove_translator(ntp);
              }
              added.push_back(ntp);
              return ss::now();
          })
          .then([]() { return ss::sleep(100ms); });
    };

    auto remove_one = [this, &added]() {
        if (added.empty()) {
            return ss::now();
        }
        auto idx = random_generators::get_int<size_t>(added.size() - 1);
        auto to_remove = added[idx];
        added.erase(added.begin() + idx);
        return ss::do_with(
          std::move(to_remove), [this](translator_id& to_remove) {
              return _scheduler->remove_translator(to_remove).then(
                []() { return ss::sleep(150ms); });
          });
    };

    auto end_time = clock::now() + 120s;
    auto is_done = [end_time] { return clock::now() >= end_time; };

    auto add_f = ss::do_until(
      [&] { return is_done(); }, [&]() { return add_one(); });
    auto remove_f = ss::do_until(
      [&] { return is_done(); }, [&]() { return remove_one(); });
    auto validate_f = ss::do_until(
      [&] { return is_done(); },
      [this] {
          auto running = _scheduler->running_translators();
          if (running > max_concurrent_translators) {
              return ss::make_exception_future(
                std::runtime_error(
                  ssx::sformat(
                    "concurrent translators count {} exceeded limit {}",
                    running,
                    max_concurrent_translators)));
          }
          auto allocated_memory
            = _scheduler->reservations()->allocated_memory();
          if (allocated_memory > total_memory) {
              return ss::make_exception_future(
                std::runtime_error(
                  ssx::sformat(
                    "Memory oversubscribed. allocated: {}, total: {}",
                    allocated_memory,
                    total_memory)));
          }
          return ss::sleep(10ms);
      });
    co_await ss::when_all_succeed(
      std::move(add_f), std::move(remove_f), std::move(validate_f));
}
