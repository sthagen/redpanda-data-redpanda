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
#include "datalake/translation/scheduling_policies.h"
#include "ssx/future-util.h"
#include "utils/to_string.h"

namespace datalake::translation::scheduling {

using namespace std::literals::chrono_literals;

class default_reservations_tracker : public reservations_tracker {
public:
    explicit default_reservations_tracker(
      size_t total_memory,
      size_t block_size,
      scheduling_notifications& notifier)
      : _total_memory((total_memory / block_size) * block_size)
      , _available_memory{_total_memory, "dl/translation/memory"}
      , _reservation_block_size(block_size)
      , _notifier(notifier) {
        auto blocks = _total_memory / block_size;
        vassert(
          blocks > 0,
          "Atleast one block of memory is needed for translation to make "
          "progress, available memory: {}, block_size: {}",
          total_memory,
          block_size);
        vlog(
          datalake_log.info,
          "starting reservation tracker with available memory: {} bytes, block "
          "size: {} bytes",
          _total_memory,
          _reservation_block_size);
    }

    bool memory_exhausted() const override {
        auto units = _available_memory.available_units();
        vassert(
          units >= 0,
          "Invalid memory reservation state, reservations cannot exceed memory "
          "limit, current: {}",
          units);
        return static_cast<size_t>(units) < _reservation_block_size;
    }

    ss::future<reservation> reserve_memory(ss::abort_source& as) override {
        auto nr = _reservation_block_size;
        // fast path
        auto opt_units = ss::try_get_units(_available_memory, nr);
        if (opt_units) {
            co_return std::move(opt_units.value());
        }
        _notifier.notify_memory_exhausted();
        co_return co_await ss::get_units(_available_memory, nr, as);
    }

    size_t allocated_memory() const override {
        return _total_memory - _available_memory.available_units();
    }

private:
    const size_t _total_memory;
    // note: the semaphore should be alive until all the reserved units are
    // deposited back.
    ssx::semaphore _available_memory;
    const size_t _reservation_block_size;
    scheduling_notifications& _notifier;
};

std::unique_ptr<reservations_tracker> make_default(
  size_t total_memory,
  size_t memory_block_size,
  scheduling_notifications& notifier) {
    return std::make_unique<default_reservations_tracker>(
      total_memory, memory_block_size, notifier);
}

std::ostream& operator<<(std::ostream& os, const translation_status& status) {
    fmt::print(
      os,
      "{{target_lag: {}, next_checkpoint: {}, memory_reserved: {}}}",
      std::chrono::duration_cast<std::chrono::milliseconds>(status.target_lag),
      std::chrono::duration_cast<std::chrono::milliseconds>(
        status.next_checkpoint_deadline - clock::now()),
      status.memory_bytes_reserved);
    return os;
}

std::ostream& operator<<(std::ostream& os, const translator& t) {
    fmt::print(os, "{{id: {}, status: {}}}", t.id(), t.status());
    return os;
}

translator_executable::translator_executable(
  translator_executable&& other) noexcept {
    if (this != &other) {
        *this = std::move(other);
    }
}

translator_executable&
translator_executable::operator=(translator_executable&& other) noexcept {
    _waiting_hook.swap_nodes(other._waiting_hook);
    _running_hook.swap_nodes(other._running_hook);
    _translator = std::move(other._translator);
    _start_time = other._start_time;
    _total_waiting_time = other._total_waiting_time;
    _total_running_time = other._total_running_time;
    _translations_scheduled = other._translations_scheduled;
    _current_wait_begin_time = other._current_wait_begin_time;
    _current_running_begin_time = other._current_running_begin_time;
    _stop_in_progress = other._stop_in_progress;
    return *this;
}

translation_status translator_executable::status() const {
    return _translator->status();
}

std::ostream& operator<<(std::ostream& os, const translator_executable& state) {
    std::optional<std::chrono::milliseconds> current_wait_time;
    if (state._current_wait_begin_time) {
        current_wait_time
          = std::chrono::duration_cast<std::chrono::milliseconds>(
            clock::now() - state._current_wait_begin_time.value());
    }
    std::optional<std::chrono::milliseconds> current_running_time;
    if (state._current_running_begin_time) {
        current_running_time
          = std::chrono::duration_cast<std::chrono::milliseconds>(
            clock::now() - state._current_running_begin_time.value());
    }
    fmt::print(
      os,
      "{{translator: {}, time_since_start: {}, current_wait_time: {}, "
      "total_wait_time: {}, current_running_time: {}, total_running_time: {},  "
      "translations_scheduled: {}, stop_in_progress: {}, "
      "running: {}, waiting: {}}}",
      *state._translator,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        clock::now() - state._start_time),
      current_wait_time,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        state.total_wait_time()),
      current_running_time,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        state.total_running_time()),
      state._translations_scheduled,
      state._stop_in_progress,
      state._running_hook.is_linked(),
      state._waiting_hook.is_linked());
    return os;
}

clock::duration translator_executable::total_running_time() const {
    auto result = _total_running_time;
    if (_current_running_begin_time) {
        result += (clock::now() - _current_running_begin_time.value());
    }
    return result;
}

clock::duration translator_executable::total_wait_time() const {
    auto result = _total_waiting_time;
    if (_current_wait_begin_time) {
        result += (clock::now() - _current_wait_begin_time.value());
    }
    return result;
}

void translator_executable::mark_waiting() {
    vassert(
      !_running_hook.is_linked(),
      "Unexpected state transition to waiting: {}",
      *this);
    // force unlink if it is already waiting.
    _waiting_hook.unlink();
    _current_wait_begin_time = clock::now();
}

void translator_executable::mark_running() {
    vassert(
      _waiting_hook.is_linked() && !_running_hook.is_linked(),
      "Unexpected state translation to running: {}",
      *this);
    _waiting_hook.unlink();
    if (_current_wait_begin_time) {
        _total_waiting_time
          += (clock::now() - _current_wait_begin_time.value());
        _current_wait_begin_time.reset();
    }
    _current_running_begin_time = clock::now();
    _translations_scheduled++;
}

void translator_executable::mark_idle() {
    vassert(
      !_waiting_hook.is_linked() && _running_hook.is_linked(),
      "Unexpected state transition to idle: {}",
      *this);
    _running_hook.unlink();
    if (_current_running_begin_time) {
        _total_running_time
          += (clock::now() - _current_running_begin_time.value());
        _current_running_begin_time.reset();
    }
    _stop_in_progress = false;
}

void translator_executable::mark_stopping() {
    vassert(
      !_waiting_hook.is_linked() && _running_hook.is_linked()
        && !_stop_in_progress,
      "Invalid request to stop translation: {}",
      *this);
    _translator->stop_translation();
    _stop_in_progress = true;
}

void executor::start_translation(
  translator_executable& state, clock::duration time_slice) {
    auto holder = gate.hold();
    try {
        state._translator->start_translation(time_slice);
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Exception {} starting translation: {}",
          std::current_exception(),
          state);
        // push it back to the end of the queue to try again later.
        state.mark_waiting();
        waiting.push_back(state);
        return;
    }
    state.mark_running();
    running.push_back(state);
}

void executor::stop_translation(translator_executable& state) {
    auto holder = gate.hold();
    vlog(datalake_log.debug, "stopping translator: {}", state);
    try {
        state.mark_stopping();
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Exception {} stopping translation: {}",
          std::current_exception(),
          state);
    }
}

scheduler::scheduler(
  size_t total_memory,
  size_t memory_block_size,
  std::unique_ptr<scheduling_policy> policy)
  : _scheduling_policy(std::move(policy))
  , _mem_tracker(reservations_tracker::make_default(
      total_memory, memory_block_size, *this)) {
    ssx::repeat_until_gate_closed_or_aborted(
      _executor.gate, _executor.as, [this] {
          return main().handle_exception([](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
              vlogl(
                datalake_log,
                log_level,
                "Encountered exception in main loop: {}",
                e);
          });
      });
}

void scheduler::notify_ready(const translator_id& id) {
    if (_executor.gate.is_closed()) {
        return;
    }
    vlog(datalake_log.trace, "ready notification from translator: {}", id);
    auto it = _executor.translators.find(id);
    if (it == _executor.translators.end()) {
        return;
    }
    auto& translator = it->second;
    if (
      translator._waiting_hook.is_linked()
      || translator._running_hook.is_linked()) {
        vlog(
          datalake_log.warn,
          "Invalid ready notification from translator, ignoring",
          translator);
        return;
    }
    vlog(datalake_log.trace, "Marking the translator ready: {}", translator);
    translator.mark_waiting();
    _executor.waiting.push_back(translator);
    _state_changed_cvar.signal();
}

void scheduler::notify_done(const translator_id& id) {
    auto holder = _executor.gate.hold();
    vlog(datalake_log.trace, "done notification from translator: {}", id);
    auto it = _executor.translators.find(id);
    if (it == _executor.translators.end()) {
        return;
    }
    auto& translator = it->second;
    if (
      translator._waiting_hook.is_linked()
      || !translator._running_hook.is_linked()) {
        vlog(
          datalake_log.warn,
          "Invalid done notification from waiting translator, ignoring",
          translator);
        return;
    }
    vlog(datalake_log.trace, "Marking the translator done: {}", translator);
    translator.mark_idle();
    _state_changed_cvar.signal();
}

bool scheduler::requires_scheduling_actions() const {
    return !_executor.waiting.empty() || _mem_tracker->memory_exhausted();
}

void scheduler::notify_memory_exhausted() {
    vlog(datalake_log.debug, "memory exhausted notification");
    _state_changed_cvar.signal();
}

ss::future<> scheduler::stop() {
    vlog(datalake_log.debug, "Stopping scheduler");
    _executor.as.request_abort();
    _state_changed_cvar.broken();
    co_await _executor.gate.close();
    co_await ss::max_concurrent_for_each(
      _executor.translators, 32, [](auto& it) mutable {
          return it.second._translator->close();
      });
}

ss::future<bool>
scheduler::add_translator(std::unique_ptr<translator> translator) {
    auto holder = _executor.gate.hold();
    const auto& id = translator->id();
    const auto& translators = _executor.translators;
    vlog(
      datalake_log.trace,
      "request to add translator with id: {}, current_translators: {}",
      id,
      translators.size());
    auto it = translators.find(id);
    if (it != translators.end()) {
        vlog(
          datalake_log.error,
          "duplicate translator registration: {}",
          it->second);
        co_return false;
    }
    it = _executor.translators
           .insert(
             std::make_pair(id, translator_executable{std::move(translator)}))
           .first;
    try {
        co_await it->second._translator->init(*this, *_mem_tracker);
    } catch (...) {
        vlog(
          datalake_log.error,
          "[{}] error initing translator: {}",
          id,
          std::current_exception());
        co_return false;
    }
    co_return true;
}

ss::future<> scheduler::remove_translator(const translator_id& id) {
    auto holder = _executor.gate.hold();
    vlog(
      datalake_log.trace,
      "request to remove translator with id: {}, current_translators: {}",
      id,
      _executor.translators.size());
    auto it = _executor.translators.find(id);
    if (it == _executor.translators.end()) {
        co_return;
    }
    it->second._waiting_hook.unlink();
    it->second._running_hook.unlink();
    auto translator = std::move(it->second);
    _executor.translators.erase(it);
    co_await translator._translator->close();
}

size_t scheduler::running_translators() const {
    return _executor.running.size();
}

ss::future<> scheduler::main() {
    vlog(datalake_log.trace, "Starting scheduling loop");
    auto holder = _executor.gate.hold();
    while (!_executor.as.abort_requested()) {
        co_await _state_changed_cvar.wait(
          [this] { return requires_scheduling_actions(); });
        vlog(
          datalake_log.trace,
          "scheduler tick,  memory_exhausted: {}",
          _mem_tracker->memory_exhausted());
        if (_mem_tracker->memory_exhausted()) {
            co_await _scheduling_policy->on_resource_exhaustion(
              _executor, *_mem_tracker);
        }
        co_await _scheduling_policy->schedule_one_translation(
          _executor, *_mem_tracker);
    }
}

// temporary default until a proper scheduling policy is implemented.
std::unique_ptr<scheduling_policy> scheduling_policy::make_default(
  size_t max_concurrent_translators, clock::duration translation_time_quota) {
    return std::make_unique<fair_scheduling_policy>(
      max_concurrent_translators, translation_time_quota);
}

std::unique_ptr<reservations_tracker> reservations_tracker::make_default(
  size_t total_memory,
  size_t memory_block_size,
  scheduling_notifications& notifier) {
    return std::make_unique<default_reservations_tracker>(
      total_memory, memory_block_size, notifier);
}

} // namespace datalake::translation::scheduling
