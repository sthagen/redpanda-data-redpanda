/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/partition_translator.h"

#include "datalake/logger.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/coroutine/as_future.hh>

namespace datalake::translation {

namespace {
using namespace std::chrono_literals;
// A simple utility to conditionally retry with backoff on failures.
static constexpr std::chrono::milliseconds initial_backoff{300};
static constexpr std::chrono::milliseconds max_translation_task_timeout{3min};
constexpr model::timeout_clock::duration wait_timeout = 5s;

template<
  typename Func,
  typename ShouldRetry,
  typename FuncRet = std::invoke_result_t<Func>,
  typename RetValueType = ss::futurize<FuncRet>::value_type>
requires std::predicate<ShouldRetry, RetValueType>
ss::futurize_t<FuncRet> retry_with_backoff(
  retry_chain_node& parent_rcn, Func&& f, ShouldRetry&& should_retry) {
    parent_rcn.check_abort();
    auto rcn = retry_chain_node(&parent_rcn);
    while (true) {
        auto result_f = co_await ss::coroutine::as_future<RetValueType>(
          ss::futurize_invoke(f));
        auto failed = result_f.failed();
        // eagerly take the exception out to avoid ignored exceptional
        // futures as retry() below can throw.
        std::exception_ptr ex = failed ? result_f.get_exception() : nullptr;
        auto retry = rcn.retry();
        if (!retry.is_allowed) {
            // No more retries allowed, propagated whatever we have.
            if (failed) {
                vassert(
                  ex != nullptr,
                  "Invalid exception, should be non null on a failed future.");
                std::rethrow_exception(ex);
            }
            co_return result_f.get();
        }
        // Further retries are allowed, check for exceptions if any.
        if (!failed) {
            auto result = result_f.get();
            if (!should_retry(result)) {
                co_return result;
            }
        }
        co_await ss::sleep_abortable(retry.delay, *retry.abort_source);
    }
}
} // namespace

partition_translator::partition_translator(
  ss::scheduling_group sg,
  std::unique_ptr<coordinator_api> coordinator,
  std::unique_ptr<data_source> data_source,
  std::unique_ptr<translation_context> translation_ctx)
  : _sg(sg)
  , _coordinator(std::move(coordinator))
  , _data_source(std::move(data_source))
  , _translation_ctx(std::move(translation_ctx))
  , _term(_data_source->term())
  , _logger(
      datalake_log, fmt::format("{}-term-{}", _data_source->ntp(), _term)) {}

void partition_translator::reconcile_properties() noexcept {
    if (_gate.is_closed()) {
        return;
    }
    _translation_ctx->reconcile_properties();
}

ss::future<coordinator::fetch_latest_translated_offset_reply>
partition_translator::fetch_latest_translated_offset(retry_chain_node& rcn) {
    auto request = coordinator::fetch_latest_translated_offset_request{};
    request.tp = _data_source->ntp().tp;
    request.topic_revision = _data_source->topic_revision();

    co_return co_await retry_with_backoff(
      rcn,
      [this, request] {
          return _coordinator->fetch_latest_translated_offset(request);
      },
      [this](coordinator::fetch_latest_translated_offset_reply reply) {
          return !_as.abort_requested()
                 && coordinator::is_retriable(reply.errc);
      });
}

ss::future<coordinator::add_translated_data_files_reply>
partition_translator::checkpoint_translation_result(
  retry_chain_node& rcn, coordinator::translated_offset_range range) {
    auto request = coordinator::add_translated_data_files_request{};
    request.tp = _data_source->ntp().tp;
    request.topic_revision = _data_source->topic_revision();
    request.ranges.push_back(std::move(range));

    co_return co_await retry_with_backoff(
      rcn,
      [this, request = std::move(request)] {
          return _coordinator->add_translated_data_files(request.copy());
      },
      [this](coordinator::add_translated_data_files_reply reply) {
          return !_as.abort_requested()
                 && coordinator::is_retriable(reply.errc);
      });
}

ss::future<>
partition_translator::translate_when_notified(kafka::offset begin_offset) {
    co_await _ready_to_translate.wait(
      [this] { return _inflight_translation_state.has_value(); });

    auto& as = _inflight_translation_state->as;
    auto reader = co_await _data_source->make_log_reader(
      begin_offset, datalake_priority(), as);
    if (!reader) {
        co_return;
    }
    ss::timer<scheduling::clock> cancellation_timer;
    cancellation_timer.set_callback([&as] { as.request_abort(); });

    auto translation_f
      = _translation_ctx
          ->translate_now(
            std::move(reader.value()), _inflight_translation_state->as)
          .finally([this] { return _translation_ctx->flush(); });
    cancellation_timer.arm(_inflight_translation_state->translate_for);
    co_await std::move(translation_f).finally([&cancellation_timer] {
        cancellation_timer.cancel();
    });
}

ss::future<> partition_translator::translate_until_stopped() {
    const auto& id = _data_source->ntp();
    vassert(
      _initialized && _scheduler && _reservations,
      "[{}] Translation started before the translator is properly initialized",
      id);

    while (!_as.abort_requested()) {
        retry_chain_node rcn{
          _as, max_translation_task_timeout, initial_backoff};

        // Reconcile with the coordinator
        auto result = co_await fetch_latest_translated_offset(rcn);
        if (result.errc != coordinator::errc::ok) {
            vlog(_logger.warn, "Failed to fetch translated offset: {}", result);
            continue;
        }

        auto last_committed_offset = result.last_iceberg_committed_offset;
        auto last_translated_offset = result.last_added_offset.value_or(
          kafka::prev_offset(_data_source->min_offset_for_translation()));

        auto reset_error
          = co_await _data_source->update_highest_translated_offset(
            last_translated_offset, _term, wait_timeout, _as);

        if (reset_error) {
            vlog(
              _logger.warn,
              "error updating highest translated offset: {}, translation will "
              "be retried",
              reset_error);
            continue;
        }

        // Update partition metrics. Note that last committed offset here
        // is NOT synchronized with outstanding commit operations. Therefore
        // if we reach this point before the most recent batch of files has
        // been committed, the commit lag metric will be out of sync at least
        // until 'wait_for_data' returns and we re-enter the loop.
        _data_source->update_commit_lag(last_committed_offset);
        _data_source->update_translation_lag(last_translated_offset);

        // Wait until some data is ready to be translated.
        auto maybe_begin_offset
          = co_await _data_source->wait_for_data_to_translate(
            last_translated_offset, _as);

        // if wait_for_data timed out (i.e. all trnaslatable records have
        // been translated already), reenter the loop. this gives us an
        // opportunity to reconcile outstanding coordinator state, which is
        // helpful for keeping lag metrics up-to-date.
        if (!maybe_begin_offset.has_value()) {
            continue;
        }

        auto begin_offset = maybe_begin_offset.value();

        // Notify the scheduler that there is some data to translate
        _scheduler->notify_ready(id);

        // wait for the scheduler to notify back that we've been given a time
        // slice (i.e. scheduled in), then translate until the time slice
        // expires or we run out of data
        auto translate_f = co_await ss::coroutine::as_future<>(
          translate_when_notified(begin_offset));

        // inflight_translation_state tracks a single scheduled chunk of work,
        // so we reset it to nullopt for the next time we're scheduled in
        _inflight_translation_state.reset();

        // Let the scheduler know we are done
        _scheduler->notify_done(id);

        if (translate_f.failed()) {
            vlog(
              _logger.warn,
              "Translation attempt failed: {}, discarding state to reset "
              "translation",
              translate_f.get_exception());
            // todo(tests): add more tests to exercise this branch logic
            // todo: enhance the API to skip uploading to cloud storage.
            co_await _translation_ctx->finish(rcn, _as).discard_result();
            continue;
        }

        // TODO: add logic to skip finish on every iteration. This is
        // predicated on whether we are close to the lag deadline or if too many
        // bytes have accumulated
        // With out this, we finish() on every translation which makes it
        // equivalent to the logic without this patch.

        auto translation_result = co_await _translation_ctx->finish(rcn, _as);
        if (!translation_result) {
            vlog(_logger.warn, "Failed to translate, retrying");
            continue;
        }

        if (begin_offset != translation_result->start_offset) {
            // This is possible if there is a gap in offsets range, eg from
            // compaction. Normally that shouldn't be the case, as translation
            // enforces max_collectible_offset which prevents compaction or
            // other forms of retention from kicking in before translation
            // actually happens. However there could be a sequence of enabling /
            // disabling iceberg configuration on the topic that can temporarily
            // unblock compaction thus creating gaps. Here we adjust the offset
            // range to so the coordinator sees a contiguous offset range.
            vlog(
              _logger.info,
              "detected an offset range gap in [{}, {}), adjusting the begin "
              "offset to avoid gaps in coordinator tracked offsets.",
              begin_offset,
              translation_result->start_offset);
            translation_result->start_offset = begin_offset;
        }

        last_translated_offset = translation_result->last_offset;
        auto checkpoint_result = co_await checkpoint_translation_result(
          rcn, std::move(translation_result.value()));
        if (checkpoint_result.errc != coordinator::errc::ok) {
            vlog(
              _logger.warn,
              "Failed to checkpoint translated files: {}",
              checkpoint_result);
        }

        reset_error = co_await _data_source->update_highest_translated_offset(
          last_translated_offset, _term, wait_timeout, _as);
        if (reset_error) {
            vlog(
              _logger.warn,
              "error updating highest translated offset: {}",
              reset_error);
        }
    }
}

const scheduling::translator_id& partition_translator::id() const {
    return _data_source->ntp();
}

ss::future<> partition_translator::init(
  scheduling::scheduling_notifications& scheduler,
  scheduling::reservations_tracker& reservations) {
    _scheduler = &scheduler;
    _reservations = &reservations;
    _initialized = true;
    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this] {
        return ss::with_scheduling_group(_sg, [this] {
            return translate_until_stopped().handle_exception(
              [this](const std::exception_ptr& ex) {
                  auto log_level = ssx::is_shutdown_exception(ex)
                                     ? ss::log_level::debug
                                     : ss::log_level::warn;
                  vlogl(
                    datalake_log,
                    log_level,
                    "[{}] Encountered exception in translation loop: {}",
                    ex,
                    _data_source->ntp());
              });
        });
    });
    return ss::make_ready_future();
}

ss::future<> partition_translator::close() noexcept {
    vlog(_logger.debug, "stopping partition translator in term {}", _term);
    _as.request_abort();
    _ready_to_translate.broken();
    if (_inflight_translation_state) {
        _inflight_translation_state->as.request_abort();
    }
    _data_source->close();
    co_await _gate.close();
    vlog(_logger.debug, "stopped partition translator in term {}", _term);
}

scheduling::translation_status partition_translator::status() const {
    // Biggest TODO: Currently we do not track intervals of target lag.
    // So the scheduler is pretty much scheduling on defaults
    // Track intervals return the correct remaining duration / memory
    // reservations. The latter is already exposed as mux::buffered_bytes()
    return {};
}

ss::future<std::optional<std::chrono::milliseconds>>
partition_translator::current_lag_ms() const {
    return _data_source->current_lag_ms(wait_timeout);
}

void partition_translator::start_translation(
  scheduling::clock::duration duration) {
    if (_gate.is_closed()) {
        return;
    }
    vassert(
      !_inflight_translation_state.has_value(),
      "Invalid translation state - attempt to start translation on "
      "{} when already in flight.",
      _data_source->ntp());
    _inflight_translation_state = inflight_translation_state{
      .translate_for = duration};
    _ready_to_translate.broadcast();
}

void partition_translator::stop_translation() {
    if (_gate.is_closed() || !_inflight_translation_state) {
        return;
    }
    _inflight_translation_state->as.request_abort();
}
} // namespace datalake::translation
