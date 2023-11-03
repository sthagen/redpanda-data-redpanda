/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "transform/transform_processor.h"

#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "random/simple_time_jitter.h"
#include "transform/logger.h"
#include "units.h"
#include "wasm/api.h"

#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/sleep.hh>

#include <optional>

namespace transform {

namespace {

class queue_output_consumer {
public:
    queue_output_consumer(ss::queue<model::record_batch>* output, probe* probe)
      : _output(output)
      , _probe(probe) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        // This is a "safe" cast as all our offsets come from the translating
        // reader, but we don't have a seperate type for kafka::record_batch vs
        // model::record_batch.
        _last_offset = model::offset_cast(b.last_offset());
        _probe->increment_read_bytes(b.size_bytes());
        co_await _output->push_eventually(std::move(b));
        co_return ss::stop_iteration::no;
    }
    std::optional<kafka::offset> end_of_stream() const { return _last_offset; }

private:
    std::optional<kafka::offset> _last_offset;
    ss::queue<model::record_batch>* _output;
    probe* _probe;
};

struct drain_result {
    ss::chunked_fifo<model::record_batch> batches;
    kafka::offset latest_offset;
};

ss::future<drain_result>
drain_queue(ss::queue<transformed_batch>* queue, probe* p) {
    static constexpr size_t max_batches_bytes = 1_MiB;
    if (queue->empty()) {
        co_await queue->not_empty();
    }
    drain_result result;
    size_t batches_size = 0;
    while (!queue->empty()) {
        auto batch_size = queue->front().batch.size_bytes();
        batches_size += queue->front().batch.size_bytes();
        // ensure if there is a large batch we make some progress
        // otherwise cap how much data we send to the sink at once.
        if (!result.batches.empty() && batches_size > max_batches_bytes) {
            break;
        }
        p->increment_write_bytes(batch_size);
        auto transformed = queue->pop();
        result.latest_offset = transformed.input_offset;
        result.batches.push_back(std::move(transformed.batch));
    }
    co_return result;
}

/**
 * A specific exception to throw on processor::stop so that we're not accidently
 * swallowing unexpected exceptions.
 */
class processor_shutdown_exception : public std::exception {
    const char* what() const noexcept override {
        return "processor shutting down";
    }
};

} // namespace

processor::processor(
  model::transform_id id,
  model::ntp ntp,
  model::transform_metadata meta,
  ss::shared_ptr<wasm::engine> engine,
  error_callback cb,
  std::unique_ptr<source> source,
  std::vector<std::unique_ptr<sink>> sinks,
  std::unique_ptr<offset_tracker> offset_tracker,
  probe* p)
  : _id(id)
  , _ntp(std::move(ntp))
  , _meta(std::move(meta))
  , _engine(std::move(engine))
  , _source(std::move(source))
  , _sinks(std::move(sinks))
  , _offset_tracker(std::move(offset_tracker))
  , _error_callback(std::move(cb))
  , _probe(p)
  , _consumer_transform_pipe(1)
  , _transform_producer_pipe(1)
  , _task(ss::now())
  , _logger(tlog, ss::format("{}/{}", _meta.name(), _ntp.tp.partition())) {
    vassert(
      _sinks.size() == 1,
      "expected only a single sink, got: {}",
      _sinks.size());
}

ss::future<> processor::start() {
    try {
        co_await _engine->start();
    } catch (const std::exception& ex) {
        vlog(_logger.warn, "error starting processor engine: {}", ex);
        _error_callback(_id, _ntp, _meta);
    }
    _as = {};
    _consumer_transform_pipe = ss::queue<model::record_batch>(1);
    _transform_producer_pipe = ss::queue<transformed_batch>(1);
    _task = when_all_shutdown(
      run_consumer_loop(), run_transform_loop(), run_producer_loop());
}

ss::future<> processor::stop() {
    auto ex = std::make_exception_ptr(processor_shutdown_exception());
    _as.request_abort_ex(ex);
    _consumer_transform_pipe.abort(ex);
    _transform_producer_pipe.abort(ex);
    co_await std::exchange(_task, ss::now());
    co_await _engine->stop();
}

ss::future<> processor::poll_sleep() {
    constexpr auto fallback_poll_interval = std::chrono::seconds(1);
    simple_time_jitter<ss::lowres_clock> jitter(fallback_poll_interval);
    try {
        co_await ss::sleep_abortable<ss::lowres_clock>(
          jitter.next_duration(), _as);
    } catch (const ss::sleep_aborted&) {
        // do nothing, the caller will handle exiting properly.
    }
}

ss::future<kafka::offset> processor::load_start_offset() {
    auto latest_committed = co_await _offset_tracker->load_committed_offset();
    if (latest_committed) {
        co_return kafka::next_offset(latest_committed.value());
    }
    auto latest = _source->latest_offset();
    // We "commit" at the previous offset so that we resume at the latest.
    co_await _offset_tracker->commit_offset(kafka::prev_offset(latest));
    co_return latest;
}

ss::future<> processor::run_consumer_loop() {
    auto offset = co_await load_start_offset();
    vlog(_logger.trace, "starting at offset {}", offset);
    while (!_as.abort_requested()) {
        // TODO(rockwood): It's possible that the stored is deleted due to
        // retention policy since the last successful commit. We should handle
        // this by restarting from the low watermark.
        auto reader = co_await _source->read_batch(offset, &_as);
        auto last_offset = co_await std::move(reader).consume(
          queue_output_consumer(&_consumer_transform_pipe, _probe),
          model::no_timeout);
        if (!last_offset) {
            vlog(
              _logger.trace,
              "received no results, sleeping before polling at offset {}",
              offset);
            co_await poll_sleep();
            continue;
        }
        offset = kafka::next_offset(*last_offset);
        vlog(_logger.trace, "consumed up to offset {}", offset);
    }
}

ss::future<> processor::run_transform_loop() {
    while (!_as.abort_requested()) {
        auto batch = co_await _consumer_transform_pipe.pop_eventually();
        auto offset = model::offset_cast(batch.last_offset());
        batch = co_await _engine->transform(std::move(batch), _probe);
        co_await _transform_producer_pipe.push_eventually(
          {.batch = std::move(batch), .input_offset = offset});
    }
}

ss::future<> processor::run_producer_loop() {
    while (!_as.abort_requested()) {
        auto drained = co_await drain_queue(&_transform_producer_pipe, _probe);
        co_await _sinks[0]->write(std::move(drained.batches));
        co_await _offset_tracker->commit_offset(drained.latest_offset);
    }
}

template<typename... T>
ss::future<> processor::when_all_shutdown(T&&... futs) {
    return ss::when_all_succeed(handle_run_loop(std::forward<T>(futs))...)
      .discard_result();
}

ss::future<> processor::handle_run_loop(ss::future<> fut) {
    try {
        co_await std::move(fut);
    } catch (const processor_shutdown_exception&) {
        // Do nothing, this is an expected error on shutdown
    } catch (const std::exception& ex) {
        vlog(_logger.warn, "error running transform: {}", ex);
        _error_callback(_id, _ntp, _meta);
    }
}

model::transform_id processor::id() const { return _id; }
const model::ntp& processor::ntp() const { return _ntp; }
const model::transform_metadata& processor::meta() const { return _meta; }
bool processor::is_running() const { return !_task.available(); }
} // namespace transform