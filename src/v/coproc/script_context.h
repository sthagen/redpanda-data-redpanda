/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "coproc/exception.h"
#include "coproc/script_context_router.h"
#include "coproc/shared_script_resources.h"
#include "coproc/supervisor.h"
#include "coproc/types.h"
#include "random/simple_time_jitter.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

namespace coproc {
/**
 * The script_context is the smallest schedulable unit in the coprocessor
 * framework. One context is created per registered coprocessor script,
 * representing one fiber.
 *
 * Important to note is the level of concurrency provided. Within a
 * script_context there is one fiber for which scheduled asynchronous work is
 * performed in sync, meaning that for each read -> send -> write that occurs
 * within the run loop, those actions will occur in order.
 *
 * Since each script_context has one of these fibers of its own, no one context
 * will wait for work to be finished by another in order to continue making
 * progress. They all operate independently of eachother.
 */
class script_context {
public:
    /**
     * class constructor
     * @param script_id Uniquely identifyable id
     * @param ctx Shared state, shared across all script_contexts on a shard
     * @param ntp_ctxs Map of interested ntps, strongly retained by 'this'
     **/
    explicit script_context(
      script_id, shared_script_resources&, routes_t&&) noexcept;

    script_context(const script_context&) = delete;
    script_context(script_context&&) = delete;
    script_context& operator=(script_context&&) = delete;
    script_context& operator=(const script_context&) = delete;

    ~script_context() noexcept = default;

    /// Startups up a single managed fiber responsible for maintaining the pace
    /// of the run loop.
    /// Returns a future which returns when the script fiber
    /// has completed or throws \ref script_failed_exception
    ss::future<> start();

    /**
     * Stop the single managed fiber started by 'start()'.
     * @returns a future that resolves when the fiber stops and all resources
     * held by this context are relinquished
     */
    ss::future<> shutdown();

    /// Returns copy of active routes
    routes_t get_routes() const { return _routes; }

private:
    ss::future<> do_execute();

    ss::future<>
      send_request(supervisor_client_protocol, process_batch_request);

    ss::future<> process_reply(process_batch_reply);

private:
    /// Killswitch for in-process reads
    ss::abort_source _abort_source;

    /// Manages async fiber that begins when calling 'start()'
    ss::gate _gate;

    // Reference to resources shared across all script_contexts on 'this'
    // shard
    shared_script_resources& _resources;

    /// Collection representing all inputs and outputs for this coprocessor.
    /// Offsets per materialized topic are tracked and input is only incremented
    /// when all outputs are up to date.
    routes_t _routes;

    /// Uniquely identifying script id. Generated by coproc engine
    script_id _id;
};
} // namespace coproc
