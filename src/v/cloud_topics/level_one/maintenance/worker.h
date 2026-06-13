/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"
#include "cloud_topics/level_one/maintenance/compaction/compaction_source.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/worker_probe.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cluster/metadata_cache.h"
#include "compaction/key_offset_map.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "ssx/semaphore.h"
#include "ssx/work_queue.h"
#include "utils/adjustable_semaphore.h"

#include <seastar/core/scheduling.hh>

#include <absl/hash/hash.h>

class WorkerManagerTestFixture;

namespace cloud_topics::l1 {

class worker_manager;

// Selects which job(s) of maintenance work a pause/resume applies to.
enum class maintenance_job_type { compaction, leveling, all };

// A per-shard worker that accepts compaction and leveling jobs and performs
// either de-duplication (for compaction) or rewrites (for leveling) using a
// `sink`, `source`, and `reducer`. Can be pre-empted to either cancel or stop
// an inflight job. Compaction and leveling run on independent fibers; leveling
// supports multiple inflight jobs per worker shard up to a configurable cap.
class compaction_worker {
public:
    // Describes whether a worker on a given shard is `active` and available
    // for compaction jobs, or `paused` and temporarily unavailable, or fully
    // `stopped`.
    enum class worker_state { active, paused, stopped };

    // io and metastore are passed to the compaction `source` and `sink`.
    compaction_worker(
      worker_manager*,
      io*,
      metastore*,
      cluster::metadata_cache*,
      ss::scheduling_group,
      level_one_reader_probe*);

    // Launches background loop.
    ss::future<> start();

    // Closes concurrency primitives and sets `_compaction_job_state` and both
    // per-job worker states to `stopped` to indicate to a potential inflight
    // compaction/leveling job that it should exit early before waiting on and
    // clearing the `*_work_fut`s.
    ss::future<> stop();

    // Sets `_state = compaction_job_state::soft_stop`. This is a request to
    // checkpoint any valuable progress from the inflight compaction job and
    // finish at earliest convenience, e.g. when a worker shard is being
    // pre-empted for various reasons. It is up to users/currently running
    // compaction jobs to respect this flag.
    //
    // This function cancels the inflight compaction job but does not affect the
    // worker state- the worker will continue to accept compaction jobs
    // after this function is called.
    void interrupt_compaction_job();

    // Sets `_state = compaction_job_state::hard_stop`, indicating the inflight
    // compaction job should stop promptly and abandon any in progress work,
    // e.g. during shutdown. It is up to users/currently running compaction jobs
    // to respect this flag.
    //
    // This function stops the inflight compaction job but does not affect
    // the worker state- the worker will continue to accept compaction jobs
    // after this function is called.
    void terminate_compaction_job();

    // Submits a `do_pause_worker()` job to the `_worker_update_queue`, pausing
    // the requested type(s) of work (compaction, leveling, or all) on this
    // worker.
    ss::future<> pause_worker(maintenance_job_type);

    // Submits a `do_resume_worker()` job to the `_worker_update_queue`,
    // resuming the requested type(s) of work on this worker.
    ss::future<> resume_worker(maintenance_job_type);

    // Alert the compaction fiber that new compaction work may be available.
    void alert_compaction_fiber();

    // Alert the leveling fiber that new leveling work may be available.
    void alert_leveling_fiber();

    // Hard-stops every inflight leveling job for every tidp on this worker.
    void terminate_leveling_jobs();

    // Hard-stops every inflight leveling job for the given tidp on this worker.
    void terminate_leveling_jobs_for_tidp(model::topic_id_partition);

private:
    // Launches a single backgrounded loop into its `*_work_fut`, if not
    // already running.
    void resume_compaction_work_loop();
    void resume_leveling_work_loop();

    ss::future<> pause_compaction_work_loop();
    ss::future<> pause_leveling_work_loop();

    // The compaction loop which waits for jobs to become available.
    ss::future<> compaction_work_loop();

    // The leveling loop which dispatches up to
    // `cloud_topics_max_concurrent_leveling_jobs_per_shard` jobs concurrently
    // via an `adjustable_semaphore` slot pool.
    ss::future<> leveling_work_loop();

    // Joins a single loop's future and clears it.
    ss::future<> clear_compaction_work_fut();
    ss::future<> clear_leveling_work_fut();

    // Soft-stops every inflight leveling job on this worker (a graceful
    // wind-down request), e.g. when leveling is paused.
    void interrupt_leveling_jobs();

    // Pauses the requested type(s) of work: interrupts the inflight job(s),
    // marks the job as paused so its loop won't run, and joins the loop fiber,
    // leaving the corresponding `*_work_fut` as `std::nullopt`. No new jobs of
    // that kind are processed until resumed.
    ss::future<> do_pause_worker(maintenance_job_type);

    // Resumes the requested type(s) of work: clears the paused mark and
    // relaunches the loop fiber.
    ss::future<> do_resume_worker(maintenance_job_type);

    // Requests a compaction of the provided job's CTP against the metastore
    // sample it carries.
    ss::future<> compact_log(compaction_job*);

    // Small wrapper around `do_level_range()` that completes the job on the
    // manager and releases its concurrency slot when done.
    ss::future<> level_range(foreign_leveling_job_ptr, ssx::semaphore_units);

    // Runs one leveling range, driving the leveling source/sink/reducer
    // pipeline with a single-range input.
    ss::future<> do_level_range(leveling_job*);

    // Retrieves a compaction job from the `_worker_manager`, if one is
    // available.
    ss::future<std::optional<foreign_compaction_job_ptr>>
    try_acquire_compaction_work_from_manager();

    // Retrieves a leveling job from the `_worker_manager`, if one is available.
    ss::future<std::optional<foreign_leveling_job_ptr>>
    try_acquire_leveling_work_from_manager();

    // After completing a compaction job, go back to the `worker_manager` shard
    // to mark the work as "complete" (i.e reset the CTP's `inflight_shard` to
    // indicate there is no longer an in-process compaction occurring).
    ss::future<>
      complete_compaction_work_on_manager(foreign_compaction_job_ptr);

    // After completing a leveling job, go back to the `worker_manager` shard
    // to mark the work as "complete".
    ss::future<> complete_leveling_work_on_manager(foreign_leveling_job_ptr);

    // Performs lazy initialization of the `compaction::key_offset_map` using
    // its reserved memory, if it is uninitialized.
    ss::future<> initialize_map();

    // Returns `true` iff the worker is currently in an `active` state. That is,
    // the worker has not been `paused`, nor has it been `stopped` or is in the
    // process of shutdown.
    bool should_run_compaction() const;
    bool should_run_leveling() const;

private:
    friend class ::WorkerManagerTestFixture;

    // The state of a potentially inflight compaction job (`idle`, `running`,
    // `cancelled`, or `stopped`) on this worker. `idle` means no compaction job
    // is currently running on this worker. `running` means a compaction job is
    // inflight. `cancelled` means that the inflight compaction job on this
    // worker has been requested to checkpoint its valuable progress and finish
    // at earliest convenience (a graceful stop), whereas `stopped` means that
    // the inflight compaction job running on this worker has been pre-empted to
    // abandon all work and return as soon as possible. `cancelled`/`stopped` do
    // not mean that the worker itself is stopped from running future compaction
    // jobs.
    compaction_job_state _compaction_job_state{compaction_job_state::idle};

    // The state(s) of the worker, which are `active`, `paused`, or `stopped`.
    // Compaction and leveling are independent.
    // * A worker in an `active` state should have an active `_*_work_fut` value
    //   which is accepting and completing maintenance jobs.
    // * A worker in a `paused` state has `_*_work_fut == std::nullopt`
    //   and is not accepting maintenance jobs.
    // * A worker in a `stopped` state is in the process of shutting down and
    //   therefore has its concurrency primitives closed and is not accepting
    //   maintenance jobs.
    worker_state _worker_target_compaction_state{worker_state::paused};
    worker_state _worker_target_leveling_state{worker_state::paused};

    std::optional<model::ntp> _inflight_ntp;

    // Per-inflight-leveling-job soft/hard-stop signal. Lives until the job's
    // background fiber completes. Multiple jobs may be inflight concurrently
    // on this worker, each with its own state.
    struct leveling_job_handle {
        compaction_job_state state{compaction_job_state::idle};
    };
    using leveling_job_handle_ptr = ss::lw_shared_ptr<leveling_job_handle>;

    // Keyed by (tidp, base_offset) to disambiguate multiple ranges of the same
    // CTP running concurrently on this shard.
    struct inflight_key {
        model::topic_id_partition tidp;
        kafka::offset base_offset;

        bool operator==(const inflight_key&) const = default;
    };
    struct inflight_key_hash {
        using is_transparent = void;
        size_t operator()(const inflight_key& k) const noexcept {
            return absl::HashOf(k.tidp, k.base_offset);
        }
    };

    chunked_hash_map<inflight_key, leveling_job_handle_ptr, inflight_key_hash>
      _inflight_leveling;

    // If set, the active background loops for taking jobs from the
    // `_worker_manager` and running them.
    std::optional<ss::future<>> _compaction_work_fut;
    std::optional<ss::future<>> _leveling_work_fut;

    // A queue which is used to linearize pause/resume requests of this worker.
    ssx::work_queue _worker_update_queue;

    // The shard local key-offset map used for de-duplication during compaction.
    // This is lazily initialized when a compaction job is first ran on this
    // worker/shard.
    std::unique_ptr<compaction::hash_key_offset_map> _map{nullptr};

    ss::gate _gate;

    ss::abort_source _as;

    // Used to alert the compaction fiber that a job has become available, or
    // when `cloud_topics_compaction_interval_ms` config changes.
    ss::condition_variable _compaction_cv;

    // Used to alert the leveling fiber that a job has become available, or
    // when `cloud_topics_leveling_interval_ms` config changes.
    ss::condition_variable _leveling_cv;

    // Signalled whenever all inflight leveling jobs are drained.
    ss::condition_variable _leveling_drained_cv;

    // The interval on which the compaction fiber polls for new work.
    config::binding<std::chrono::milliseconds> _compaction_poll_interval;

    // The interval on which the leveling fiber polls for new work.
    config::binding<std::chrono::milliseconds> _leveling_poll_interval;

    // Caps how many leveling jobs run concurrently on this shard.
    config::binding<size_t> _leveling_max_concurrent_jobs;
    adjustable_semaphore _leveling_sem;

    // Captured at construction so that changing the config at runtime does not
    // take effect without a restart.
    size_t _upload_part_size;

    // Owned by `scheduler`.
    worker_manager* _worker_manager;

    // Owned by `app`.
    io* _io;

    // Owned by `app`.
    metastore* _metastore;

    cluster::metadata_cache* _metadata_cache;

    ss::scheduling_group _compaction_sg;

    compaction_worker_probe _probe;

    // Owned by `app`.
    level_one_reader_probe* _l1_reader_probe;
};

} // namespace cloud_topics::l1
