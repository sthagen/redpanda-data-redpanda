/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/property.h"
#include "ssx/semaphore.h"
#include "units.h"

#include <cstdint>

namespace storage {

/**
 * This class is extension of ss::semaphore to fit the needs
 * of the storage_resources class's tracking of byte/concurrency
 * allowances.
 *
 * Callers may use this class as either a soft or hard quota.  In
 * the hard case, regular async-waiting semaphore calls (ss::get_units)
 * may be used.  In the soft case, the take() function will allow the
 * semaphore count to go negative, but return a `checkpoint_hint` field
 * that prompts the holder of the units to release some.
 *
 * This is 'adjustable' in that:
 * - Regular sempahores are just a counter: they have no
 *   memory of their intended capacity.  In order to enable runtime
 *   changes to the max units in a semaphore, we must keep an extra
 *   record of the capacity.
 * - This enables runtime configuration changes to parameters that
 *   control the capacity of a semaphore.
 */
class adjustable_allowance {
public:
    explicit adjustable_allowance(uint64_t capacity)
      : adjustable_allowance(capacity, "s/allowance") {}
    adjustable_allowance(uint64_t capacity, const ss::sstring& sem_name)
      : _sem(capacity, sem_name)
      , _capacity(capacity) {}

    void set_capacity(uint64_t capacity) noexcept {
        if (capacity > _capacity) {
            _sem.signal(capacity - _capacity);
        } else if (capacity < _capacity) {
            _sem.consume(_capacity - capacity);
        }

        _capacity = capacity;
    }

    /**
     * When a consumer wants some units, it gets them unconditionally, but
     * gets a hint as to whether it exceeded the capacity.  That is the hint
     * to e.g. the offset translator that now is the time to checkpoint
     * because there are too many dirty bytes.
     */
    struct take_result {
        ssx::semaphore_units units;
        bool checkpoint_hint{false};
    };

    /**
     * Non-blocking consume of units, may send the semaphore negative.
     *
     * Includes a hint in the response if the semaphore has gone negative,
     * to induce the caller to release some units when they can.
     */
    take_result take(size_t units) {
        take_result result = {
          .units = ss::consume_units(_sem, units),
          .checkpoint_hint = _sem.current() <= 0};

        return result;
    }

    /**
     * Blocking get units: will block until units are available.
     */
    ss::future<ssx::semaphore_units> get_units(size_t units) {
        return ss::get_units(_sem, units);
    }

    size_t current() const noexcept { return _sem.current(); }

private:
    ssx::semaphore _sem;

    uint64_t _capacity;
};

/**
 * This class is used by various storage components to control consumption
 * of shared system resources.  It broadly does this in two ways:
 * - Limiting concurrency of certain types of operation
 * - Controlling buffer sizes depending on available resources
 */
class storage_resources {
public:
    // If we don't have this much disk space available per partition,
    // don't both falloc'ing at all.
    static constexpr size_t min_falloc_step = 128_KiB;

    storage_resources();
    storage_resources(config::binding<size_t>);
    storage_resources(
      config::binding<size_t>,
      config::binding<uint64_t>,
      config::binding<uint64_t>,
      config::binding<uint64_t>);
    storage_resources(const storage_resources&) = delete;

    /**
     * Call this when the storage::node_api state is updated
     */
    void update_allowance(uint64_t total, uint64_t free);

    /**
     * Call this when topics_table gets updated
     */
    void update_partition_count(size_t partition_count);

    uint64_t get_space_allowance() { return _space_allowance; }

    size_t get_falloc_step(std::optional<uint64_t>);
    size_t calc_falloc_step();

    bool
    offset_translator_take_bytes(int32_t bytes, ssx::semaphore_units& units);

    bool
    configuration_manager_take_bytes(size_t bytes, ssx::semaphore_units& units);

    bool stm_take_bytes(size_t bytes, ssx::semaphore_units& units);

    adjustable_allowance::take_result compaction_index_take_bytes(size_t bytes);
    bool compaction_index_bytes_available() {
        return _compaction_index_bytes.current() > 0;
    }

    ss::future<ssx::semaphore_units> get_recovery_units() {
        return _inflight_recovery.get_units(1);
    }

    ss::future<ssx::semaphore_units> get_close_flush_units() {
        return _inflight_close_flush.get_units(1);
    }

    /**
     * An adjustable_allowance will set checkpoint_hint whenever its units
     * are exhausted, but this can happen with pathological frequency if
     * many units are hogged by partitions that have written a lot of
     * data then stopped.
     *
     * To mitigate this, filter out checkpoint hints if the partition
     * taking units is not occupying more than a threshold number of
     * units.  This means that instead of doing an avalanche of snapshots
     * under this unpleasant state, we will instead violate target_replay_bytes
     */
    bool filter_checkpoints(
      adjustable_allowance::take_result&&, ssx::semaphore_units&);

    /**
     * Call this when the partition count or the target replay bytes changes
     */
    void update_min_checkpoint_bytes();

private:
    uint64_t _space_allowance{9};
    uint64_t _space_allowance_free{0};

    size_t _partition_count{9};
    config::binding<size_t> _segment_fallocation_step;
    config::binding<uint64_t> _target_replay_bytes;
    config::binding<uint64_t> _max_concurrent_replay;
    config::binding<uint64_t> _compaction_index_mem_limit;
    size_t _append_chunk_size;

    // A lower bound on how many units a caller must have to be
    // eligible for flush, to prevent pathological case where on caller
    // happens to repeatedly request units close to the parent semaphore
    // being exhausted
    // See https://github.com/redpanda-data/redpanda/issues/6854
    uint64_t _min_checkpoint_bytes{0};

    size_t _falloc_step{0};
    bool _falloc_step_dirty{false};

    // These 'dirty_bytes' semaphores control how many bytes
    // may be written to logs in between checkpoints/snapshots, in
    // order to limit the quantity of data that must be replayed after
    // a restart.

    // How many bytes may all logs on this shard advance before
    // the offset translator must checkpoint to the kvstore?
    adjustable_allowance _offset_translator_dirty_bytes{0};

    // How many bytes may logs write between checkpoints of the
    // configuration_manager?
    adjustable_allowance _configuration_manager_dirty_bytes{0};

    // How many bytes may all consensus instances write before
    // we ask them to start snapshotting their state machines?
    adjustable_allowance _stm_dirty_bytes{0};

    // How much memory may all compacted partitions on this shard
    // use for their spill_key_index objects
    adjustable_allowance _compaction_index_bytes{0};

    // How many logs may be recovered (via log_manager::manage)
    // concurrently?
    adjustable_allowance _inflight_recovery{0};

    // How many logs may be flushed during segment close concurrently?
    // (e.g. when we shut down and ask everyone to flush)
    adjustable_allowance _inflight_close_flush{0};
};

} // namespace storage