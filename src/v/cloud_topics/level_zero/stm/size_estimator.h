/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/format_to.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <cstdint>
#include <deque>

namespace cloud_topics {

/// Estimates total data size of L0 using sparse cumulative checkpointing.
///
/// The level-zero CTP log stores small placeholder entries that point to
/// variable-size data objects in cloud storage. As data is appended
/// and the log is prefix-truncated, we need to estimate how much data is
/// addressable without reading from disk.
///
/// The approach: record the cumulative bytes written at sparse offsets
/// (checkpoints). On query, use linear interpolation between the bracketing
/// checkpoints to estimate bytes at any offset.
///
/// Checkpoint spacing is controlled by a byte threshold (default 50 MiB).
/// The maximum estimation error equals this threshold. Checkpoints adapt
/// to workload changes: a burst of large entries produces denser checkpoints
/// in that offset range.
///
/// This class is serialized as part of the CTP STM snapshot. When the STM
/// restores from a snapshot, log entries before the snapshot offset are not
/// replayed, so the size estimator state must be persisted to avoid losing
/// checkpoints built from those earlier entries.
///
/// Memory: at most max_checkpoints * 16 bytes per log (default 4 KiB).
///
/// Downsampling: To bound memory regardless of workload, checkpoint count is
/// capped at max_checkpoints (default 256, ~4 KiB). When the cap is
/// exceeded, downsample() drops every other checkpoint. This halves the count
/// while preserving the first and last checkpoints, maintaining coverage of
/// the full offset range. Because checkpoint values are cumulative (absolute
/// byte counts, not deltas), the remaining checkpoints need no adjustment --
/// interpolation between any two surviving checkpoints is still correct.
///
/// Crucially, the checkpoint interval is *not* increased on downsample. New
/// checkpoints continue to arrive at the original fine-grained interval,
/// filling the freed half of the array. This produces a natural time-biased
/// decay: recent offsets (the tip of the log) retain full resolution while
/// older regions get exponentially coarser spacing after repeated rounds.
/// This is the right tradeoff because size estimation precision matters most
/// near the last reconciled offset (LRO), which is always at or near the
/// tip.
class size_estimator
  : public serde::
      envelope<size_estimator, serde::version<0>, serde::compat_version<0>> {
public:
    struct checkpoint
      : serde::
          envelope<checkpoint, serde::version<0>, serde::compat_version<0>> {
        model::offset offset;
        uint64_t cumulative_bytes{0};

        auto serde_fields() { return std::tie(offset, cumulative_bytes); }
    };

    static constexpr uint64_t default_checkpoint_interval = 50ULL * 1024 * 1024;

    /// Default cap: 4 KiB / 16 bytes per checkpoint = 256 checkpoints.
    static constexpr size_t default_max_checkpoints = 256;

    size_estimator() = default;
    explicit size_estimator(
      uint64_t checkpoint_interval,
      size_t max_checkpoints = default_max_checkpoints);

    /// Record bytes written at a given log offset.
    ///
    /// Idempotent: offsets at or below the previously recorded offset are
    /// ignored. This supports correct behavior if entries are replayed.
    void record(model::offset offset, uint64_t size_bytes);

    /// Estimate the cumulative bytes written up to and including the given
    /// offset using linear interpolation between checkpoints.
    ///
    /// This is the low-level building block used by estimated_active_bytes()
    /// to compute how many bytes fall below a given watermark. It can also be
    /// useful for diagnostics or for computing sizes over arbitrary offset
    /// ranges.
    ///
    /// Returns 0 if no checkpoints exist or the offset precedes all
    /// checkpoints. Returns total_bytes() if the offset is at or past the
    /// last recorded entry.
    uint64_t estimate_bytes_at(model::offset offset) const;

    /// Estimate the bytes of cloud data that are still active in L0 -- that
    /// is, data above the last reconciled log offset (LRO) that has not yet
    /// been moved to L1.
    ///
    /// This is the primary query interface. ctp_stm::estimated_data_size()
    /// calls it with the LRO as the low watermark, and the result is added
    /// to the L1 metastore size in frontend::size_bytes() to produce the
    /// total partition size reported by the Kafka DescribeLogDirs API.
    ///
    /// Computed as total_bytes() - estimate_bytes_at(low_watermark).
    ///
    /// \param low_watermark log offset at or below which data has been
    ///        reconciled to L1 (typically the last reconciled log offset).
    ///        Pass model::offset{-1} to treat all data as active.
    uint64_t estimated_active_bytes(model::offset low_watermark) const;

    /// Total cumulative bytes ever recorded.
    uint64_t total_bytes() const noexcept { return _total_bytes; }

    /// Remove checkpoints no longer needed for estimates above the given
    /// offset. One checkpoint at or below the offset is retained for
    /// interpolation.
    void gc_below(model::offset offset);

    /// Number of checkpoints stored (for testing and metrics).
    size_t checkpoint_count() const noexcept { return _checkpoints.size(); }

    /// Checkpoint interval in bytes.
    uint64_t checkpoint_interval() const noexcept {
        return _checkpoint_interval;
    }

    fmt::iterator format_to(fmt::iterator it) const;

    auto serde_fields() {
        return std::tie(
          _total_bytes,
          _last_offset,
          _checkpoints,
          _checkpoint_interval,
          _max_checkpoints);
    }

private:
    /// Keep every other checkpoint. Called when checkpoint count exceeds
    /// _max_checkpoints. The first and last checkpoints are always retained.
    void downsample();

    /// Running total of all bytes ever recorded.
    uint64_t _total_bytes{0};

    /// The highest offset for which bytes have been recorded.
    /// Initialized to -1 so that offset 0 is accepted on first use.
    model::offset _last_offset{-1};

    /// Sparse checkpoints mapping offset to cumulative bytes.
    /// Always sorted by offset in ascending order.
    std::deque<checkpoint> _checkpoints;

    /// Byte threshold between consecutive checkpoints.
    uint64_t _checkpoint_interval{default_checkpoint_interval};

    /// Maximum number of checkpoints before downsampling is triggered.
    size_t _max_checkpoints{default_max_checkpoints};
};

} // namespace cloud_topics
