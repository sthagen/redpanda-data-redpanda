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

#include "cloud_topics/dl_snapshot.h"
#include "cloud_topics/dl_version.h"
#include "cloud_topics/level_zero/stm/ctp_stm_offsets.h"
#include "serde/envelope.h"

namespace experimental::cloud_topics {

class dl_version_monotonic_invariant
  : public serde::envelope<
      dl_version_monotonic_invariant,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    void set_version(dl_version version) noexcept {
        // Greater or equal for `_last_version` is required to handle retries.
        // Greater for `_last_snapshot_version` to avoid mutating an existing
        // snapshot.
        vassert(
          version >= _last_version && version > _last_snapshot_version,
          "Version can't go backwards. Current version: {}, new version: {}, "
          "last snapshot version: {}",
          _last_version,
          version,
          _last_snapshot_version);
        _last_version = version;
    }

    void set_last_snapshot_version(dl_version version) noexcept {
        // Greater or equal is required to handle retries.
        vassert(
          version >= _last_snapshot_version,
          "Snapshot version can't go backwards. Current snapshot version: {}, "
          "new snapshot version: {}",
          _last_snapshot_version,
          version);
        set_version(version);
        _last_snapshot_version = version;
    }

    auto serde_fields() {
        return std::tie(_last_version, _last_snapshot_version);
    }

private:
    dl_version _last_version;
    dl_version _last_snapshot_version;
};

/// In-memory state of the data layout state machine (ctp_stm).
///
/// Separating the state from the state machine allows the state to be
/// checkpointed and restored independently of the state machine.
class ctp_stm_state
  : public serde::
      envelope<ctp_stm_state, serde::version<0>, serde::compat_version<0>> {
    friend class ctp_stm_state_accessor;

public:
    /// Create a handle to a snapshot of the state at the current version.
    /// The snapshot id can be used later to read snapshot contents.
    dl_snapshot_id start_snapshot(dl_version version) noexcept;

    bool snapshot_exists(dl_snapshot_id id) const noexcept;

    /// Snapshot of the state at the given version.
    std::optional<dl_snapshot_payload> read_snapshot(dl_snapshot_id id) const;

    /// Remove all snapshots with version less than the given version.
    void remove_snapshots_before(dl_version last_version_to_keep);

    /// Get collection of offsets maintained by the STM
    ctp_stm_offsets& get_offsets() noexcept;
    const ctp_stm_offsets& get_offsets() const noexcept;

    auto serde_fields() {
        return std::tie(_snapshots, _version_invariant, _offsets);
    }

    /// Create snapshot of the ctp_stm_state for Raft snapshotting mechanism.
    /// The snapshot is just a copy of the ctp_stm_state that contains all
    /// changes introduced at offset 'snapshot_at' and earlier.
    ///
    /// This 'snapshot' shouldn't be confused with the snapshot returned by
    /// the 'read_snapshot' method. The 'dl_snapshot_payload' returned by it
    /// is supposed to be uploaded to the cloud storage and consumed by the
    /// recovery process. The 'ctp_stm_state' instance returned by this method
    /// is supposed to be used to implement 'take_snapshot' method of the
    /// 'ctp_stm' which returns Raft snapshot and is used during partition
    /// movement.
    ctp_stm_state get_state_at(model::offset snapshot_at) const noexcept;

private:
    // A list of snapshot handles that are currently open.
    // The list is ordered by version in ascending order to efficiently find the
    // oldest snapshot when running state garbage collection and to remove
    // closed snapshots.
    std::deque<dl_snapshot_id> _snapshots;

    dl_version_monotonic_invariant _version_invariant;
    ctp_stm_offsets _offsets;
};

}; // namespace experimental::cloud_topics
