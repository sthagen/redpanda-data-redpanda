/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm_state.h"

#include "cloud_topics/dl_snapshot.h"
#include "model/fundamental.h"

#include <algorithm>
#include <iterator>

namespace experimental::cloud_topics {

dl_snapshot_id ctp_stm_state::start_snapshot(dl_version version) noexcept {
    _version_invariant.set_last_snapshot_version(version);

    auto id = dl_snapshot_id(version);
    _snapshots.push_back(id);

    return id;
}

bool ctp_stm_state::snapshot_exists(dl_snapshot_id id) const noexcept {
    return std::binary_search(
      _snapshots.begin(),
      _snapshots.end(),
      id,
      [](const dl_snapshot_id& a, const dl_snapshot_id& b) {
          return a.version < b.version;
      });
}

std::optional<dl_snapshot_payload>
ctp_stm_state::read_snapshot(dl_snapshot_id id) const {
    auto it = std::find_if(
      _snapshots.begin(), _snapshots.end(), [&id](const dl_snapshot_id& s) {
          return s.version == id.version;
      });

    // Snapshot not found.
    if (it == _snapshots.end()) {
        return std::nullopt;
    }

    return dl_snapshot_payload{
      .id = *it,
    };
}

void ctp_stm_state::remove_snapshots_before(dl_version last_version_to_keep) {
    if (_snapshots.empty()) {
        throw std::runtime_error(fmt::format(
          "Attempt to remove snapshots before version {} but no snapshots "
          "exist",
          last_version_to_keep));
    }

    // Find the first snapshot to keep. It is the first snapshot with a version
    // equal or greater than the version to keep.
    auto it = std::lower_bound(
      _snapshots.begin(),
      _snapshots.end(),
      last_version_to_keep,
      [](const dl_snapshot_id& a, dl_version b) { return a.version < b; });

    if (it == _snapshots.begin()) {
        // Short circuit if there are no snapshots to remove
        return;
    } else if (it == _snapshots.end()) {
        throw std::runtime_error(fmt::format(
          "Trying to remove snapshots before an non-existent snapshot",
          last_version_to_keep));
    } else {
        _snapshots.erase(_snapshots.begin(), it);
    }
}

ctp_stm_offsets& ctp_stm_state::get_offsets() noexcept { return _offsets; }

const ctp_stm_offsets& ctp_stm_state::get_offsets() const noexcept {
    return _offsets;
}

ctp_stm_state
ctp_stm_state::get_state_at(model::offset snapshot_at) const noexcept {
    ctp_stm_state result;

    // Copy snapshots
    std::copy_if(
      _snapshots.begin(),
      _snapshots.end(),
      std::back_inserter(result._snapshots),
      [snapshot_at](const dl_snapshot_id& o) noexcept {
          return o.version() <= snapshot_at;
      });

    // Copy version invariant and offsets
    if (!result._snapshots.empty()) {
        // The snapshot versions can't go back
        result._version_invariant.set_last_snapshot_version(
          result._snapshots.back().version);
    }

    result._offsets.advance_last_reconciled_offset(
      _offsets.get_last_reconciled_offset());

    return result;
}

} // namespace experimental::cloud_topics
