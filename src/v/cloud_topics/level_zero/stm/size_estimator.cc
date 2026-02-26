/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/size_estimator.h"

#include "base/vassert.h"

#include <algorithm>

namespace cloud_topics {

size_estimator::size_estimator(
  uint64_t checkpoint_interval, size_t max_checkpoints)
  : _checkpoint_interval(checkpoint_interval)
  , _max_checkpoints(max_checkpoints) {}

void size_estimator::record(model::offset offset, uint64_t size_bytes) {
    if (offset <= _last_offset) {
        return;
    }

    _total_bytes += size_bytes;
    _last_offset = offset;

    uint64_t last_cp_bytes = _checkpoints.empty()
                               ? 0
                               : _checkpoints.back().cumulative_bytes;

    if (_total_bytes - last_cp_bytes >= _checkpoint_interval) {
        _checkpoints.push_back(
          checkpoint{.offset = offset, .cumulative_bytes = _total_bytes});
        if (_checkpoints.size() > _max_checkpoints) {
            downsample();
        }
    }
}

uint64_t size_estimator::estimate_bytes_at(model::offset offset) const {
    if (offset >= _last_offset) {
        return _total_bytes;
    }

    if (_checkpoints.empty() || offset < _checkpoints.front().offset) {
        return 0;
    }

    // Offset is at or past the last checkpoint but before _last_offset.
    // Interpolate between last checkpoint and current position.
    if (offset >= _checkpoints.back().offset) {
        const auto& last = _checkpoints.back();
        if (_last_offset == last.offset) {
            return last.cumulative_bytes;
        }
        auto range = static_cast<double>(_last_offset() - last.offset());
        auto pos = static_cast<double>(offset() - last.offset());
        auto bytes_in_range = static_cast<double>(
          _total_bytes - last.cumulative_bytes);
        return last.cumulative_bytes
               + static_cast<uint64_t>(pos / range * bytes_in_range);
    }

    // Find the two checkpoints bracketing the offset via binary search.
    auto it = std::upper_bound(
      _checkpoints.begin(),
      _checkpoints.end(),
      offset,
      [](model::offset off, const checkpoint& cp) { return off < cp.offset; });

    // it points to the first checkpoint with offset > target.
    // (it-1) points to the last checkpoint with offset <= target.
    //
    // Safety: the early returns above guarantee that offset falls strictly
    // between front().offset and back().offset, so upper_bound cannot return
    // begin() (front <= offset) or end() (offset < back).
    vassert(
      it != _checkpoints.end() && it != _checkpoints.begin(),
      "upper_bound returned an unexpected boundary iterator for offset {}",
      offset);
    const auto& right = *it;
    const auto& left = *(it - 1);

    if (left.offset == offset) {
        return left.cumulative_bytes;
    }

    auto range = static_cast<double>(right.offset() - left.offset());
    auto pos = static_cast<double>(offset() - left.offset());
    auto bytes_in_range = static_cast<double>(
      right.cumulative_bytes - left.cumulative_bytes);
    return left.cumulative_bytes
           + static_cast<uint64_t>(pos / range * bytes_in_range);
}

uint64_t
size_estimator::estimated_active_bytes(model::offset low_watermark) const {
    if (_total_bytes == 0) {
        return 0;
    }
    auto dropped = estimate_bytes_at(low_watermark);
    return _total_bytes - dropped;
}

void size_estimator::gc_below(model::offset offset) {
    if (_checkpoints.size() <= 1) {
        return;
    }

    auto it = std::lower_bound(
      _checkpoints.begin(),
      _checkpoints.end(),
      offset,
      [](const checkpoint& cp, model::offset off) { return cp.offset < off; });

    // it points to first checkpoint with offset >= target.
    // Keep one checkpoint below for interpolation.
    if (it == _checkpoints.begin()) {
        return;
    }

    --it; // The checkpoint to retain
    if (it == _checkpoints.begin()) {
        return;
    }

    _checkpoints.erase(_checkpoints.begin(), it);
}

void size_estimator::downsample() {
    std::deque<checkpoint> kept;
    for (size_t i = 0; i < _checkpoints.size(); i += 2) {
        kept.push_back(_checkpoints[i]);
    }
    // Always preserve the last checkpoint.
    if (kept.back().offset != _checkpoints.back().offset) {
        kept.push_back(_checkpoints.back());
    }
    _checkpoints = std::move(kept);
}

fmt::iterator size_estimator::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{total_bytes={}, last_offset={}, checkpoints={}}}",
      _total_bytes,
      _last_offset,
      _checkpoints.size());
}

} // namespace cloud_topics
