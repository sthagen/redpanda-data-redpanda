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

#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"

#include <fmt/core.h>

#include <optional>

namespace cloud_topics::l1 {

// A contiguous range of offsets in a partition that should be rewritten,
// together with per-range stats the scheduler can use to plan work.
struct levelable_range
  : serde::
      envelope<levelable_range, serde::version<0>, serde::compat_version<0>> {
    bool operator==(const levelable_range&) const = default;

    auto serde_fields() {
        return std::tie(base_offset, last_offset, size_bytes);
    }

    kafka::offset base_offset;
    kafka::offset last_offset;
    // Sum of the undersized extents' bytes within this range.
    size_t size_bytes{0};

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{base:{}, last:{}, size_bytes:{}}}",
          base_offset,
          last_offset,
          size_bytes);
    }
};

// Builds a sequence of `levelable_range`s eligible for leveling from extents
// provided via `process_extent`, returning them from `finalize()`. An extent
// is considered undersized when its length is strictly less than
// `min_acceptable_extent_bytes`.
//
// We only consider rewriting a *run* of consecutive undersized extents.
// Healthy extents close the active run. We never include a healthy extent in
// a leveling range, as those have per-byte rewrite cost without corresponding
// extent-count savings.
//
//   - Undersized extent: extend (or open) the active range.
//   - Healthy extent: close the active range.
//   - On close: commit the range only if it contains more than one
//     extent (K > 1), as singleton runs can't reduce extent count.
class leveling_range_builder {
public:
    explicit leveling_range_builder(size_t min_acceptable_extent_bytes)
      : _min_acceptable_extent_bytes(min_acceptable_extent_bytes) {}

    // Processes a single extent.
    void process_extent(kafka::offset base, kafka::offset last, size_t len) {
        if (len >= _min_acceptable_extent_bytes) {
            // A healthy extent closes any active run.
            maybe_commit_range();
            return;
        }
        if (!_range.has_value()) {
            _range.emplace(base, last, len, 1);
            return;
        }
        _range->last = last;
        _range->bytes += len;
        _range->extent_count += 1;
    }

    // Commit any pending range and return the accumulated ranges. Must
    // be called after all extents have been processed.
    chunked_vector<levelable_range> finalize() && {
        maybe_commit_range();
        return std::move(_ranges);
    }

private:
    struct in_progress_range {
        kafka::offset base;
        kafka::offset last;
        size_t bytes;
        size_t extent_count;
    };

    void maybe_commit_range() {
        if (!_range.has_value()) {
            return;
        }
        if (_range->extent_count > 1) {
            _ranges.push_back(
              levelable_range{
                .base_offset = _range->base,
                .last_offset = _range->last,
                .size_bytes = _range->bytes,
              });
        }
        _range.reset();
    }

    size_t _min_acceptable_extent_bytes;
    std::optional<in_progress_range> _range;
    chunked_vector<levelable_range> _ranges;
};

} // namespace cloud_topics::l1
