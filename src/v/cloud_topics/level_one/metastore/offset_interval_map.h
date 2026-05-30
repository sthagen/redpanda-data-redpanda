/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/vassert.h"
#include "container/chunked_vector.h"
#include "container/interval_map.h"
#include "model/fundamental.h"

#include <utility>

namespace cloud_topics::l1 {

// Wrapper around an interval_map, but with an interface that makes it
// conducive to using inclusive offset ranges. The map analog of
// `offset_interval_set`: it associates a value with each range and, unlike the
// set, does not coalesce adjacent ranges.
template<typename V>
class offset_interval_map {
public:
    using imap_t = interval_map<kafka::offset::type, V>;

    struct interval {
        kafka::offset base_offset;
        kafka::offset last_offset;
        V value;
    };

    class stream {
    public:
        explicit stream(const imap_t& underlying)
          : map_(underlying)
          , iter_(map_.begin())
          , end_(map_.end()) {}

        bool has_next() const noexcept { return iter_ != end_; }
        interval next() {
            vassert(has_next(), "next() called while has_next() is false");
            interval ret{
              .base_offset = kafka::offset(iter_->first.start),
              .last_offset = kafka::offset(iter_->first.end - 1),
              .value = iter_->second,
            };
            ++iter_;
            return ret;
        }

    private:
        const imap_t& map_;
        typename imap_t::const_iterator iter_;
        typename imap_t::const_iterator end_;
    };

    bool empty() const { return map_.empty(); }
    size_t size() const { return map_.size(); }

    // Inserts the inclusive range [base, last] mapped to `value`. Returns true
    // if it was inserted, or false if the range is empty or overlaps a range
    // already present, in which case nothing is inserted.
    bool insert(kafka::offset base, kafka::offset last, V value) {
        auto len = last() - base() + 1;
        return map_
          .insert(typename imap_t::interval{base(), len}, std::move(value))
          .second;
    }

    bool contains(kafka::offset offset) const {
        return map_.find(offset()) != map_.end();
    }

    // Returns whether every offset in the inclusive range [start, last] is
    // present. Because ranges are not coalesced, [start, last] may span
    // several contiguous ranges, so walk them until the range is covered or a
    // gap is found.
    bool covers(kafka::offset start, kafka::offset last) const {
        if (start > last) {
            return false;
        }
        auto next = start();
        while (next <= last()) {
            auto it = map_.find(next);
            if (it == map_.end()) {
                return false;
            }
            // `end` is exclusive, so it is the first offset past this range
            // and the next offset that must be covered.
            next = it->first.end;
        }
        return true;
    }

    stream make_stream() const { return stream(map_); }

    chunked_vector<interval> to_vec() const {
        chunked_vector<interval> ret;
        ret.reserve(map_.size());
        auto s = make_stream();
        while (s.has_next()) {
            ret.emplace_back(s.next());
        }
        return ret;
    }

private:
    imap_t map_;
};

} // namespace cloud_topics::l1
