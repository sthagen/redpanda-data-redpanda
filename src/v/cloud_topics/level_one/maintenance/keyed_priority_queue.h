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

#include "base/vassert.h"
#include "container/chunked_hash_map.h"

#include <set>
#include <utility>

namespace cloud_topics::l1 {

/// \brief An addressable max-priority queue: holds at most one entry per key,
/// each carrying a `Value`, and supports updating or removing a key's entry
/// *in place*. Entries are ordered by `Compare` applied to their values, with
/// ties broken by key for a strict total order; `top()` is the key of the
/// greatest entry.
///
/// Operations are O(log N) where N is the number of distinct keys; `_pos`
/// addresses a key's slot in the ordered set without a key-comparison search.
template<typename Key, typename Value, typename Compare>
class keyed_priority_queue {
private:
    struct entry {
        Key key;
        Value value;
    };

    struct entry_compare {
        // `cmp` is a strict weak ordering over `Value`; extend it to a
        // strict total order over `entry`. Entries with equivalent values have
        // ties broken by comparing keys (which is required to ensure that we
        // don't silently drop entries with differing keys just because their
        // values are identical).
        bool operator()(const entry& a, const entry& b) const {
            if (cmp(a.value, b.value)) {
                return true;
            }
            if (cmp(b.value, a.value)) {
                return false;
            }
            return a.key < b.key;
        }
        Compare cmp;
    };
    using set_t = std::set<entry, entry_compare>;

public:
    explicit keyed_priority_queue(Compare cmp)
      : _ordered_entries(entry_compare{std::move(cmp)}) {}

    bool empty() const { return _ordered_entries.empty(); }
    size_t size() const { return _ordered_entries.size(); }
    bool contains(const Key& k) const { return _entry_it_by_key.contains(k); }

    /// Insert `key` with `value`, or update it in place if already present.
    void upsert(const Key& key, Value value) {
        auto [pos, inserted] = _entry_it_by_key.try_emplace(key);
        if (!inserted) {
            // `key` was already present: `pos->second` points at its current
            // (now stale) slot. std::set can't re-key an element in place, so
            // drop the old slot before reinserting at the new value.
            _ordered_entries.erase(pos->second);
        }
        pos->second
          = _ordered_entries.insert(entry{key, std::move(value)}).first;
    }

    /// Remove `key` from `_ordered_entries` and `_key_to_slot`.
    void erase(const Key& key) {
        auto it = _entry_it_by_key.find(key);
        vassert(
          it != _entry_it_by_key.end(),
          "keyed_priority_queue::erase of an absent key");
        _ordered_entries.erase(it->second);
        _entry_it_by_key.erase(it);
    }

    /// The greatest-priority (key, value) pair. Precondition: non-empty.
    std::pair<const Key&, const Value&> top() const {
        vassert(
          !_ordered_entries.empty(),
          "keyed_priority_queue::top on an empty queue");
        const auto& e = *_ordered_entries.rbegin();
        return {e.key, e.value};
    }

    void clear() {
        _ordered_entries.clear();
        _entry_it_by_key.clear();
    }

private:
    set_t _ordered_entries;
    // Maps a key to its slot in `_ordered_entries`. std::set iterators are
    // stable across unrelated inserts/erases, so caching them here is safe.
    chunked_hash_map<Key, typename set_t::iterator> _entry_it_by_key;
};

} // namespace cloud_topics::l1
