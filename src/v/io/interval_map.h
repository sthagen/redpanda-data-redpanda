/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "absl/container/btree_map.h"

#include <concepts>
#include <utility>

namespace experimental::io {

/**
 * A container that maps intervals to values.
 *
 * The interval_map holds non-overlapping, non-empty, open intervals, and
 * associates each interval with a given value. For example:
 *
 *     [0000, 4096) -> Page0
 *     [4096, 8192) -> Page1
 *
 * Non-empty intervals are intervals in which the length is greater than zero.
 * Non-overlapping means that intervals must be fully disjoint.
 *
 *     OK: [0, 10)
 *     OK: [0, 10) [10, 20)
 *     OK: [0, 10) [20, 30)
 *    BAD: [0, 10) [5, N)
 *
 * This container only works with integral data types.
 */
template<std::integral T, typename V>
class interval_map {
    struct key {
        T start;
        T end;
    };

    struct compare {
        using is_transparent = void;

        bool operator()(const key& a, const key& b) const {
            return a.start < b.start;
        }

        bool operator()(const T& a, const key& b) const { return a < b.start; }
        bool operator()(const key& a, const T& b) const { return a.start < b; }
        bool operator()(const T& a, const T& b) const { return a < b; }
    };

    using map_type = absl::btree_map<key, V, compare>;

public:
    /**
     * Container value iterator.
     */
    using const_iterator = map_type::const_iterator;

    /**
     * An interval [start, start + length).
     */
    struct interval {
        /// The start of the interval.
        T start;
        /// The length of the interval.
        T length;
    };

    /**
     * Insert an interval [start, start+length) and value.
     *
     * If true is returned then the interval was inserted, and the corresponding
     * iterator points to the inserted interval.
     *
     * If false is returned then the interval was not inserted. If insertion
     * failed because the length was zero, then the returned iterator will be
     * equal to end(). Otherwise, the iterator will point at an interval that
     * overlapped with the interval being inserted.
     *
     * Invalidates iterators.
     */
    [[nodiscard]] std::pair<const_iterator, bool>
    insert(interval interval, V value);

    /**
     * Find the interval containing \p index.
     *
     * If no such interval exists then end() is returned.
     */
    [[nodiscard]] const_iterator find(T index) const;

    /**
     * Return an iterator to the first entry in the map.
     *
     * If the map is empty then end() is returned.
     */
    [[nodiscard]] const_iterator begin() const;

    /**
     * Return an iterator to the end of the map.
     */
    [[nodiscard]] const_iterator end() const;

    /**
     * Return true if the map contains no intervals.
     */
    [[nodiscard]] bool empty() const;

    /**
     * Erase the interval pointed to by the iterator \it.
     *
     * Returns an iterator to the next element in the map.
     *
     * Invalidates iterators.
     */
    const_iterator erase(const_iterator it);

    /**
     * Returns the number of elements in the map.
     */
    [[nodiscard]] size_t size() const;

private:
    map_type map_;
};

template<std::integral T, typename V>
std::pair<typename interval_map<T, V>::const_iterator, bool>
interval_map<T, V>::insert(interval interval, V value) {
    const auto length = interval.length;
    if (length <= 0) {
        return {map_.cend(), false};
    }

    const auto start = interval.start;
    const auto end = start + length;

    auto it = map_.lower_bound(start);
    if (it == map_.end()) {
        /*
         * all intervals in the container have starting offsets that are less
         * than the starting offset of the interval being inserted.
         */
        if (map_.empty()) {
            return map_.try_emplace({start, end}, value);
        }

        // checks for overlap with the interval on the left
        it = std::prev(it);
        if (it->first.end > start) {
            return {it, false};
        }

        return {map_.try_emplace(it, {start, end}, value), true};
    }

    // checks for overlap with the interval on the right
    if (end > it->first.start) {
        return {it, false};
    }

    // there are no intervals on the left
    if (it == map_.begin()) {
        return map_.try_emplace({start, end}, value);
    }

    // checks for overlap with the interval on the left
    it = std::prev(it);
    if (it->first.end > start) {
        return {it, false};
    }

    return {map_.try_emplace(it, {start, end}, value), true};
}

template<std::integral T, typename V>
interval_map<T, V>::const_iterator interval_map<T, V>::find(T index) const {
    auto it = map_.lower_bound(index);
    if (it == map_.cend()) {
        if (map_.empty()) {
            return map_.cend();
        }
        it = std::prev(it);

    } else if (it->first.start == index) {
        return it;

    } else if (it == map_.cbegin()) {
        /*
         * equality condition failing before this means that the index is before
         * the first interval in the container.
         */
        return map_.cend();

    } else {
        --it;
    }

    assert(it->first.start < index);
    if (index < it->first.end) {
        return it;
    }

    return map_.cend();
}

template<std::integral T, typename V>
interval_map<T, V>::const_iterator interval_map<T, V>::begin() const {
    return map_.cbegin();
}

template<std::integral T, typename V>
interval_map<T, V>::const_iterator interval_map<T, V>::end() const {
    return map_.cend();
}

template<std::integral T, typename V>
bool interval_map<T, V>::empty() const {
    return map_.empty();
}

template<std::integral T, typename V>
interval_map<T, V>::const_iterator
interval_map<T, V>::erase(interval_map<T, V>::const_iterator it) {
    return map_.erase(it);
}

template<std::integral T, typename V>
size_t interval_map<T, V>::size() const {
    return map_.size();
}

} // namespace experimental::io
