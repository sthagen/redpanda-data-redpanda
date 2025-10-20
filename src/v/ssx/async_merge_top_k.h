/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "container/priority_queue.h"
#include "ssx/async_algorithm.h"

namespace ssx {

/// \brief Async merge top k elements from multiple sorted lists
template<
  std::ranges::random_access_range Rng,
  std::weakly_incrementable OutIt,
  typename Comp = std::ranges::less>
requires std::ranges::forward_range<std::ranges::range_value_t<Rng>>
ss::future<> async_merge_top_k(
  Rng&& sorted_lists, OutIt result, std::size_t k, Comp comp = {}) {
    using list_t = std::ranges::range_value_t<Rng>;
    using iter_t = std::ranges::iterator_t<list_t>;
    using value_t = std::ranges::range_value_t<list_t>;

    // The heap element: (value, list index, iterator)
    struct heap_elem {
        heap_elem(iter_t it, size_t idx)
          : value{&*it}
          , list_idx{idx}
          , it{std::move(it)} {}
        const value_t* value;
        std::size_t list_idx;
        iter_t it;
    };

    auto heap_comp = [&comp](const heap_elem& a, const heap_elem& b) {
        return comp(*a.value, *b.value);
    };
    chunked_priority_queue<heap_elem, decltype(heap_comp)> heap{heap_comp};
    if constexpr (std::ranges::sized_range<Rng>) {
        heap.reserve(std::ranges::size(sorted_lists));
    }

    size_t idx = 0;
    for (auto&& list : sorted_lists) {
        if (!std::ranges::empty(list)) {
            heap.push(heap_elem{std::ranges::begin(list), idx});
        }
        ++idx;
    }

    size_t count{0};

    co_await ssx::async_while(
      [&]() { return !heap.empty() && count < k; },
      [&]() {
          auto top = heap.pop();
          *result = std::move(*top.it);
          ++result;
          ++top.it;
          ++count;
          if (top.it != std::ranges::end(sorted_lists[top.list_idx])) {
              top.value = &(*top.it);
              heap.push(std::move(top));
          }
      });
}

} // namespace ssx
