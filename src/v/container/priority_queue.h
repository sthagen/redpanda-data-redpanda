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

#include "base/vassert.h"
#include "container/chunked_vector.h"
#include "ssx/async_algorithm.h"

#include <algorithm>
#include <ranges>

namespace detail {

template<typename Comp>
struct invert_comparator {
    template<typename T, typename U>
    constexpr bool operator()(T&& a, U&& b) const {
        return _underlying(std::forward<U>(b), std::forward<T>(a));
    }

    [[no_unique_address]] Comp _underlying;
};

/// Base class for priority queue implementations containing common
/// functionality.
///
/// \tparam T The type of elements stored in the priority queue
/// \tparam Container The underlying container type (default: std::vector<T>)
/// \tparam Comp The comparison function type (default: std::ranges::less)
template<
  typename T,
  typename Container = std::vector<T>,
  typename Comp = std::ranges::less>
requires std::same_as<typename Container::value_type, T>
         && std::is_nothrow_move_constructible_v<T>
         && std::is_nothrow_move_assignable_v<T>
class priority_queue_base {
public:
    using container_type = Container;
    using value_compare = Comp;
    using value_type = typename container_type::value_type;
    using reference = typename container_type::reference;
    using const_reference = typename container_type::const_reference;
    using size_type = typename container_type::size_type;

protected:
    /// Protected constructor for derived classes.
    /// \param comp The comparison function to use for ordering elements
    explicit priority_queue_base(Comp comp = {})
      : _comp{std::move(comp)} {}

public:
    /// \brief Checks whether the container adaptor is empty.
    /// \return true if the container adaptor is empty, false otherwise
    bool empty() const noexcept { return _cont.empty(); }

    /// \brief Returns the number of elements.
    /// \return The number of elements in the container
    size_type size() const noexcept { return _cont.size(); }

    /// Returns the underlying container as a heap.
    /// \return The container with heap ordering.
    [[nodiscard]] container_type extract_heap() && noexcept {
        return std::move(_cont);
    }

    /// Returns the underlying container sorted.
    /// \return The container sorted according to the comparison function.
    [[nodiscard]] container_type extract_sorted() && noexcept {
        std::ranges::sort_heap(_cont, _comp);
        return std::move(_cont);
    }

    /// Returns the underlying container sorted.
    /// \return The container sorted according to the comparison function.
    ss::future<container_type> async_extract_sorted() && noexcept {
        auto first = std::ranges::begin(_cont);
        auto last = std::ranges::end(_cont);
        auto distance = last - first;
        co_await ssx::async_while(
          [&distance]() { return distance > 1; },
          [&]() mutable {
              std::ranges::pop_heap(first, last, _comp);
              --last;
              --distance;
          });
        co_return std::move(_cont);
    }

protected:
    static constexpr bool has_reserve = requires(Container& c, size_type n) {
        c.reserve(n);
    };

    /// \brief Reserves storage for at least the specified number of elements
    /// (only available if the underlying container supports reserve).
    /// \param new_cap The new capacity to reserve
    void reserve(size_type new_cap)
    requires has_reserve
    {
        _cont.reserve(new_cap);
    }

    /// \brief Swaps the contents of this queue with another.
    /// \param other The other bounded_priority_queue to swap with.
    void swap(priority_queue_base& other) noexcept(
      std::is_nothrow_swappable_v<container_type>
      && std::is_nothrow_swappable_v<value_compare>) {
        using std::swap;
        swap(_cont, other._cont);
        swap(_comp, other._comp);
    }

    /// \brief Move elements from a range, when safe to do so.
    /// \tparam Range The type of the range to forward.
    /// \param range The range to forward.
    template<std::ranges::sized_range Range>
    decltype(auto) forward_view(auto&& range) noexcept {
        if constexpr (std::ranges::borrowed_range<Range>) {
            return std::forward<Range>(range);
        } else {
            return std::views::as_rvalue(range);
        }
    };

    /// \brief Restores heap property after adding an element to the back.
    /// Should be called after inserting elements into the underlying container.
    void push_heap() { std::ranges::push_heap(_cont, _comp); }

    /// \brief Restores heap property before removing the top element.
    /// Should be called before removing elements from the underlying container.
    void pop_heap() { std::ranges::pop_heap(_cont, _comp); }

    /// \brief Restores heap property after inserting a range.
    /// Should be called after inserting several elements into the underlying
    /// container.
    void make_heap() { std::ranges::make_heap(_cont, _comp); }

    Container _cont;
    Comp _comp;
};

} // namespace detail

/// Unbounded priority queue - drop-in replacement for std::priority_queue with
/// move-semantics.
///
/// This class provides a compatible interface with std::priority_queue while
/// allowing for future extensions and optimizations. It maintains a max-heap
/// by default (largest element at top).
///
/// \tparam T The type of elements stored in the priority queue
/// \tparam Container The underlying container type (default: std::vector<T>)
/// \tparam Comp The comparison function type (default: std::ranges::less)
template<
  typename T,
  typename Container = std::vector<T>,
  typename Comp = std::ranges::less>
requires std::same_as<typename Container::value_type, T>
         && std::is_nothrow_move_constructible_v<T>
         && std::is_nothrow_move_assignable_v<T>
class priority_queue : public detail::priority_queue_base<T, Container, Comp> {
private:
    using base = detail::priority_queue_base<T, Container, Comp>;

public:
    using typename base::const_reference;
    using typename base::container_type;
    using typename base::reference;
    using typename base::size_type;
    using typename base::value_compare;
    using typename base::value_type;

    /// Default constructor - creates an empty priority queue.
    priority_queue() = default;

    /// Constructor with comparison function.
    /// \param comp The comparison function to use for ordering elements
    explicit priority_queue(Comp comp)
      : base{std::move(comp)} {}

    /// \brief Reserves storage for at least the specified number of elements
    /// (only available if the underlying container supports reserve).
    /// \param new_cap The new capacity to reserve
    void reserve(size_type new_cap)
    requires base::has_reserve
    {
        return base::reserve(new_cap);
    }

    /// \brief Accesses the top element.
    /// \pre !empty()
    /// \return Reference to the top element
    const_reference top() const noexcept {
        vassert(!base::empty(), "top() called on empty priority queue");
        return base::_cont.front();
    }

    /// \brief Removes and returns the top element
    /// \pre !empty()
    /// \return The top element.
    [[nodiscard]] value_type pop() noexcept {
        vassert(!base::empty(), "pop() called on empty priority queue");
        base::pop_heap();
        value_type val = std::move(base::_cont.back());
        base::_cont.pop_back();
        return val;
    }

    /// \brief Insert an element.
    /// \param val The element to insert
    template<typename U>
    requires std::same_as<std::remove_cvref_t<U>, value_type>
    void push(U&& val) {
        base::_cont.emplace_back(std::forward<U>(val));
        base::push_heap();
    }

    /// \brief Inserts a range of elements.
    /// \tparam Range The type of range to insert.
    /// \param range The range of elements to insert.
    template<std::ranges::sized_range Range>
    requires std::is_nothrow_move_constructible_v<value_type>
    void push_range(Range&& range) {
        auto&& forwarded = base::template forward_view<Range>(range);

        const size_type input_size = std::ranges::size(forwarded);

        if constexpr (base::has_reserve) {
            reserve(base::size() + input_size);
        }

        if (base::empty()) {
            for (auto&& val : forwarded) {
                base::_cont.emplace_back(std::forward<decltype(val)>(val));
            }

            if (input_size == 1) {
                base::push_heap();
            } else if (input_size > 1) {
                base::make_heap();
            }
        } else {
            for (auto&& val : forwarded) {
                push(std::forward<decltype(val)>(val));
            }
        }
    }

    /// \brief Inserts a range of elements..
    /// \tparam Range The type of range to insert.
    /// \param range The range of elements to insert.
    template<std::ranges::sized_range Range>
    requires std::is_nothrow_move_assignable_v<value_type>
    ss::future<> async_push_range(Range&& range) {
        if constexpr (base::has_reserve) {
            reserve(base::size() + std::ranges::size(range));
        }

        co_await ssx::async_for_each(
          base::template forward_view<Range>(range),
          [this]<typename V>(V&& val) { this->push(std::forward<V>(val)); });
    }

    /// \brief Swaps the contents of this queue with another.
    /// \param other The other priority_queue to swap with.
    using base::swap;
};

/// \brief The bounded priority queue is a container that maintains the top-k of
/// inserted elements.
///
/// It provides logarithmic insertion.
/// top() and pop() are not provided, since it would require O(n) time, instead,
/// use extract_heap(), or extract_sorted().
template<
  typename T,
  typename Container = std::vector<T>,
  typename Comp = std::ranges::less>
requires std::same_as<typename Container::value_type, T>
         && std::is_nothrow_move_constructible_v<T>
         && std::is_nothrow_move_assignable_v<T>
class bounded_priority_queue
  : public detail::
      priority_queue_base<T, Container, detail::invert_comparator<Comp>> {
private:
    using base = detail::
      priority_queue_base<T, Container, detail::invert_comparator<Comp>>;

public:
    using typename base::const_reference;
    using typename base::container_type;
    using typename base::reference;
    using typename base::size_type;
    using typename base::value_compare;
    using typename base::value_type;

    /// \brief Constructs a bounded priority queue with the specified capacity
    /// and comparison function.
    /// \param capacity The maximum number of elements the queue can hold.
    /// \param comp The comparison function to use for ordering elements.
    /// Note: Internally uses a min-heap (inverted comparator) for efficient
    /// top-K operations.
    explicit bounded_priority_queue(size_type capacity, Comp comp = {})
      : base{detail::invert_comparator<Comp>{std::move(comp)}}
      , _cap{capacity} {}

    /// \brief Checks whether the container adaptor is full
    bool full() const noexcept { return base::size() == _cap; }

    /// \brief Reserves storage for at least the specified number of elements
    /// (only available if the underlying container supports reserve).
    /// \param cap The new capacity to reserve (bounded to capacity on
    /// construction).
    void reserve(size_type new_cap)
    requires base::has_reserve
    {
        return base::reserve(std::min(_cap, new_cap));
    }

    /// \brief Inserts an element. The worst element may be evicted if the
    /// container is full.
    /// \param val The element to insert.
    /// \return whether the element was inserted.
    template<typename U>
    requires std::same_as<std::remove_cvref_t<U>, value_type>
    bool push(U&& val) {
        if (!full()) {
            base::_cont.push_back(std::forward<U>(val));
            base::push_heap();
            return true;
        }

        // Min-heap: front contains the worst of the K best elements
        if (!comp()(base::_cont.front(), val)) {
            return false; // New element is not better than worst
        }

        // Move worst (front) to back: O(log K)
        base::pop_heap();
        // Replace worst with new element
        base::_cont.back() = std::forward<U>(val);
        // Restore heap
        base::push_heap();

        return true;
    }

    /// \brief Inserts a range of elements. Worse elements may be evicted if the
    /// container is full.
    /// \param range The range of elements to insert.
    /// \return void
    template<std::ranges::sized_range Range>
    requires std::is_nothrow_move_assignable_v<value_type>
    void push_range(Range&& range) {
        if constexpr (base::has_reserve) {
            reserve(base::size() + std::ranges::size(range));
        }

        for (auto&& val : base::template forward_view<Range>(range)) {
            push(std::forward<decltype(val)>(val));
        }
    }

    /// \brief Inserts a range of elements. Worse elements may be evicted if the
    /// container is full.
    /// \param range The range of elements to insert.
    /// \return ss::future<void>
    template<std::ranges::sized_range Range>
    requires std::is_nothrow_move_assignable_v<value_type>
    ss::future<> async_push_range(Range&& range) {
        if constexpr (base::has_reserve) {
            reserve(base::size() + std::ranges::size(range));
        }

        co_await ssx::async_for_each(
          base::template forward_view<Range>(range),
          [this]<typename V>(V&& val) { this->push(std::forward<V>(val)); });
    }

    /// \brief Swaps the contents of this queue with another.
    /// \param other The other bounded_priority_queue to swap with.
    void swap(bounded_priority_queue& other) noexcept(
      std::is_nothrow_swappable_v<container_type>
      && std::is_nothrow_swappable_v<value_compare>
      && std::is_nothrow_swappable_v<Comp>) {
        using std::swap;
        base::swap(other);
        swap(_cap, other._cap);
    }

private:
    auto comp() const noexcept { return base::_comp._underlying; }

    size_type _cap;
};

template<typename T, typename Comp = std::ranges::less>
using chunked_priority_queue = priority_queue<T, chunked_vector<T>, Comp>;

template<typename T, typename Comp = std::ranges::less>
using chunked_bounded_priority_queue
  = bounded_priority_queue<T, chunked_vector<T>, Comp>;
