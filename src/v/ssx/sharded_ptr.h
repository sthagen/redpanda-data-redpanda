/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "base/vassert.h"
#include "utils/mutex.h"

#include <seastar/core/smp.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include <memory>

namespace ssx {

/// A pointer that is safe to share between shards.
///
/// The pointer can be reset only by the home shard; other shards shall not
/// observe the change until the update has run on their reactor.
///
/// As such, it is safe to maintain a pointer or reference to the pointee an any
/// shard until the next yield point.
template<typename T>
class sharded_ptr {
public:
    sharded_ptr()
      : _shard{ss::this_shard_id()} {}
    ~sharded_ptr() noexcept = default;

    sharded_ptr(sharded_ptr&& other) noexcept = default;
    sharded_ptr& operator=(sharded_ptr&&) noexcept = default;

    sharded_ptr(sharded_ptr const&) = delete;
    sharded_ptr& operator=(sharded_ptr const&) = delete;

    /// dereferences pointer to the managed object for the local shard.
    ///
    /// reset must have been called at least once.
    /// stop must not have been called.
    T& operator*() const { return local().operator*(); }

    /// dereferences pointer to the managed object for the local shard.
    ///
    /// reset must have been called at least once.
    /// stop must not have been called.
    T* operator->() const { return local().operator->(); }

    /// checks if there is an associated managed object on the local shard.
    ///
    /// This is safe to call at any time on any shard.
    explicit operator bool() const {
        return _state.size() > ss::this_shard_id() && local() != nullptr;
    }

    /// replaces the managed object.
    ///
    /// Must be called on the home shard and is safe to call consurrently.
    ss::future<> reset(std::shared_ptr<T> u = nullptr) {
        assert_shard();
        auto mu{co_await _mutex.get_units()};
        if (_state.empty()) {
            _state.resize(ss::smp::count);
        }

        co_await ss::smp::invoke_on_all([this, u]() noexcept { local() = u; });
    }

    /// replaces the managed object by constructing a new one.
    ///
    /// Must be called on the home shard and is safe to call concurrently.
    /// returns an ss::broken_semaphore if stop() has been called.
    template<typename... Args>
    ss::future<> reset(Args&&... args) {
        return reset(std::make_shared<T>(std::forward<Args>(args)...));
    }

    /// stop managing any object.
    ///
    /// Must be called on the home shard and is safe to call concurrently.
    /// returns an ss::broken_semaphore if stop() has been called.
    ss::future<> stop() {
        co_await _mutex.with([this] { _mutex.broken(); });
        _state = {};
    }

    /// return the home shard.
    ///
    /// This is safe to call at any time on any shard.
    auto shard_id() const { return _shard; }

    /// get a reference to the local instance
    ///
    /// reset must have been called at least once.
    /// stop must not have been called.
    std::shared_ptr<T> const& local() const {
        return _state[ss::this_shard_id()];
    }

    /// get a reference to the local instance
    ///
    /// reset must have been called at least once.
    /// stop must not have been called.
    std::shared_ptr<T>& local() { return _state[ss::this_shard_id()]; }

private:
    void assert_shard() const {
        vassert(
          ss::this_shard_id() == _shard,
          "reset must be called on home shard: ",
          _shard);
    }
    ss::shard_id _shard;
    std::vector<std::shared_ptr<T>> _state;
    mutex _mutex;
};

} // namespace ssx
