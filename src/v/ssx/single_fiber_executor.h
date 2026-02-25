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
#include "base/vassert.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/weak_ptr.hh>

#include <expected>
#include <functional>
#include <type_traits>
#include <utility>

namespace ssx {

// a function that accepts an ss::abort_source& and returns any ss::future
template<typename Func>
concept abortable_async_fn = requires(Func&& f, ss::abort_source& as) {
    { f(as) } -> ss::Future;
};

/**
 * Single-fiber executor for abortable async functions.
 * - It runs at most one function at a time.
 * - When a new function is submitted all previously submitted functions are
 * interrupted using an ss::abort_source.
 * - Regardless how much progress a previously submitted function has made by
 * then, if its callers did not receive a result yet it will receive
 * errc::interrupted.
 * - An interrupted function may remain running when its caller has received
 * errc::interrupted. The new submitted function only starts working when the
 * old one has finished.
 *
 * For memory safety users should:
 * - either call submit() with rvalue callables holding gate(s) for
 * component(s) used in said callables
 * - or call `co_await executor_instance.drain()` before destructing
 * `executor_instance` to wait for the currently running function to finish.
 *
 * The executor is only compile-time polymorphic: all submitted functions must
 * have the same type. In particular they should either all be lvalue references
 * or all objects. It would be convenient to accept any lambdas of the same
 * signature, but since the implementation stores the function while waiting for
 * the result, it needs to know the exact type. Options to work around it:
 * - use `ss::noncopyable_function` or another type-erased wrapper;
 * - use a custom callable class;
 * - create lambdas with a constexpr function to get their type in compile time.
 *
 * It may seem that there is a lot of excessive moves, and the logic is
 * overcomplicated. It's not exactly the case as there are some hidden race
 * conditions:
 * 1) every time we fulfill a promise its continuation may call submit(),
 * drain(), request_abort() or ~single_fiber_executor();
 * 2) launch() may result in a synchronouse call to complete_running();
 * 3) user-submitted functions may call submit(), drain(), request_abort() or
 * ~single_fiber_executor() as well.
 * Each call to launch() and fulfilling each promise should be treated as a
 * scheduling point.
 */
namespace single_fiber_executor_detail {
enum class errc { interrupted };
}

template<abortable_async_fn Func>
class single_fiber_executor
  : public ss::weakly_referencable<single_fiber_executor<Func>> {
public:
    using errc = single_fiber_executor_detail::errc;

private:
    using self_t = single_fiber_executor<Func>;
    using orig_future_t = decltype(std::declval<Func>()(
      std::declval<ss::abort_source&>()));
    using stored_t = orig_future_t::value_type;
    using ret_value_t = std::conditional_t<
      std::is_same_v<stored_t, ss::internal::monostate>,
      void,
      stored_t>;
    using wrapped_ret_value_t = std::expected<ret_value_t, errc>;
    using promise_t = ss::promise<wrapped_ret_value_t>;
    using future_t = ss::future<wrapped_ret_value_t>;
    using ss::weakly_referencable<single_fiber_executor<Func>>::weak_from_this;

private:
    struct handle_t {
        // invariant: we never store a handle with a fulfilled promise or with a
        // triggered abort source
        promise_t promise;
        const ss::lw_shared_ptr<ss::abort_source> as;

        explicit handle_t(promise_t&& p)
          : promise(std::move(p))
          , as(ss::make_lw_shared<ss::abort_source>()) {}
    };

    struct running_t {
        std::optional<handle_t> handle;

        explicit running_t(promise_t&& promise)
          : handle(std::move(promise)) {}
        void run(Func&& func, ss::weak_ptr<self_t>&& executor) {
            try {
                ssx::background
                  = func(*handle->as)
                      .then_wrapped(
                        [executor = std::move(executor),
                         as = handle->as](orig_future_t&& f) mutable {
                            if (executor) {
                                executor->complete_running(std::move(f));
                            } else {
                                // caller received errc::interrupted, no-one is
                                // interested in the result
                                f.ignore_ready_future();
                            }
                            return ss::now();
                        });
            } catch (...) {
                vassert(executor, "executor is nullptr");
                executor->complete_running(
                  ss::make_exception_future<ret_value_t>(
                    std::current_exception()));
            }
        }
        running_t(const running_t&) = delete;
        running_t& operator=(const running_t&) = delete;
        running_t(running_t&&) = default;
        running_t& operator=(running_t&&) = default;
        ~running_t() { vassert(!handle, "promise not fulfilled"); };
    };

    struct requested_t {
        Func func;
        promise_t promise;

        explicit requested_t(Func&& func)
          : func(std::forward<Func>(func)) {}
    };

    // _running is set iff the background fiber is running
    std::optional<running_t> _running;
    std::optional<requested_t> _pending;

    enum class status { operating, draining, drained };
    status _status{status::operating};

    void launch(promise_t&& promise, Func&& func) {
        vassert(!_running, "launch() called while already running");
        _running.emplace(std::move(promise));
        _running->run(std::forward<Func>(func), weak_from_this());
    }

    void mark_interrupted(std::optional<promise_t>&& p) {
        if (p) {
            p->set_value(wrapped_ret_value_t{std::unexpect, errc::interrupted});
        }
    }

    // returns a promise to mark as interrupted
    std::optional<promise_t> abort_unfulfilled() {
        if (_pending) {
            // if there is a pending function, then _running is already aborted
            // and its promise has been fulfilled
            auto promise = std::move(_pending->promise);
            _pending = std::nullopt;
            return promise;
        }
        if (_running && _running->handle) {
            auto promise = std::move(_running->handle->promise);
            auto as = std::move(_running->handle->as);
            _running->handle = std::nullopt;
            as->request_abort();
            return promise;
        }
        return std::nullopt;
        // if _running is present keep it to indicate the fiber is still running
    }

    void complete_running(orig_future_t&& f) {
        vassert(_running, "no info about the background fiber completed");
        if (_running->handle) {
            auto p = std::move(_running->handle->promise);
            _running->handle = std::nullopt;
            _running = std::nullopt;
            if (f.failed()) [[unlikely]] {
                p.set_exception(std::move(f).get_exception());
            } else {
                if constexpr (std::is_same_v<ret_value_t, void>) {
                    std::move(f).get();
                    p.set_value();
                } else {
                    p.set_value(std::move(f).get());
                }
            }
        } else {
            // caller received errc::interrupted, no-one is
            // interested in the result
            f.ignore_ready_future();
            _running = std::nullopt;
        }

        // _running CAN have a value here, as fulfilling the promise may trigger
        // some continuation which submits a new function
        if (!_running && _pending) {
            auto pending = std::move(*_pending);
            _pending = std::nullopt;
            launch(
              std::move(pending.promise), std::forward<Func>(pending.func));
        }
    }

public:
    ss::future<wrapped_ret_value_t> submit(Func&& func) {
        if (_status != status::operating) {
            return ssx::now(
              wrapped_ret_value_t{std::unexpect, errc::interrupted});
        }
        auto interrupted_promise = abort_unfulfilled();
        std::optional<future_t> f;
        if (_running) {
            vassert(
              _running->handle == std::nullopt,
              "abort_unfulfilled() should have aborted it");
            _pending.emplace(std::forward<Func>(func));
            f.emplace(_pending->promise.get_future());
        } else {
            promise_t promise;
            f.emplace(promise.get_future());
            launch(std::move(promise), std::forward<Func>(func));
        }
        mark_interrupted(std::move(interrupted_promise));
        return std::move(*f);
    }

    void request_abort() { mark_interrupted(abort_unfulfilled()); }

    ss::future<> drain() {
        vassert(_status == status::operating, "drain() called repeatedly");
        _status = status::draining;
        request_abort();
        if (_running) {
            promise_t promise;
            auto future = promise.get_future();
            _running->handle.emplace(std::move(promise));
            co_await future.then_wrapped(
              std::mem_fn(
                &ss::future<wrapped_ret_value_t>::ignore_ready_future));
        }
        _status = status::drained;
    }

    single_fiber_executor() = default;
    single_fiber_executor(const self_t&) = delete;
    single_fiber_executor(self_t&&) = delete;
    self_t& operator=(self_t&&) = delete;
    self_t& operator=(const self_t&) = delete;
    ~single_fiber_executor() {
        vassert(_status != status::draining, "cannot destroy while draining");
        request_abort();
    };
};

/**
 * A helper to create a repeater async callable that uses a retry chain node to
 * determine when to stop repeating.
 *
 * @tparam Func: an `ssx::abortable_async_fn`, i.e. a callable type that
 * accepts `ss::abort_source&` and returns `ss::future<R>` for some type `R`.
 * May be a reference.
 * @tparam StopCondition: a callable type that accepts `R` and returns
 * `ss::stop_iteration`. It determines whether to stop repeating based on the
 * result of the last invocation of `Func`. May be a reference.
 * @tparam Rcn: the retry chain node type. Defaults to `retry_chain_node`
 * (i.e. `basic_retry_chain_node<ss::lowres_clock>`) via the deduction guide.
 * Use `basic_retry_chain_node<ss::manual_clock>` in tests.
 * @tparam RcnArgs: arguments to construct the Rcn.
 * Usually it's either an `Rcn *` to create a child node attached to a parent,
 * or some other arguments like timeouts to construct a root node.
 * @returns: an `ssx::abortable_async_fn` that repeats calling `Func` until
 * `StopCondition` returns `ss::stop_iteration::yes` or the retry chain node
 * stops allowing retries.
 *
 * RCN's abort source triggers the abort source passed to func, but not the
 * other way round. Callers can always capture parent RCN's abort source if
 * they want the whole chain to stop if the function is aborted by its local
 * abort source.
 */
template<abortable_async_fn Func, typename StopCondition, typename Rcn>
class basic_repeater_with_rcn {
public:
    template<typename... RcnArgs>
    constexpr basic_repeater_with_rcn(
      Func&& func, StopCondition&& stop, RcnArgs&&... rcn_args)
      : _func(std::forward<Func>(func))
      , _stop(std::forward<StopCondition>(stop))
      , _rcn(std::forward<RcnArgs>(rcn_args)...) {}
    using future_t = decltype(std::declval<Func>()(
      std::declval<ss::abort_source&>()));

    future_t operator()(ss::abort_source& as) {
        auto permit = _rcn.retry();
        if (!permit.is_allowed) {
            co_return typename future_t::value_type{};
        }
        auto subscription = _rcn.root_abort_source().subscribe(
          [&as] noexcept { as.request_abort(); });
        while (true) {
            auto r = co_await _func(as);
            if (_stop(r) == ss::stop_iteration::yes) {
                co_return r;
            }
            auto permit = _rcn.retry();
            if (!permit.is_allowed) {
                co_return r;
            }
            // Using local abort source. If RCN's abort source is triggered,
            // it will trigger the local one too.
            co_await ss::sleep_abortable<typename Rcn::clock>(permit.delay, as);
        }
    }

private:
    Func _func;
    StopCondition _stop;
    Rcn _rcn;
};

namespace detail {
template<typename T>
using remove_rvalue_reference = std::
  conditional_t<std::is_rvalue_reference_v<T>, std::remove_reference_t<T>, T>;
}

// Deduction guide to preserve value categories for functions.
// If `basic_repeater_with_rcn` was a function and `_func` and `_stop` were
// local variables it would happen automatically. But it would be 5 nested
// lambdas this way.
template<
  abortable_async_fn Func,
  typename StopCondition,
  typename Rcn = retry_chain_node,
  typename... RcnArgs>
basic_repeater_with_rcn(Func&&, StopCondition&&, RcnArgs&&...)
  -> basic_repeater_with_rcn<
    detail::remove_rvalue_reference<Func&&>,
    detail::remove_rvalue_reference<StopCondition&&>,
    Rcn>;

template<abortable_async_fn Func, typename StopCondition>
using repeater_with_rcn
  = basic_repeater_with_rcn<Func, StopCondition, retry_chain_node>;

} // namespace ssx
