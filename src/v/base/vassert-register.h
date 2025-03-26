/*
 * Copyright 2020 Redpanda Data, Inc.
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

#include <seastar/util/backtrace.hh>

#include <string_view>

namespace base {
// We want to enforce using a non-capturing function for callbacks to ensure
// a static lifetime for the callback as we will not permit unregistering
// a callback to prevent a race condition between unregistering the callback
// on one thread and having the callback called by a different thread.

// A callback function which can be registered.
using assert_cb_func = void (*)(std::string_view);

// Register a callback which will be called after an assert files, but before
// the abort. It receives as its argument the full message of any assert or
// register_event calls.
// Only the first registration has any effect, calls to this function do
// nothing.
// The callback is only called once, only for the first vassert crash. This is
// to avoid a potential infinite loop if the callback triggers another vassert.
void register_cb(assert_cb_func cb);

/**
 * @brief Registers a vassert event
 *
 * @param bt The backtrace from the assert
 * @param message The message to register.
 */
void register_event(const ss::saved_backtrace& bt, std::string_view message);

} // namespace base
