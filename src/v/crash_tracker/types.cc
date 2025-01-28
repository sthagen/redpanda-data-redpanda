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

#include "crash_tracker/types.h"

namespace crash_tracker {

std::ostream& operator<<(std::ostream& os, const crash_description& cd) {
    fmt::print(os, "{}", cd.crash_message.c_str());

    const auto opt_stacktrace = cd.stacktrace.c_str();
    const auto has_stacktrace = strlen(opt_stacktrace) > 0;
    if (has_stacktrace) {
        fmt::print(os, " Backtrace: {}.", opt_stacktrace);
    }

    const auto opt_add_info = cd.addition_info.c_str();
    const auto has_add_info = strlen(opt_add_info) > 0;
    if (has_add_info) {
        fmt::print(os, " {}", opt_add_info);
    }

    return os;
}

bool is_crash_loop_limit_reached(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(eptr);
    } catch (const crash_loop_limit_reached&) {
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace crash_tracker
