/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/format_to.h"

#include <cstdint>

namespace cloud_io {

enum class [[nodiscard]] download_result : int32_t {
    success,
    notfound,
    timedout,
    failed,
};

inline fmt::iterator format_to(download_result r, fmt::iterator out) {
    switch (r) {
    case download_result::success:
        return fmt::format_to(out, "{{success}}");
    case download_result::notfound:
        return fmt::format_to(out, "{{key_not_found}}");
    case download_result::timedout:
        return fmt::format_to(out, "{{timed_out}}");
    case download_result::failed:
        return fmt::format_to(out, "{{failed}}");
    }
}

enum class [[nodiscard]] upload_result : int32_t {
    success,
    timedout,
    failed,
    cancelled,
};

inline fmt::iterator format_to(upload_result r, fmt::iterator out) {
    switch (r) {
    case upload_result::success:
        return fmt::format_to(out, "{{success}}");
    case upload_result::timedout:
        return fmt::format_to(out, "{{timed_out}}");
    case upload_result::failed:
        return fmt::format_to(out, "{{failed}}");
    case upload_result::cancelled:
        return fmt::format_to(out, "{{cancelled}}");
    }
}

} // namespace cloud_io
