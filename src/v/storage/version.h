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

#include "base/format_to.h"
#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <fmt/format.h>

#include <stdexcept>
#include <string_view>

namespace storage {

enum class record_version_type { v1 };

inline fmt::iterator format_to(record_version_type version, fmt::iterator out) {
    switch (version) {
    case record_version_type::v1:
        return fmt::format_to(out, "v1");
    }
    throw std::runtime_error("Wrong record version");
}

inline record_version_type from_string(std::string_view version) {
    if (version == "v1") {
        return record_version_type::v1;
    }
    throw std::invalid_argument(
      fmt::format("Wrong record version name: {}", version));
}

inline ss::sstring to_string(record_version_type version) {
    return ss::sstring(fmt::to_string(version));
}

} // namespace storage
