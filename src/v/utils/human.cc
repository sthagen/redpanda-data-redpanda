// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/human.h"

namespace human {

fmt::iterator latency::format_to(fmt::iterator it) const {
    constexpr static std::array units{"μs", "ms", "secs"};
    static constexpr double step = 1000;
    double x = value;
    for (const char* unit : units) {
        if (x <= step) {
            return fmt::format_to(it, "{:03.3f}{}", x, unit);
        }
        x /= step;
    }
    return fmt::format_to(it, "{}unknown_units", x);
}

fmt::iterator bytes::format_to(fmt::iterator it) const {
    constexpr static std::array units = {
      "bytes", "KiB", "MiB", "GiB", "TiB", "PiB"};
    static constexpr double step = 1024;
    double x = value;
    for (auto& unit : units) {
        if (x <= step) {
            return fmt::format_to(it, "{:03.3f}{}", x, unit);
        }
        x /= step;
    }
    return fmt::format_to(it, "{}unknown_units", x);
}

} // namespace human
