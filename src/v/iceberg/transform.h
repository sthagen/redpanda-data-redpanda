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
#include "utils/fixed_string.h"

#include <cstdint>
#include <variant>

namespace iceberg {

struct identity_transform {
    static constexpr fixed_string key{"identity"};

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "identity");
    }
};
struct bucket_transform {
    static constexpr fixed_string key{"bucket"};
    uint32_t n;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "bucket({})", n);
    }
};
struct truncate_transform {
    static constexpr fixed_string key{"truncate"};
    uint32_t length;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "truncate({})", length);
    }
};
struct year_transform {
    static constexpr fixed_string key{"year"};

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "year");
    }
};
struct month_transform {
    static constexpr fixed_string key{"month"};

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "month");
    }
};
struct day_transform {
    static constexpr fixed_string key{"day"};

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "day");
    }
};
struct hour_transform {
    static constexpr fixed_string key{"hour"};

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "hour");
    }
};
struct void_transform {
    static constexpr fixed_string key{"void"};

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "void");
    }
};

using transform = std::variant<
  identity_transform,
  bucket_transform,
  truncate_transform,
  year_transform,
  month_transform,
  day_transform,
  hour_transform,
  void_transform>;
bool operator==(const transform& lhs, const transform& rhs);
std::ostream& operator<<(std::ostream&, const transform&);

} // namespace iceberg

template<>
struct fmt::formatter<iceberg::transform> {
    constexpr auto parse(fmt::format_parse_context& ctx) const {
        return ctx.begin();
    }
    auto format(const iceberg::transform& t, fmt::format_context& ctx) const {
        return std::visit(
          [&ctx](const auto& v) { return fmt::format_to(ctx.out(), "{}", v); },
          t);
    }
};
