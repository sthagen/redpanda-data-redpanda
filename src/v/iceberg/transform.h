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

#include "utils/fixed_string.h"

#include <cstdint>
#include <ostream>
#include <variant>

namespace iceberg {

struct identity_transform {
    static constexpr fixed_string key{"identity"};
};
struct bucket_transform {
    static constexpr fixed_string key{"bucket"};
    uint32_t n;
};
struct truncate_transform {
    static constexpr fixed_string key{"truncate"};
    uint32_t length;
};
struct year_transform {
    static constexpr fixed_string key{"year"};
};
struct month_transform {
    static constexpr fixed_string key{"month"};
};
struct day_transform {
    static constexpr fixed_string key{"day"};
};
struct hour_transform {
    static constexpr fixed_string key{"hour"};
};
struct void_transform {
    static constexpr fixed_string key{"void"};
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
