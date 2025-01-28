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

#include <cstdint>
#include <ostream>
#include <variant>

namespace iceberg {

struct identity_transform {};
struct bucket_transform {
    uint32_t n;
};
struct truncate_transform {
    uint32_t length;
};
struct year_transform {};
struct month_transform {};
struct day_transform {};
struct hour_transform {};
struct void_transform {};

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
