/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once
#include "iceberg/values.h"

namespace iceberg {
struct bucket_transform_hashing_visitor {
    uint32_t operator()(const int_value&);
    uint32_t operator()(const long_value&);
    uint32_t operator()(const time_value&);
    uint32_t operator()(const date_value&);
    uint32_t operator()(const timestamp_value&);
    uint32_t operator()(const timestamptz_value&);
    uint32_t operator()(const decimal_value&);
    uint32_t operator()(const string_value&);
    uint32_t operator()(const fixed_value&);
    uint32_t operator()(const binary_value&);
    uint32_t operator()(const uuid_value&);

    uint32_t operator()(const auto& value) {
        throw std::invalid_argument(
          fmt::format(
            "value {} must be of type supported by the bucket transform",
            value));
    };
};
} // namespace iceberg
