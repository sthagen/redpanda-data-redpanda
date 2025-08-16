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
struct truncate_transform_visitor {
    uint32_t length;

    value operator()(const int_value&);
    value operator()(const long_value&);
    value operator()(const decimal_value&);
    value operator()(const string_value&);
    value operator()(const binary_value&);

    value operator()(const auto& value) {
        throw std::invalid_argument(
          fmt::format(
            "value {} must be of type supported by the bucket transform",
            value));
    };
};
} // namespace iceberg
