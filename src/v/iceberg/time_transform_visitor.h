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

#include "iceberg/values.h"

namespace iceberg {

struct hour_transform_visitor {
    int32_t operator()(const primitive_value& v);

    template<typename T>
    int32_t operator()(const T& t) {
        throw std::invalid_argument(
          fmt::format("hourly_visitor not implemented for value {}", t));
    }
};

} // namespace iceberg
