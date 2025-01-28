/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/transform_utils.h"

#include "iceberg/time_transform_visitor.h"
#include "iceberg/transform.h"

namespace iceberg {

struct transform_applying_visitor {
    explicit transform_applying_visitor(const value& source_val)
      : source_val_(source_val) {}
    const value& source_val_;

    value operator()(const identity_transform&) {
        auto primitive = std::get_if<primitive_value>(&source_val_);
        if (primitive) {
            return make_copy(*primitive);
        }
        throw std::invalid_argument(
          fmt::format("value {} must be primitive", source_val_));
    }

    value operator()(const hour_transform&) {
        int_value v{std::visit(hour_transform_visitor{}, source_val_)};
        return v;
    }

    template<typename T>
    value operator()(const T&) {
        throw std::invalid_argument(
          "transform_applying_visitor not implemented for transform");
    }
};

value apply_transform(const value& source_val, const transform& transform) {
    return std::visit(transform_applying_visitor{source_val}, transform);
}

} // namespace iceberg
