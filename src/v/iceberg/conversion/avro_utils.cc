/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/avro_utils.h"

#include <avro/Schema.hh>

namespace iceberg {
std::optional<avro::NodePtr> maybe_flatten_union(const avro::NodePtr& node) {
    if (node->type() != avro::AVRO_UNION || node->leaves() != 2) {
        return std::nullopt;
    }

    // NOTE: Avro union branches must be unique by definition

    auto b0 = node->leafAt(0);
    auto b1 = node->leafAt(1);

    if (b0->type() == avro::AVRO_NULL) {
        return b1;
    }
    if (b1->type() == avro::AVRO_NULL) {
        return b0;
    }

    return std::nullopt;
}
} // namespace iceberg
