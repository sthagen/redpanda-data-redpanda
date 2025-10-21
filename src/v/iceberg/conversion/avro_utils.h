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

#include "iceberg/datatypes.h"

#include <avro/Node.hh>

#include <optional>

namespace iceberg {
/**
 * Since the Avro specification doesn't include a way to express optional
 * fields, users commonly use a UNION typed field with one or two  alternates
 * NULL and the other of the desired type.
 *
 * This is a helper function to detect that pattern during record schema
 * conversion.
 *
 * returns the non-NULL leaf, if present, which can then be converted to a
 * redpanda-native field type. nullopt indicates "the node does not contain an
 * optional".
 */
std::optional<avro::NodePtr> maybe_flatten_union(const avro::NodePtr&);
} // namespace iceberg
