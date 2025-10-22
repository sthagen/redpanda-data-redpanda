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

/**
 * Take an Avro node and the corresponding Iceberg field type and produce a
 * label based on the JSON encoding for unions laid out in the Avro spec.
 *
 * See https://avro.apache.org/docs/1.12.0/specification/#json-encoding
 *
 * Treatment of logical types is somewhat subtle. Since the mapping from logical
 * types to Avro base types to iceberg field types is not 1:1, propagating
 * logical type names into union branch struct fields can introduce ambiguity.
 * Additionally, named types may collide w/ logical type names (i.e. there is no
 * rule against it). To avoid dealing with collisions, we map branches w/
 * logical type info to struct fields w/ the base type name.
 *
 * For example a {"type": "int", "logicalType": "time-millis"} maps to 'int'
 * rather than 'time'.
 */
ss::sstring union_branch_label(
  const avro::NodePtr& branch, const iceberg::field_type& branch_type);
} // namespace iceberg
