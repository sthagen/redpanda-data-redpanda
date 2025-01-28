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

#include "iceberg/compatibility_types.h"
#include "iceberg/datatypes.h"

namespace iceberg {

/**
   check_types - Performs a basic type check between two Iceberg field types,
   enforcing the Primitive Type Promotion policy laid out in
   https://iceberg.apache.org/spec/#schema-evolution

   - For non-primitive types, checks strict equality - i.e. struct == struct,
     list != map
   - Unwraps and compares input types, returns the result. Does not account for
     nesting.

   @param src  - The type of some field in an existing schema
   @param dest - The type of some field in a new schema, possibly compatible
                 with src.
   @return The result of the type check:
             - type_promoted::yes - if src -> dest is a valid type promotion
               e.g. int -> long
             - type_promoted::no  - if src == dest
             - compat_errc::mismatch - if src != dest but src -> dest is not
               permitted e.g. int -> string or struct -> list
 */
type_check_result
check_types(const iceberg::field_type& src, const iceberg::field_type& dest);

/**
 * annotate_schema_transform - Answers whether a one schema can "evolve" into
 * another, structurally.
 *
 * Traverse the schemas in lockstep, in a recursive depth-first fashion, marking
 * nested fields in one of the following ways:
 *   - for a field from the source schema:
 *     - the field does not exist in the dest schema (i.e. drop column)
 *     - a backwards compatible field exists in the dest schema (update column)
 *   - for a field from the dest schema:
 *     - the field receives its ID from a compatible field in the source schema
 *     - the field does not appear in the source schema (i.e. add column)
 *
 * Considerations:
 *   - This function modifies nested_field metadata, but ID updates are deferred
 *     to validate_schema_transform (see below). This is mainly to allow
 *     constness throughout the traversal, ensuring that we do not make
 *     inadvertent structural changes to either input struct.
 *
 * @param source - the schema we want to evolve _from_ (probably the current
 *                 schema for some table)
 * @param dest   - the proposed schema (probably extracted from some incoming
 *                 record)
 *
 * @return schema_transform_state (indicating success), or an error code
 */
schema_transform_result
annotate_schema_transform(const struct_type& source, const struct_type& dest);

/**
 * validate_schema_transform - Finish evaluating backwards compatibility of
 * some schema with another. Makes any required changes to 'dest' and enforces
 * rules around field nullability ("requiredness"").
 *
 * Visit each field in the destination struct again,
 * this time checking nullability invariants and assigning final column IDs to
 * fields mapped from the source structs.
 *
 * Preconditions:
 *   - 'dest' has been fed though 'annotate_schema_transform' already, along
 *     with a source schema.
 *
 * @param dest - the proposed schema (probably extracted from some incoming
 *               record)
 *
 * @return schema_transform_state (indicating success), or an error code
 */
schema_transform_result validate_schema_transform(struct_type& dest);

/**
 * evolve_schema - Prepares dest for insertion into table metadata.
 *
 * Annotate dest with source metadata, evaluate the annotations, and
 * return whether the table schema needs an update.
 *
 * Preconditions:
 *   - Both input structs are un-annotated. That is, none of their
 *     fields have the optional ::meta filled in.
 *
 * Postconditions:
 *   - All fields in dest are either assigned a unique ID carried over
 *     from source, or are marked as "new" (i.e. needing a unique ID
 *     assigned before insertion into metadata)
 *   - Each field in source is annotated with metadata indicating whether
 *     it has a compatible counterpart in dest.
 *
 * @param source - The source (i.e original) schema
 * @param dest   - the proposed schema (probably extracted from some incoming
 *                 record)
 *
 * @return Whether the schema changed from source->dest, or an error
 */
schema_evolution_result
evolve_schema(const struct_type& source, struct_type& dest);

} // namespace iceberg
