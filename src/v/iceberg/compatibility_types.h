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

#include "base/outcome.h"
#include "base/seastarx.h"

#include <seastar/util/bool_class.hh>

#include <fmt/core.h>

namespace iceberg {

enum class compat_errc {
    mismatch,
};

/**
 * type_promoted - returned from check_types(field_type src, field_type dest)
 * to indicate
 *   a) that src & dest are compatible types
 *   b) whether a field of type src requires type promotion to type dest, as
 *      follows:
 *      - ::no - types are identical if primitive, of same structural type
 *         otherwise
 *      - ::yes - src is promoted to dest and it is always safe to do so
 *      - ::changes_partition - src is promoted to dest but doing so would
 *        change the value of a partition transform function involving that
 *        field.
 */
enum class type_promoted {
    no,
    yes,
    changes_partition,
};

using type_check_result = checked<type_promoted, compat_errc>;

// TODO(oren): possibly a whole error_code?
enum class schema_evolution_errc {
    type_mismatch,
    incompatible,
    ambiguous,
    violates_map_key_invariant,
    new_required_field,
    null_nested_field,
    invalid_state,
    partition_spec_conflict,
};

/**
 * schema_transform_state - Used to keep a running count of added & removed
 * fields _below_ a certain point in the visitor recursion.
 *
 * This is useful for cheaply enforcing map key correctness without having to
 * perform redundant checks on sub-field metadata. Also for communicating binary
 * "was there a structural change" to calling code.
 */
struct schema_transform_state {
    size_t n_removed{0};
    size_t n_added{0};
    size_t n_promoted{0};
    size_t n_removed_partition_fields{0};

    // when we remove a field, it's always counted in n_removed, and _also_
    // counted in n_removed_partition_fields iff it was a partition field.
    // therefore, to avoid double counting, don't include
    // n_removed_partition_fields in the total.
    size_t total() { return n_removed + n_added + n_promoted; }
};

schema_transform_state&
operator+=(schema_transform_state& lhs, const schema_transform_state& rhs);

using schema_errc_result = checked<std::nullopt_t, schema_evolution_errc>;

using schema_transform_result
  = checked<schema_transform_state, schema_evolution_errc>;

using schema_changed = ss::bool_class<struct schema_changed_tag>;
using schema_evolution_result = checked<schema_changed, schema_evolution_errc>;
using ids_filled = ss::bool_class<struct ids_filled_tag>;

} // namespace iceberg

template<>
struct fmt::formatter<iceberg::schema_evolution_errc>
  : formatter<std::string_view> {
    auto format(iceberg::schema_evolution_errc d, format_context& ctx) const
      -> format_context::iterator;
};

template<>
struct fmt::formatter<iceberg::type_promoted> : formatter<std::string_view> {
    auto format(iceberg::type_promoted d, format_context& ctx) const
      -> format_context::iterator;
};
