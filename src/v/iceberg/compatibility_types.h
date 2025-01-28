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

using type_promoted = ss::bool_class<struct type_promoted_tag>;
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
    size_t total() { return n_removed + n_added + n_promoted; }
};

schema_transform_state&
operator+=(schema_transform_state& lhs, const schema_transform_state& rhs);

using schema_errc_result = checked<std::nullopt_t, schema_evolution_errc>;

using schema_transform_result
  = checked<schema_transform_state, schema_evolution_errc>;

using schema_changed = ss::bool_class<struct schema_changed_tag>;
using schema_evolution_result = checked<schema_changed, schema_evolution_errc>;

} // namespace iceberg

template<>
struct fmt::formatter<iceberg::schema_evolution_errc>
  : formatter<std::string_view> {
    auto format(iceberg::schema_evolution_errc d, format_context& ctx) const
      -> format_context::iterator;
};
