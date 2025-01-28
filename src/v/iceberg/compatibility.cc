/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/compatibility.h"

#include "iceberg/compatibility_utils.h"
#include "iceberg/datatypes.h"

#include <ranges>
#include <variant>

namespace iceberg {

namespace {
struct primitive_type_promotion_policy_visitor {
    template<typename T, typename U>
    requires(!std::is_same_v<T, U>)
    type_check_result operator()(const T&, const U&) const {
        return compat_errc::mismatch;
    }

    template<typename T>
    type_check_result operator()(const T&, const T&) const {
        return type_promoted::no;
    }

    type_check_result
    operator()(const iceberg::int_type&, const iceberg::long_type&) const {
        return type_promoted::yes;
    }

    type_check_result operator()(
      const iceberg::date_type&, const iceberg::timestamp_type&) const {
        return type_promoted::yes;
    }

    type_check_result
    operator()(const iceberg::float_type&, const iceberg::double_type&) {
        return type_promoted::yes;
    }

    type_check_result operator()(
      const iceberg::decimal_type& src, const iceberg::decimal_type& dst) {
        if (iceberg::primitive_type{src} == iceberg::primitive_type{dst}) {
            return type_promoted::no;
        }
        if ((dst.scale == src.scale && dst.precision > src.precision)) {
            return type_promoted::yes;
        }
        return compat_errc::mismatch;
    }

    type_check_result
    operator()(const iceberg::fixed_type& src, const iceberg::fixed_type& dst) {
        if (iceberg::primitive_type{src} == iceberg::primitive_type{dst}) {
            return type_promoted::no;
        }
        return compat_errc::mismatch;
    }
};

struct field_type_check_visitor {
    explicit field_type_check_visitor(
      primitive_type_promotion_policy_visitor policy)
      : policy(policy) {}

    template<typename T, typename U>
    requires(!std::is_same_v<T, U>)
    type_check_result operator()(const T&, const U&) const {
        return compat_errc::mismatch;
    }

    // For non-primitives, type identity is sufficient to pass this check.
    // e.g. any two struct types will pass w/o indicating type promotion,
    // whereas a struct and, say, a list would produce a mismatch error code.
    // The member fields of two such structs (or the element fields of two
    // lists, k/v for a map, etc.) will be checked elsewhere.
    template<typename T>
    type_check_result operator()(const T&, const T&) const {
        return type_promoted::no;
    }

    type_check_result
    operator()(const primitive_type& src, const primitive_type& dest) {
        return std::visit(policy, src, dest);
    }

private:
    primitive_type_promotion_policy_visitor policy;
};

/**
 * update_field - Field appears in both source and dest schema and the two are
 * _structurally_ compatible. Requiredness & primitive type invariants are
 * checked downstream.
 *
 * Update the destination field metadata with field info about the source field:
 *   - The ID
 *   - The requiredness
 *   - The type of the source field (if primitive)
 *
 * Update the source field metadata to indicate that it was not dropped from the
 * schema.
 */
schema_transform_state
update_field(const nested_field& src, const nested_field& dest) {
    src.set_evolution_metadata(nested_field::removed::no);
    auto src_type = [](const field_type& t) -> std::optional<primitive_type> {
        if (std::holds_alternative<primitive_type>(t)) {
            return std::get<primitive_type>(t);
        }
        return std::nullopt;
    }(src.type);
    dest.set_evolution_metadata(nested_field::src_info{
      .id = src.id,
      .required = src.required,
      .type = src_type,
    });
    return {};
}

/**
 * add_field - This is a brand new field. Mark it as such.
 *
 * Metadata indicates to downstream systems that this field needs a fresh,
 * table-unique ID.
 */
schema_transform_state add_field(const nested_field& f) {
    f.set_evolution_metadata(nested_field::is_new{});
    return {.n_added = 1};
}

/**
 * remove_field - This field (from the source schema) does not appear in the
 * destination schema. Mark it as such.
 */
schema_transform_state remove_field(const nested_field& f) {
    f.set_evolution_metadata(nested_field::removed::yes);
    return {.n_removed = 1};
}

/**
 * annotate_schema_visitor - visit all the fields of two struct_types,
 * recursively, in lockstep.
 *
 * This visitor encodes _structural_ invariants. Type checking, requiredness
 * checking, and (eventually) write/initial default wiring are deferred to a
 * later step (see below).
 */
class annotate_schema_visitor {
public:
    schema_transform_result
    visit(const struct_type& source_t, const struct_type& dest_t) && {
        return std::invoke(*this, source_t, dest_t);
    }

    schema_transform_result operator()(
      const struct_type& source_struct, const struct_type& dest_struct) const {
        schema_transform_state state{};

        for (const auto& f : dest_struct.fields) {
            if (f == nullptr) {
                return schema_evolution_errc::null_nested_field;
            }
            if (auto res = std::invoke(*this, source_struct, *f);
                res.has_error()) {
                return res.error();
            } else {
                state += res.value();
            }
        }

        // for any nested field in the source struct not visited and mapped to
        // the dest struct, recursively mark that field and all nested fields
        // therein for removal. This is helpful for column ID accounting
        // downstream.
        if (auto res = for_each_field(
              source_struct,
              [&state](const nested_field* f) { state += remove_field(*f); },
              [](const nested_field* f) {
                  // visit only those fields that were not already marked (i.e.
                  // mapped correctly into the destination struct). assume that
                  // if a field is marked already then all fields nested under
                  // it are also marked
                  return !f->has_evolution_metadata();
              });
            res.has_error()) {
            return res.error();
        }

        return state;
    }

    schema_transform_result operator()(
      const struct_type& source_parent, const nested_field& dest_field) const {
        // Note that column renaming is NOT supported
        auto matches = source_parent.fields
                       | std::views::filter(
                         [&dest_field](const nested_field_ptr& nf) {
                             return nf != nullptr
                                    && nf->name == dest_field.name;
                         });

        auto match_it = matches.begin();
        auto n_matches = std::distance(match_it, matches.end());

        schema_transform_state state{};

        if (n_matches == 0) {
            // Since this is a new field, we don't have any source schema
            // context to push down into the recursion. Instead, visit the
            // destination field directly, with nesting, calling add_field
            // for each.
            if (auto res = for_each_field(
                  dest_field,
                  [&state](const nested_field* f) { state += add_field(*f); });
                res.has_error()) {
                return res.error();
            }
        } else if (n_matches == 1) {
            const auto& source_field = *match_it;
            if (auto vt_res = std::visit(
                  *this, source_field->type, dest_field.type);
                vt_res.has_error()) {
                return vt_res.error();
            } else {
                state += vt_res.value();
            }
            state += update_field(*source_field, dest_field);
        } else {
            return schema_evolution_errc::ambiguous;
        }

        return state;
    }

    schema_transform_result
    operator()(const list_type& source_list, const list_type& dest_list) const {
        const auto& source_element = source_list.element_field;
        auto& dest_element = dest_list.element_field;

        vassert(
          source_element != nullptr && dest_element != nullptr,
          "List element fields assumed to be non-NULL");

        schema_transform_state state{};

        if (auto ve_res = std::visit(
              *this, source_element->type, dest_element->type);
            ve_res.has_error()) {
            return ve_res.error();
        } else {
            state += ve_res.value();
        }

        state += update_field(*source_element, *dest_element);

        return state;
    }

    schema_transform_result
    operator()(const map_type& source_map, const map_type& dest_map) const {
        const auto& source_key = source_map.key_field;
        auto& dest_key = dest_map.key_field;

        vassert(
          source_key != nullptr && dest_key != nullptr,
          "Map key fields are assumed to be non-NULL");

        schema_transform_state state{};

        if (auto vk_res = std::visit(*this, source_key->type, dest_key->type);
            vk_res.has_error()) {
            return vk_res.error();
        } else if (vk_res.value().total() > 0) {
            return schema_evolution_errc::violates_map_key_invariant;
        }
        state += update_field(*source_key, *dest_key);

        const auto& source_value = source_map.value_field;
        auto& dest_value = dest_map.value_field;

        vassert(
          source_value != nullptr && dest_value != nullptr,
          "Map val fields are assumed to be non-NULL");

        if (auto vv_res = std::visit(
              *this, source_value->type, dest_value->type);
            vv_res.has_error()) {
            return vv_res.error();
        } else {
            state += vv_res.value();
        }

        state += update_field(*source_value, *dest_value);

        return state;
    }

    schema_transform_result
    operator()(const primitive_type&, const primitive_type&) const {
        // this is a leaf by definition, so all we care about is the overload
        // resolving. the underlying primitive types are resolved and checked
        // later

        // unused
        return schema_transform_state{};
    }

    template<typename S, typename D>
    requires(!std::is_same_v<S, D>)
    schema_transform_result operator()(const S&, const D&) const {
        return schema_evolution_errc::incompatible;
    }
};

/**
 * validate_transform_visitor - validate evolution_metadata for some nested
 * field and assign field ID as appropriate. Note new fields and promoted types
 * on the input schema_transform_state.
 *
 * Considerations:
 *   - Adding a required column is allowed by the Iceberg spec but omitted
 *     here. We may implement it in the future.
 *   - Changing an optional column to required is not allowed, per spec.
 *   - The type check on src_info is for primitive types only. The annotation
 *     step checks structural type compatibility.
 *   - Assigning a fresh ID to a new column requires table metadata context,
 *     so that computation is deferred to a subsequent step..
 *   - TODO: support for initial-default and write-default
 */
struct validate_transform_visitor {
    explicit validate_transform_visitor(
      nested_field* f, schema_transform_state& state)
      : f_(f)
      , state_(&state) {}
    schema_errc_result operator()(const nested_field::src_info& src) {
        bool promoted = false;
        if (src.type.has_value()) {
            if (auto ct_res = check_types(src.type.value(), f_->type);
                ct_res.has_error()) {
                return schema_evolution_errc::type_mismatch;
            } else if (ct_res.value() == type_promoted::yes) {
                promoted = true;
            }
        }
        if (
          src.required == field_required::no
          && f_->required == field_required::yes) {
            return schema_evolution_errc::new_required_field;
        } else if (src.required != f_->required) {
            promoted = true;
        }
        *state_ += schema_transform_state{.n_promoted = promoted ? 1ul : 0ul};
        f_->id = src.id;
        return std::nullopt;
    }

    schema_errc_result operator()(const nested_field::is_new&) {
        if (f_->required == field_required::yes) {
            return schema_evolution_errc::new_required_field;
        }
        *state_ += schema_transform_state{.n_added = 1};
        return std::nullopt;
    }

    template<typename T>
    schema_errc_result operator()(const T&) {
        return schema_evolution_errc::invalid_state;
    }

private:
    nested_field* f_;
    schema_transform_state* state_;
};

} // namespace

type_check_result
check_types(const iceberg::field_type& src, const iceberg::field_type& dest) {
    return std::visit(
      field_type_check_visitor{primitive_type_promotion_policy_visitor{}},
      src,
      dest);
}

schema_transform_result
annotate_schema_transform(const struct_type& source, const struct_type& dest) {
    return annotate_schema_visitor{}.visit(source, dest);
}

schema_transform_result validate_schema_transform(struct_type& dest) {
    schema_transform_state state{};
    if (auto res = for_each_field(
          dest,
          [&state](nested_field* f) {
              vassert(
                f->has_evolution_metadata(),
                "Should have visited every destination field");
              return std::visit(validate_transform_visitor{f, state}, f->meta);
          });
        res.has_error()) {
        return res.error();
    }
    return state;
}

schema_evolution_result
evolve_schema(const struct_type& source, struct_type& dest) {
    bool any_change = false;
    if (auto annotate_res = annotate_schema_transform(source, dest);
        annotate_res.has_error()) {
        return annotate_res.error();
    } else {
        any_change = any_change || annotate_res.value().total() > 0;
    }

    if (auto validate_res = validate_schema_transform(dest);
        validate_res.has_error()) {
        return validate_res.error();
    } else {
        any_change = any_change || validate_res.value().total() > 0;
    }
    return schema_changed{any_change};
}

} // namespace iceberg
