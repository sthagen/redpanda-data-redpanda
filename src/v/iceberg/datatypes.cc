/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/datatypes.h"

#include <variant>

namespace iceberg {

struct primitive_type_comparison_visitor {
    template<typename T, typename U>
    bool operator()(const T&, const U&) const {
        return false;
    }
    bool operator()(const decimal_type& lhs, const decimal_type& rhs) const {
        return lhs.precision == rhs.precision && lhs.scale == rhs.scale;
    }
    bool operator()(const fixed_type& lhs, const fixed_type& rhs) const {
        return lhs.length == rhs.length;
    }
    template<typename T>
    bool operator()(const T&, const T&) const {
        return true;
    }
};

bool operator==(const primitive_type& lhs, const primitive_type& rhs) {
    return std::visit(primitive_type_comparison_visitor{}, lhs, rhs);
}

struct field_type_comparison_visitor {
    template<typename T, typename U>
    bool operator()(const T&, const U&) const {
        return false;
    }
    bool
    operator()(const primitive_type& lhs, const primitive_type& rhs) const {
        return lhs == rhs;
    }
    bool operator()(const struct_type& lhs, const struct_type& rhs) const {
        return lhs == rhs;
    }
    bool operator()(const list_type& lhs, const list_type& rhs) const {
        return lhs == rhs;
    }
    bool operator()(const map_type& lhs, const map_type& rhs) const {
        return lhs == rhs;
    }
};

bool operator==(const field_type& lhs, const field_type& rhs) {
    return std::visit(field_type_comparison_visitor{}, lhs, rhs);
}
bool operator==(const nested_field& lhs, const nested_field& rhs) {
    return lhs.id == rhs.id && lhs.required == rhs.required
           && lhs.name == rhs.name && lhs.type == rhs.type;
}

namespace {

struct type_copying_visitor {
    field_type operator()(const primitive_type& t) { return make_copy(t); }
    field_type operator()(const struct_type& t) {
        struct_type ret;
        for (const auto& field_ptr : t.fields) {
            ret.fields.emplace_back(field_ptr ? field_ptr->copy() : nullptr);
        }
        return ret;
    }
    field_type operator()(const list_type& t) {
        list_type ret;
        if (t.element_field) {
            ret.element_field = t.element_field->copy();
        }
        return ret;
    }
    field_type operator()(const map_type& t) {
        map_type ret;
        if (t.key_field) {
            ret.key_field = t.key_field->copy();
        }
        if (t.value_field) {
            ret.value_field = t.value_field->copy();
        }
        return ret;
    }
};

ss::sstring format_nested_field_ptr_type(const iceberg::nested_field_ptr& ptr) {
    if (ptr == nullptr) {
        return "null";
    }
    return fmt::format("{}", ptr->type);
}

ss::sstring
format_nested_field_ptr_name_type(const iceberg::nested_field_ptr& ptr) {
    if (ptr == nullptr) {
        return "null";
    }
    return fmt::format(
      "{}:{}<{}{}>",
      ptr->id,
      ptr->name,

      ptr->required == iceberg::field_required::yes ? "" : "?",
      ptr->type);
}

} // namespace

primitive_type make_copy(const primitive_type& type) { return type; }

field_type make_copy(const field_type& type) {
    return std::visit(type_copying_visitor{}, type);
}

fmt::iterator format_struct_type(fmt::iterator it, const struct_type& st) {
    it = fmt::format_to(it, "struct[");
    if (!st.fields.empty()) {
        auto fit = st.fields.begin();
        it = fmt::format_to(it, "{}", format_nested_field_ptr_name_type(*fit));
        ++fit;
        for (; fit != st.fields.end(); ++fit) {
            it = fmt::format_to(
              it, ", {}", format_nested_field_ptr_name_type(*fit));
        }
    }
    return fmt::format_to(it, "]");
}

fmt::iterator format_list_type(fmt::iterator it, const list_type& lt) {
    return fmt::format_to(
      it, "list<{}>", format_nested_field_ptr_type(lt.element_field));
}

fmt::iterator format_map_type(fmt::iterator it, const map_type& mt) {
    return fmt::format_to(
      it,
      "map<{},{}>",
      format_nested_field_ptr_type(mt.key_field),
      format_nested_field_ptr_type(mt.value_field));
}

fmt::iterator format_nested_field(fmt::iterator it, const nested_field& nf) {
    return fmt::format_to(
      it,
      "{{id: {}, name: {}, required: {}, type: {}}}",
      nf.id,
      nf.name,
      nf.required,
      nf.type);
}

fmt::iterator format_primitive_type(fmt::iterator it, const primitive_type& t) {
    return std::visit(
      [it](const auto& v) { return fmt::format_to(it, "{}", v); }, t);
}

fmt::iterator format_field_type(fmt::iterator it, const field_type& t) {
    return std::visit(
      [it](const auto& v) { return fmt::format_to(it, "{}", v); }, t);
}

std::ostream& operator<<(std::ostream& o, const struct_type& st) {
    fmt::print(o, "{}", st);
    return o;
}

std::ostream& operator<<(std::ostream& o, const list_type& lt) {
    fmt::print(o, "{}", lt);
    return o;
}

std::ostream& operator<<(std::ostream& o, const map_type& mt) {
    fmt::print(o, "{}", mt);
    return o;
}

std::ostream& operator<<(std::ostream& o, const nested_field& nf) {
    fmt::print(o, "{}", nf);
    return o;
}

bool operator==(const struct_type& lhs, const struct_type& rhs) {
    if (lhs.fields.size() != rhs.fields.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.fields.size(); i++) {
        bool has_lhs = lhs.fields[i] != nullptr;
        bool has_rhs = rhs.fields[i] != nullptr;
        if (has_lhs != has_rhs) {
            return false;
        }
        if (has_lhs == false) {
            continue;
        }
        if (*lhs.fields[i] != *rhs.fields[i]) {
            return false;
        }
    }
    return true;
}
bool operator==(const list_type& lhs, const list_type& rhs) {
    bool has_lhs = lhs.element_field != nullptr;
    bool has_rhs = rhs.element_field != nullptr;
    if (has_lhs != has_rhs) {
        return false;
    }
    if (!has_lhs) {
        // Both nullptr.
        return true;
    }
    return *lhs.element_field == *rhs.element_field;
}
bool operator==(const map_type& lhs, const map_type& rhs) {
    bool has_key_lhs = lhs.key_field != nullptr;
    bool has_key_rhs = rhs.key_field != nullptr;
    if (has_key_lhs != has_key_rhs) {
        return false;
    }
    bool has_val_lhs = lhs.value_field != nullptr;
    bool has_val_rhs = rhs.value_field != nullptr;
    if (has_val_lhs != has_val_rhs) {
        return false;
    }
    if (has_key_lhs && *lhs.key_field != *rhs.key_field) {
        return false;
    }
    if (has_val_lhs && *lhs.value_field != *rhs.value_field) {
        return false;
    }
    return true;
}

std::ostream& operator<<(std::ostream& o, const primitive_type& t) {
    fmt::print(o, "{}", t);
    return o;
}

std::ostream& operator<<(std::ostream& o, const field_type& t) {
    fmt::print(o, "{}", t);
    return o;
}

struct_type struct_type::copy() const {
    chunked_vector<nested_field_ptr> fields_copy;
    fields_copy.reserve(fields.size());
    for (const auto& f : fields) {
        fields_copy.emplace_back(f->copy());
    }
    return {std::move(fields_copy)};
}

const nested_field* struct_type::find_field_by_name(
  const std::vector<ss::sstring>& nested_name) const {
    const auto* cur_struct_type = this;
    const nested_field* field = nullptr;
    for (const auto& n : nested_name) {
        if (!cur_struct_type) {
            return nullptr;
        }

        for (const auto& f : cur_struct_type->fields) {
            if (f->name == n) {
                field = f.get();
                break;
            }
        }
        if (!field) {
            return nullptr;
        }

        cur_struct_type = std::get_if<struct_type>(&field->type);
    }
    return field;
}

list_type list_type::create(
  int32_t element_id, field_required element_required, field_type element) {
    // NOTE: the element field doesn't have a name. Functionally, the list type
    // is represented as:
    // - element-id
    // - element-type
    // - element-required
    // Despite the missing name though, many Iceberg implementations represent
    // the list with a nested_field.
    return list_type{
      .element_field = nested_field::create(
        element_id, "element", element_required, std::move(element))};
}

map_type map_type::create(
  int32_t key_id,
  field_type key_type,
  int32_t val_id,
  field_required val_req,
  field_type val_type) {
    // NOTE: the keys and values don't have names, and the key is always
    // required. Functionally, a map type is represented as:
    // - key-id
    // - key-type
    // - value-id
    // - value-required
    // - value-type
    // Despite the missing names though, many Iceberg implementations represent
    // the map with two nested_fields.
    return map_type{
      .key_field = nested_field::create(
        key_id, "key", field_required::yes, std::move(key_type)),
      .value_field = nested_field::create(
        val_id, "value", val_req, std::move(val_type)),
    };
}

nested_field_ptr nested_field::copy() const {
    return nested_field::create(id, name, required, make_copy(type), meta);
};

void nested_field::set_evolution_metadata(evolution_metadata v) const {
    vassert(
      !has_evolution_metadata(),
      "Evolution metadata should not be overwritten");
    meta = v;
}

bool nested_field::has_evolution_metadata() const {
    return !std::holds_alternative<std::nullopt_t>(meta);
}

bool nested_field::is_add() const {
    return !has_evolution_metadata() || std::holds_alternative<is_new>(meta);
}
bool nested_field::is_drop() const {
    return std::holds_alternative<removed>(meta) && std::get<removed>(meta);
}

} // namespace iceberg
