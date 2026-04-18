/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/partition.h"

#include "base/vlog.h"
#include "iceberg/logger.h"
#include "iceberg/transform_utils.h"

namespace iceberg {

fmt::iterator partition_field::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{source_id: {}, transform: {}, field_id: {}, name: {}}}",
      source_id,
      transform,
      field_id,
      name);
}

fmt::iterator partition_spec::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{spec_id: {}, fields: {}}}", spec_id, fields);
}

std::optional<partition_spec> partition_spec::resolve(
  const unresolved_partition_spec& spec, const struct_type& schema_type) {
    auto cur_field_id = partition_field::id_t{1000};
    chunked_vector<partition_field> fields;
    for (const auto& field : spec.fields) {
        const auto* source_field = schema_type.find_field_by_name(
          field.source_name);
        if (
          !source_field
          || !std::holds_alternative<primitive_type>(source_field->type)) {
            return std::nullopt;
        }
        const auto res = validate_transform_can_be_applied(
          field.transform, source_field->type);
        if (res.has_error()) {
            vlog(
              log.warn,
              "Error resolving partition spec: {}",
              res.error().what());
            return std::nullopt;
        }

        fields.push_back(
          partition_field{
            .source_id = source_field->id,
            .field_id = cur_field_id,
            .name = field.name,
            .transform = field.transform,
          });
        cur_field_id += 1;
    }

    return partition_spec{
      .spec_id = partition_spec::id_t{0},
      .fields = std::move(fields),
    };
}

const partition_field*
partition_spec::get_field(nested_field::id_t source_id) const {
    auto it = std::ranges::find(fields, source_id, &partition_field::source_id);
    if (it == fields.end()) {
        return nullptr;
    }
    return &(*it);
}

} // namespace iceberg
