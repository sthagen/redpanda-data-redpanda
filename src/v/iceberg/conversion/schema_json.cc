/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/schema_json.h"

#include "iceberg/datatypes.h"

namespace {
static constexpr iceberg::nested_field::id_t placeholder_field_id{0};
}

namespace iceberg {

conversion_outcome<iceberg::struct_type>
type_to_iceberg(const json_conversion_ir& ir) {
    auto root = make_copy(ir.root());

    if (auto s = std::get_if<iceberg::struct_type>(&root)) {
        return iceberg::struct_type(std::move(*s));
    } else {
        iceberg::struct_type ret;

        ret.fields.push_back(
          iceberg::nested_field::create(
            placeholder_field_id,
            "root",
            iceberg::field_required::no,
            std::move(root)));

        return ret;
    }
}

} // namespace iceberg
