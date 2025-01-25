// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/fragmented_vector.h"
#include "iceberg/transform.h"

namespace iceberg {

struct unresolved_partition_spec {
    struct field {
        // Components of the nested source field name, in increasing depth
        // order.
        std::vector<ss::sstring> source_name;
        transform transform;
        ss::sstring name;

        friend bool operator==(const field&, const field&) = default;
        friend std::ostream& operator<<(std::ostream&, const field&);
    };

    chunked_vector<field> fields;

    // Validate if the spec can be used as a default spec. For this the spec can
    // only refer to allowed subfields of the redpanda struct column.
    bool is_valid_for_default_spec() const;

    friend bool operator==(
      const unresolved_partition_spec&, const unresolved_partition_spec&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const unresolved_partition_spec&);
};

} // namespace iceberg
