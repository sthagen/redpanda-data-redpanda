// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "container/chunked_vector.h"
#include "iceberg/transform.h"

#include <seastar/core/sstring.hh>

namespace iceberg {

struct unresolved_partition_spec {
    using column_reference = std::vector<ss::sstring>;
    struct field {
        // Components of the nested source field name, in increasing depth
        // order.
        column_reference source_name;
        transform transform;
        ss::sstring name;

        void autogenerate_name();

        fmt::iterator format_to(fmt::iterator it) const;

        friend bool operator==(const field&, const field&) = default;
    };

    chunked_vector<field> fields;

    // Validate if the spec can be used as a default spec. For this the spec can
    // only refer to allowed subfields of the redpanda struct column.
    bool is_valid_for_default_spec() const;

    friend bool operator==(
      const unresolved_partition_spec&,
      const unresolved_partition_spec&) = default;

    fmt::iterator format_to(fmt::iterator it) const;

    unresolved_partition_spec copy() const {
        return {
          .fields = fields.copy(),
        };
    }
};

} // namespace iceberg
