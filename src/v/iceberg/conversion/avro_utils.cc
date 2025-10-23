/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/avro_utils.h"

#include "ssx/sformat.h"

#include <seastar/util/variant_utils.hh>

#include <avro/Schema.hh>

namespace iceberg {
std::optional<avro::NodePtr> maybe_flatten_union(const avro::NodePtr& node) {
    if (node->type() != avro::AVRO_UNION || node->leaves() != 2) {
        return std::nullopt;
    }

    // NOTE: Avro union branches must be unique by definition

    auto b0 = node->leafAt(0);
    auto b1 = node->leafAt(1);

    if (b0->type() == avro::AVRO_NULL) {
        return b1;
    }
    if (b1->type() == avro::AVRO_NULL) {
        return b0;
    }

    return std::nullopt;
}

struct branch_label_primitive_type_visitor {
    explicit branch_label_primitive_type_visitor(const avro::NodePtr& branch)
      : branch(branch) {}

    ss::sstring operator()(const iceberg::fixed_type&) {
        return branch->name().simpleName();
    }

    ss::sstring operator()(const iceberg::string_type&) {
        if (branch->type() == avro::AVRO_ENUM) {
            return branch->name().simpleName();
        }
        return ssx::sformat("{}", branch->type());
    }

    ss::sstring operator()(const iceberg::decimal_type&) {
        if (branch->type() == avro::AVRO_FIXED) {
            return branch->name().simpleName();
        }
        return ssx::sformat("{}", branch->type());
    }

    ss::sstring operator()(const auto&) {
        return ssx::sformat("{}", branch->type());
    }

    const avro::NodePtr& branch;
};

struct branch_label_visitor {
    explicit branch_label_visitor(const avro::NodePtr& branch)
      : branch(branch) {}

    ss::sstring operator()(const iceberg::primitive_type& pt) {
        return ss::visit(pt, branch_label_primitive_type_visitor{branch});
    }

    ss::sstring operator()(const iceberg::struct_type&) {
        return branch->name().simpleName();
    }

    ss::sstring operator()(const iceberg::list_type&) {
        return ss::sstring{"array"};
    }

    ss::sstring operator()(const iceberg::map_type&) {
        return ss::sstring{"map"};
    }

    const avro::NodePtr& branch;
};

ss::sstring union_branch_label(
  const avro::NodePtr& branch, const iceberg::field_type& branch_type) {
    return ss::visit(branch_type, branch_label_visitor{branch});
}
} // namespace iceberg
