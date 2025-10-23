/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/admin/services/utils.h"

#include "features/feature_table.h"
#include "serde/protobuf/rpc.h"

namespace admin::utils {

void check_license(const features::feature_table& ft) {
    if (ft.should_sanction()) {
        const auto& license = ft.get_license();
        auto status = [&license]() {
            return !license.has_value()    ? "not present"
                   : license->is_expired() ? "expired"
                                           : "unknown error";
        };
        throw serde::pb::rpc::failed_precondition_exception(
          fmt::format("Invalid license: {}", status()));
    }
}

} // namespace admin::utils
