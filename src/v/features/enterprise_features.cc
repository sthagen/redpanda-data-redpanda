/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "enterprise_features.h"

#include "base/vassert.h"

namespace features {

void enterprise_feature_report::set(
  license_required_feature feat, bool enabled) {
    auto insert = [feat](vtype& dest, const vtype& other) {
        vassert(
          !other.contains(feat),
          "feature {{{}}} cannot be both enabled and disabled",
          feat);
        dest.insert(feat);
    };

    if (enabled) {
        insert(_enabled, _disabled);
    } else {
        insert(_disabled, _enabled);
    }
}

bool enterprise_feature_report::test(license_required_feature feat) {
    auto en = _enabled.contains(feat);
    auto di = _disabled.contains(feat);
    vassert(
      en != di, "Enterprise features should be either enabled xor disabled");
    return en;
}

} // namespace features
