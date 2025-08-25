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

#pragma once

#include "model/namespace.h"

namespace cluster {

// contain rules for topic operations in an easy-to-find place
class topic_rules {
public:
    static inline bool can_be_disabled(model::topic_namespace_view tp_ns) {
        return model::is_user_topic(tp_ns) || (tp_ns == model::tx_manager_nt);
    }
};

} // namespace cluster
