// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/fwd.h"
#include "migrations/feature_migrator.h"

namespace features::migrators {

/// topic_id_migrator assigns topic ids (KIP-516) to pre-25.2 topics that were
/// created without a topic id.
class topic_id_migrator final : public feature_migrator {
public:
    explicit topic_id_migrator(cluster::controller& c)
      : feature_migrator(c) {}

private:
    features::feature get_feature() final { return _feature; }
    ss::future<> do_mutate() final;

    features::feature _feature{features::feature::topic_ids};
};

} // namespace features::migrators
