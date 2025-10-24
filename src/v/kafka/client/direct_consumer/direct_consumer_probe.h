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

#include "base/seastarx.h"
#include "metrics/metrics.h"

namespace kafka {
namespace client {

struct direct_consumer_probe {
    struct configuration {
        ss::sstring group_name;
        std::vector<ss::metrics::label_instance> labels;
    };

    explicit direct_consumer_probe(configuration);
    direct_consumer_probe(const direct_consumer_probe&) = delete;
    direct_consumer_probe& operator=(const direct_consumer_probe&) = delete;
    direct_consumer_probe(direct_consumer_probe&&) = delete;
    direct_consumer_probe& operator=(direct_consumer_probe&&) = delete;
    ~direct_consumer_probe() = default;

    void setup_metrics(configuration);

    size_t n_fetch_errors;

private:
    metrics::public_metric_groups _public_metrics;
};

} // namespace client
} // namespace kafka
