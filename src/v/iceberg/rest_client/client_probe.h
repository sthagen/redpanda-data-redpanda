/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "http/probe.h"
#include "metrics/metrics.h"
#include "net/types.h"

namespace iceberg::rest_client {

class client_probe : public http::client_probe {
public:
    enum class endpoint {
        oauth_token,
        create_namespace,
        create_table,
        load_table,
        drop_table,
        commit_table_update,
        get_config,
    };
    client_probe(
      net::public_metrics_disabled public_disable, ss::metrics::label_instance);

    void register_request(endpoint e);
    void register_failed_request(endpoint e);
    void register_timeout() { ++num_request_timeouts; }

private:
    void setup_public_metrics(
      net::public_metrics_disabled disable, ss::metrics::label_instance);

    metrics::public_metric_groups _public_metrics;

    size_t num_request_timeouts{0};

    size_t num_oauth_token_requests{0};
    size_t num_oauth_token_requests_failed{0};

    size_t num_create_namespace_requests{0};
    size_t num_create_namespace_requests_failed{0};

    size_t num_create_table_requests{0};
    size_t num_create_table_requests_failed{0};

    size_t num_load_table_requests{0};
    size_t num_load_table_requests_failed{0};

    size_t num_drop_table_requests{0};
    size_t num_drop_table_requests_failed{0};

    size_t num_commit_table_update_requests{0};
    size_t num_commit_table_update_requests_failed{0};

    size_t num_get_config_requests{0};
    size_t num_get_config_requests_failed{0};
};

} // namespace iceberg::rest_client
