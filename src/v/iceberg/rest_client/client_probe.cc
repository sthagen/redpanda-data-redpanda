/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/rest_client/client_probe.h"

#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

namespace iceberg::rest_client {

namespace {
const auto group_name = prometheus_sanitize::metrics_name(
  "iceberg:rest_client");
} // namespace

client_probe::client_probe(
  net::public_metrics_disabled public_disable,
  ss::metrics::label_instance label)
  : http::client_probe() {
    setup_public_metrics(public_disable, std::move(label));
}

void client_probe::setup_public_metrics(
  net::public_metrics_disabled disable, ss::metrics::label_instance label) {
    namespace sm = ss::metrics;
    if (disable) {
        return;
    }

    std::vector<sm::label_instance> labels;
    labels.emplace_back(std::move(label));
    _public_metrics.add_group(
      group_name,
      {
        sm::make_counter(
          "total_puts",
          [this] { return get_total_put_requests(); },
          sm::description("Number of completed PUT requests"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "total_gets",
          [this] { return get_total_get_requests(); },
          sm::description("Number of completed GET requests"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "total_requests",
          [this] { return get_total_requests(); },
          sm::description(
            "Number of completed HTTP requests (includes PUT and GET)"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "active_puts",
          [this] { return get_active_put_requests(); },
          sm::description("Number of active PUT requests at the moment"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "active_gets",
          [this] { return get_active_get_requests(); },
          sm::description("Number of active GET requests at the moment"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "active_requests",
          [this] { return get_active_requests(); },
          sm::description(
            "Number of active HTTP requests at the moment "
            "(includes PUT and GET)"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "total_inbound_bytes",
          [this] { return get_inbound_bytes(); },
          sm::description(
            "Total number of bytes received from the Iceberg REST catalog"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "total_outbound_bytes",
          [this] { return get_outbound_bytes(); },
          sm::description(
            "Total number of bytes sent to the Iceberg REST catalog"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_transport_errors",
          [this] { return get_transport_errors(); },
          sm::description("Total number of transport errors (TCP and TLS)"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_request_timeouts",
          [this] { return num_request_timeouts; },
          sm::description(
            "Total number of catalog requests that could no longer be retried "
            "because they timed out. This may occur if the catalog is down"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_oauth_token_requests",
          [this] { return num_oauth_token_requests; },
          sm::description(
            "Total number of requests sent to the oauth_token endpoint"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_oauth_token_requests_failed",
          [this] { return num_oauth_token_requests_failed; },
          sm::description(
            "Number of requests sent to the oauth_token endpoint that failed"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_create_namespace_requests",
          [this] { return num_create_namespace_requests; },
          sm::description(
            "Total number of requests sent to the create_namespace endpoint"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_create_namespace_requests_failed",
          [this] { return num_create_namespace_requests_failed; },
          sm::description(
            "Number of requests sent to the create_namespace "
            "endpoint that failed"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_create_table_requests",
          [this] { return num_create_table_requests; },
          sm::description(
            "Total number of requests sent to the create_table endpoint"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_create_table_requests_failed",
          [this] { return num_create_table_requests_failed; },
          sm::description(
            "Number of requests sent to the create_table endpoint that failed"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_load_table_requests",
          [this] { return num_load_table_requests; },
          sm::description(
            "Total number of requests sent to the load_table endpoint"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_load_table_requests_failed",
          [this] { return num_load_table_requests_failed; },
          sm::description(
            "Number of requests sent to the load_table endpoint that failed"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_drop_table_requests",
          [this] { return num_drop_table_requests; },
          sm::description(
            "Total number of requests sent to the drop_table endpoint"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_drop_table_requests_failed",
          [this] { return num_drop_table_requests_failed; },
          sm::description(
            "Number of requests sent to the drop_table endpoint that failed"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_commit_table_update_requests",
          [this] { return num_commit_table_update_requests; },
          sm::description(
            "Total number of requests sent to the "
            "commit_table_update endpoint"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_commit_table_update_requests_failed",
          [this] { return num_commit_table_update_requests_failed; },
          sm::description(
            "Number of requests sent to the commit_table_update "
            "endpoint that failed"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_get_config_requests",
          [this] { return num_get_config_requests; },
          sm::description(
            "Total number of requests sent to the "
            "config endpoint"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_get_config_requests_failed",
          [this] { return num_get_config_requests_failed; },
          sm::description(
            "Number of requests sent to the config "
            "endpoint that failed"),
          labels)
          .aggregate({sm::shard_label}),
      });
}

void client_probe::register_request(endpoint e) {
    switch (e) {
        using enum endpoint;
    case oauth_token:
        ++num_oauth_token_requests;
        return;
    case create_namespace:
        ++num_create_namespace_requests;
        return;
    case create_table:
        ++num_create_table_requests;
        return;
    case load_table:
        ++num_load_table_requests;
        return;
    case drop_table:
        ++num_drop_table_requests;
        return;
    case commit_table_update:
        ++num_commit_table_update_requests;
        return;
    case get_config:
        ++num_get_config_requests;
        return;
    }
}

void client_probe::register_failed_request(endpoint e) {
    switch (e) {
        using enum endpoint;
    case oauth_token:
        ++num_oauth_token_requests_failed;
        return;
    case create_namespace:
        ++num_create_namespace_requests_failed;
        return;
    case create_table:
        ++num_create_table_requests_failed;
        return;
    case load_table:
        ++num_load_table_requests_failed;
        return;
    case drop_table:
        ++num_drop_table_requests_failed;
        return;
    case commit_table_update:
        ++num_commit_table_update_requests_failed;
        return;
    case get_config:
        ++num_get_config_requests_failed;
        return;
    }
}
} // namespace iceberg::rest_client
