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

#include "cluster_link/model/types.h"
#include "kafka/client/cluster.h"
#include "kafka/client/configuration.h"
#include "kafka/protocol/types.h"
#include "ssx/sformat.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <algorithm>
#include <concepts>
#include <expected>

namespace cluster_link {
kafka::client::connection_configuration
metadata_to_kafka_config(const model::metadata&);

/// Converts a CA certificate given as either a file path or an inline value.
net::certificate to_certificate(const model::tls_file_or_value&);

/// Converts a client key/cert pair given as either file paths or inline
/// values. Both must be given in the same form (asserts otherwise -- the
/// admin converter that produces `model::tls_file_or_value` writes key and
/// cert from the same source, so a mismatch is unreachable in practice).
net::key_store to_key_store(
  const model::tls_file_or_value& key, const model::tls_file_or_value& cert);

/// Returns true if `have` includes every bit set in `required`.
template<typename T>
bool has_required_permissions(T have, T required) {
    return (have & required) == required;
}

/// Cluster-level DESCRIBE permission (0x100), required on the source cluster to
/// read security objects (ACLs, roles) for replication.
inline constexpr auto cluster_describe_permission
  = kafka::cluster_authorized_operations{0x100};

/// A Kafka API whose highest supported wire version can be negotiated: a
/// KafkaApi (providing `key`) that also exposes the `max_valid` version this
/// build supports.
template<typename T>
concept NegotiableKafkaApi = kafka::KafkaApi<T> && requires {
    { T::max_valid } -> std::convertible_to<const kafka::api_version&>;
};

/// Negotiates the wire version to use for the Kafka API `ApiT` against the
/// source `cluster`: the highest version both the source and this build
/// support. Error messages use `ApiT::name` (the wire protocol API name). On
/// success returns the negotiated version; on failure returns an error string
/// suitable for a task state_transition reason. Rethrows
/// ss::abort_requested_exception so callers can propagate aborts.
template<NegotiableKafkaApi ApiT>
ss::future<std::expected<kafka::api_version, ss::sstring>>
negotiate_api_version(kafka::client::cluster& cluster, ss::abort_source& as) {
    try {
        auto supported_api_versions = co_await cluster.supported_api_versions(
          ApiT::key, as);
        if (!supported_api_versions.has_value()) {
            co_return std::unexpected(
              ssx::sformat(
                "Failed to get supported API version for {}", ApiT::name));
        }
        if (supported_api_versions->min > ApiT::max_valid) {
            co_return std::unexpected(
              ssx::sformat(
                "Unsupported API version for {}: {}",
                ApiT::name,
                supported_api_versions->min));
        }
        co_return std::min(supported_api_versions->max, ApiT::max_valid);
    } catch (const ss::abort_requested_exception&) {
        // Rethrow abort requested to allow caller to handle it
        throw;
    } catch (const std::exception& e) {
        co_return std::unexpected(
          ssx::sformat(
            "Failed to get supported API version for {}: {}",
            ApiT::name,
            e.what()));
    }
}

} // namespace cluster_link
