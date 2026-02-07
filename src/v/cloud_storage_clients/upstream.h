/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_roles/apply_credentials.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/credential_manager.h"
#include "cloud_storage_clients/upstream_key.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/weak_ptr.hh>

namespace cloud_storage_clients {

class upstream_self_configuration_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/// A cloud storage upstream/client factory.
class upstream
  : public ss::weakly_referencable<upstream>
  , public ss::peering_sharded_service<upstream> {
public:
    using client_ptr = ss::shared_ptr<client>;

    explicit upstream(
      upstream_key key,
      client_configuration config,
      ss::shared_ptr<ss::tls::certificate_credentials> tls_credentials,
      ss::shared_ptr<client_probe> probe);

    /// \defgroup Lifecycle
    /// @{
    ss::future<> start();
    ss::future<> stop();
    void prepare_stop();
    /// @}

    const upstream_key& key() const noexcept { return _key; }

    /// Create a client using the provided abort source.
    ///
    /// Client lifetime is decoupled from the upstream so that clients can
    /// complete in-flight operations even if the upstream is evicted from the
    /// registry.
    ///
    /// A client that outlives the upstream can finish in-flight operations but
    /// new ones might fail i.e. because of outdated credentials.
    /// client::is_valid can be used to check if the client is still valid for
    /// new operations.
    ///
    /// Abort source must outlive the client. Abort status is checked
    /// asynchronously.
    client_ptr make_client(ss::abort_source& client_as) noexcept;

    void maybe_refresh_credentials();
    uint64_t token_refresh_count() const noexcept;

    /// Performs the dual functions of loading refreshed credentials into
    /// apply_credentials object, as well as initializing the client pool
    /// the first time this function is called.
    void load_credentials(cloud_roles::credentials credentials);

private:
    ss::future<> client_self_configure();
    ss::future<
      std::optional<cloud_storage_clients::client_self_configuration_output>>
    do_client_self_configure(client_ptr client);
    ss::future<> accept_self_configure_result(
      std::optional<client_self_configuration_output> result);

    ss::future<> wait_for_credentials();

    ss::abort_source _as;
    ss::gate _gate;

    upstream_key _key;

    client_configuration _config;
    net::base_transport::configuration _transport_config;

    ss::shared_ptr<client_probe> _probe;

    /// Holds and applies the credentials for requests to S3. Shared pointer to
    /// enable rotating credentials to all clients.
    ss::lw_shared_ptr<cloud_roles::apply_credentials> _apply_credentials;
    ss::condition_variable _credentials_var;

    credential_manager _credential_manager;

    ssx::semaphore _self_config_barrier{0, "self_config_barrier"};
};

}; // namespace cloud_storage_clients
