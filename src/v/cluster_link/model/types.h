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

#include "serde/envelope.h"
#include "serde/rw/variant.h"
#include "utils/named_type.h"
#include "utils/unresolved_address.h"
#include "utils/uuid.h"

#include <seastar/util/bool_class.hh>

namespace cluster_link::model {
/// ID of the panda link - used internally based off of controller offset
using id_t = named_type<int64_t, struct id_tag>;
/// UUID of the panda link - used externally
using uuid_t = named_type<uuid_t, struct uuid_tag>;
/// Name of the panda link
using name_t = named_type<ss::sstring, struct name_tag>;

/**
 * @brief SCRAM credentials to use for authentication
 */
struct scram_credentials
  : serde::
      envelope<scram_credentials, serde::version<0>, serde::compat_version<0>> {
    /// SCRAM username
    ss::sstring username;
    /// SCRAM password
    ss::sstring password;
    /// SASL-SCRAM mechanism to use
    ss::sstring mechanism;

    friend bool operator==(const scram_credentials&, const scram_credentials&)
      = default;
    auto serde_fields() { return std::tie(username, password, mechanism); }
};

/**
 * @brief Represents the settings for connection to a remote cluster
 */
struct connection_config
  : serde::
      envelope<connection_config, serde::version<0>, serde::compat_version<0>> {
    /// List of addresses to bootstrap the connection
    std::vector<net::unresolved_address> bootstrap_servers;
    /// Support authn variants.  Currently only SCRAM but update this to add
    /// support for OIDC or GSSAPI in the future.
    using authn_variant = serde::variant<scram_credentials>;
    /// Authentication configuration for the connection
    std::optional<authn_variant> authn_config;
    /// Path to certificate file to use
    ss::sstring cert_file_path;
    /// Path to key file (when mTLS is in use)
    ss::sstring key_file_path;
    /// Path to the CA file to use
    ss::sstring ca_file_path;

    friend bool operator==(const connection_config&, const connection_config&)
      = default;

    auto serde_fields() {
        return std::tie(
          bootstrap_servers,
          authn_config,
          cert_file_path,
          key_file_path,
          ca_file_path);
    }
};

struct metadata
  : serde::envelope<metadata, serde::version<0>, serde::compat_version<0>> {
    /// Name of the panda link
    name_t name;
    /// Unique identifier for the panda link
    uuid_t uuid;
    /// Connection settings for the panda link
    connection_config connection;
    /// Type to indicate if the panda link is paused
    using paused_t = ss::bool_class<struct paused_tag>;
    /// Flag indicating if the panda link has been paused
    paused_t paused{paused_t::no};

    friend bool operator==(const metadata&, const metadata&) = default;

    auto serde_fields() { return std::tie(name, uuid, connection, paused); }
};
} // namespace cluster_link::model

template<>
struct fmt::formatter<cluster_link::model::scram_credentials>
  : fmt::formatter<string_view> {
    auto
    format(const cluster_link::model::scram_credentials& m, format_context& ctx)
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<
  std::optional<cluster_link::model::connection_config::authn_variant>>
  : fmt::formatter<string_view> {
    auto format(
      const std::optional<
        cluster_link::model::connection_config::authn_variant>& m,
      format_context& ctx) -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::connection_config>
  : fmt::formatter<string_view> {
    auto
    format(const cluster_link::model::connection_config& m, format_context& ctx)
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cluster_link::model::metadata>
  : fmt::formatter<string_view> {
    auto format(const cluster_link::model::metadata& m, format_context& ctx)
      -> decltype(ctx.out());
};
