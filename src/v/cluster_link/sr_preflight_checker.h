/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster_link/errc.h"
#include "cluster_link/model/types.h"
#include "schema/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <memory>

namespace cluster_link {

/// \brief Probes a prospective link's source Schema Registry during preflight.
///
/// Builds a Schema Registry REST client from the API-mode source connection via
/// `schema_registry_sync::make_source_sr_client` and issues a list to confirm
/// the source is reachable with the supplied credentials.
///
/// The shared `make_source_sr_client` builder is intended to also back the
/// real HTTP-backed `source_reader`
/// (cluster_link/schema_registry_sync/source_reader.h, today only
/// `unavailable_source_reader`) once it is wired, so the preflight and the sync
/// read path construct the source-SR client one way.
class source_sr_prober {
public:
    source_sr_prober() = default;
    source_sr_prober(const source_sr_prober&) = delete;
    source_sr_prober(source_sr_prober&&) = delete;
    source_sr_prober& operator=(const source_sr_prober&) = delete;
    source_sr_prober& operator=(source_sr_prober&&) = delete;
    virtual ~source_sr_prober() = default;

    static std::unique_ptr<source_sr_prober> make_default();

    /// Connects to the source Schema Registry described by \p cfg to confirm it
    /// is reachable with the supplied credentials. A failed connection or
    /// authentication is reported as an `errc::link_sr_unreachable` error.
    virtual ss::future<cl_result<void>> check_source_reachable(
      const model::schema_registry_sync_config::shadow_schema_registry_api& cfg,
      ss::abort_source& as) = 0;
};

/// \brief Validates the Schema Registry side of a prospective cluster link.
///
/// Owns the collaborators the check needs (a `source_sr_prober` for the source
/// side and a reference to the local destination `schema::registry`) so that
/// callers only depend on this one seam rather than on the individual pieces.
class sr_preflight_checker {
public:
    sr_preflight_checker() = default;
    sr_preflight_checker(const sr_preflight_checker&) = delete;
    sr_preflight_checker(sr_preflight_checker&&) = delete;
    sr_preflight_checker& operator=(const sr_preflight_checker&) = delete;
    sr_preflight_checker& operator=(sr_preflight_checker&&) = delete;
    virtual ~sr_preflight_checker() = default;

    static std::unique_ptr<sr_preflight_checker> make_default(
      schema::registry& destination, std::unique_ptr<source_sr_prober>);

    /// Validates a prospective link's Schema Registry API-sync configuration:
    /// that the source Schema Registry is reachable with the supplied
    /// credentials, and that every destination context the link would import
    /// into is empty. A no-op for links not using Schema Registry API mode.
    virtual ss::future<err_info>
    check(const model::metadata& md, ss::abort_source& as) = 0;
};

} // namespace cluster_link
