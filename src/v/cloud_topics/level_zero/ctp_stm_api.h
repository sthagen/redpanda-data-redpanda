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

#include "base/outcome.h"
#include "cloud_topics/dl_snapshot.h"
#include "cloud_topics/dl_version.h"
#include "model/record.h"

#include <seastar/core/gate.hh>
#include <seastar/util/log.hh>

#include <ostream>

namespace experimental::cloud_topics {

class ctp_stm;

enum class ctp_stm_api_errc {
    timeout,
    not_leader,
};

std::ostream& operator<<(std::ostream& o, ctp_stm_api_errc errc);

class ctp_stm_api {
public:
    ctp_stm_api(ss::logger& logger, ss::shared_ptr<ctp_stm> stm);
    ctp_stm_api(ctp_stm_api&&) noexcept = default;

    ~ctp_stm_api() {
        vassert(_gate.is_closed(), "object destroyed before calling stop()");
    }

public:
    ss::future<> stop();

public:
    /// Request a new snapshot to be created.
    ss::future<checked<dl_snapshot_id, ctp_stm_api_errc>> start_snapshot();

    /// Read the payload of a snapshot.
    std::optional<dl_snapshot_payload> read_snapshot(dl_snapshot_id id);

    /// Remove all snapshots with version less than the given version.
    /// This must be called periodically as new snapshots are being created
    /// to avoid the state growing indefinitely.
    ss::future<checked<void, ctp_stm_api_errc>>
    remove_snapshots_before(dl_version last_version_to_keep);

private:
    /// Replicate a record batch and wait for it to be applied to the ctp_stm.
    /// Returns the offset at which the batch was applied.
    ss::future<checked<model::offset, ctp_stm_api_errc>>
    replicated_apply(model::record_batch&& batch);

private:
    ss::logger& _logger;

    /// Gate held by async operations to ensure that the API is not destroyed
    /// while an operation is in progress.
    ss::gate _gate;

    /// The API can only read the state of the stm. The state can be mutated
    /// only via \ref consensus::replicate calls.
    ss::shared_ptr<const ctp_stm> _stm;
};

} // namespace experimental::cloud_topics
