/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/partition_manifest.h"
#include "storage/log.h"

namespace archival {

enum class replica_state_anomaly_type {
    // Gap between the manifest and the local storage
    offsets_gap,

    // Inconsistent offset-translator state
    ot_state,
};

struct replica_state_anomaly {
    replica_state_anomaly_type type;
    ss::sstring message;
};

/// The ntp_archiver starts when the partition becomes a leader.
/// If the partition's state is good then it can just start uploading the
/// data to S3. But if the state of the partition is not correct for some
/// reason (software bug) the uploads might "poison" the object storage.
///
/// The problem can be prevented if we will check invariants before allowing
/// the ntp_archiver to continue. This class implements these checks.
///
/// We're validating the replica state over previously uploaded metadata.
/// The idea is that we can gradually build confidence by making incremental
/// metadata checks. This class validates that there is no gap between the
/// local start offset and the last uploaded delta and that the offset
/// translation metadata doesn't change after the leadership transfer.
///
//  This code also defines manual intervention point. Once the
/// problem is detected the NTP archiver shouldn't do anything other than
/// reporting the problem periodically and asking for intervention. After
/// examining the anomaly the operator might decide to disable the checks and
/// allow the uploads to continue. This is an improvement over the situation
/// when the uploads are getting stuck because we're not doing any extra
/// work. It may also be easier to diagnose the problem if the archiver is
/// not running. Potentially, we can do some automatic mitigations
/// (transferring leadership or blocking writes).
class replica_state_validator {
public:
    explicit replica_state_validator(
      const storage::log&, const cloud_storage::partition_manifest&);

    bool has_anomalies() const noexcept;

    const chunked_vector<replica_state_anomaly>& get_anomalies() const noexcept;

    void maybe_print_scarry_log_message() const;

private:
    /// Run validations.
    /// Return set of detected anomalies.
    /// Throw if we failed to perform validation process.
    chunked_vector<replica_state_anomaly> validate();

    const storage::log* _log;
    const cloud_storage::partition_manifest* _manifest;
    chunked_vector<replica_state_anomaly> _anomalies;
};

} // namespace archival
