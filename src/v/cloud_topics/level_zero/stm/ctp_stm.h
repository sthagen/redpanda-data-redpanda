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

#include "cloud_topics/level_zero/stm/ctp_stm_state.h"
#include "raft/persisted_stm.h"

namespace experimental::cloud_topics {

class ctp_stm_api;

class ctp_stm final : public raft::persisted_stm<> {
    friend class ctp_stm_api;

public:
    static constexpr const char* name = "ctp_stm";

    ctp_stm(ss::logger&, raft::consensus*);

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        return raft::stm_initial_recovery_policy::read_everything;
    }

private:
    ss::future<> do_apply(const model::record_batch& batch) override;

    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;
    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units u) override;

    ss::future<> apply_raft_snapshot(const iobuf&) override;
    ss::future<iobuf> take_raft_snapshot(model::offset) override;

private:
    ctp_stm_state _state;
};

} // namespace experimental::cloud_topics
