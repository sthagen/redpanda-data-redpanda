// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/level_zero/ctp_stm_api.h"

#include "base/outcome.h"
#include "cloud_topics/level_zero/ctp_stm.h"
#include "cloud_topics/level_zero/ctp_stm_commands.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "raft/consensus.h"
#include "serde/rw/uuid.h"
#include "storage/record_batch_builder.h"

#include <stdexcept>

namespace experimental::cloud_topics {

std::ostream& operator<<(std::ostream& o, ctp_stm_api_errc errc) {
    switch (errc) {
    case ctp_stm_api_errc::timeout:
        return o << "timeout";
    case ctp_stm_api_errc::not_leader:
        return o << "not_leader";
    }
}

ctp_stm_api::ctp_stm_api(ss::logger& logger, ss::shared_ptr<ctp_stm> stm)
  : _logger(logger)
  , _stm(std::move(stm)) {}

ss::future<> ctp_stm_api::stop() { co_await _gate.close(); }

ss::future<checked<dl_snapshot_id, ctp_stm_api_errc>>
ctp_stm_api::start_snapshot() {
    vlog(_logger.debug, "Replicating ctp_stm_cmd::start_snapshot_cmd");
    auto h = _gate.hold();

    storage::record_batch_builder builder(
      model::record_batch_type::ctp_stm_command, model::offset(0));
    builder.add_raw_kv(
      serde::to_iobuf(ctp_stm_key::start_snapshot),
      serde::to_iobuf(start_snapshot_cmd()));

    auto batch = std::move(builder).build();

    auto apply_result = co_await replicated_apply(std::move(batch));
    if (apply_result.has_failure()) {
        co_return apply_result.error();
    }

    // We abuse knowledge of implementation detail here to construct the
    // dl_snapshot_id without having to setup listeners and notifiers of command
    // apply.
    auto expected_id = dl_snapshot_id(dl_version(apply_result.value()));

    // Ensure that the expected snapshot was created.
    if (!_stm->_state.snapshot_exists(expected_id)) {
        throw std::runtime_error(fmt::format(
          "Snapshot with expected id not found after waiting for command to be "
          "applied: {}",
          expected_id));
    }

    co_return outcome::success(expected_id);
}

std::optional<dl_snapshot_payload>
ctp_stm_api::read_snapshot(dl_snapshot_id id) {
    return _stm->_state.read_snapshot(id);
}

ss::future<checked<void, ctp_stm_api_errc>>
ctp_stm_api::remove_snapshots_before(dl_version last_version_to_keep) {
    vlog(_logger.debug, "Replicating ctp_stm_cmd::remove_snapshots_cmd");
    auto h = _gate.hold();

    storage::record_batch_builder builder(
      model::record_batch_type::ctp_stm_command, model::offset(0));
    builder.add_raw_kv(
      serde::to_iobuf(ctp_stm_key::remove_snapshots_before_version),
      serde::to_iobuf(
        remove_snapshots_before_version_cmd(last_version_to_keep)));

    auto batch = std::move(builder).build();
    auto apply_result = co_await replicated_apply(std::move(batch));
    if (apply_result.has_failure()) {
        co_return apply_result.error();
    }

    co_return outcome::success();
}

ss::future<checked<model::offset, ctp_stm_api_errc>>
ctp_stm_api::replicated_apply(model::record_batch&& batch) {
    model::term_id term = _stm->_raft->term();

    auto opts = raft::replicate_options(raft::consistency_level::quorum_ack);
    opts.set_force_flush();
    auto res = co_await _stm->_raft->replicate(term, std::move(batch), opts);

    if (res.has_error()) {
        throw std::runtime_error(
          fmt::format("Failed to replicate overlay: {}", res.error()));
    }

    co_await _stm->wait(
      res.value().last_offset, model::timeout_clock::now() + 30s);

    co_return res.value().last_offset;
}

}; // namespace experimental::cloud_topics
