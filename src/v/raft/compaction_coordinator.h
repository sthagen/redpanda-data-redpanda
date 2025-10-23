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

#include "model/fundamental.h"
#include "raft/consensus_client_protocol.h"
#include "raft/follower_stats.h"
#include "raft/fundamental.h"
#include "raft/logger.h"
#include "random/simple_time_jitter.h"
#include "ssx/abort_source.h"
#include "ssx/single_fiber_executor.h"
#include "storage/log.h"
#include "storage/types.h"

#include <algorithm>

namespace raft {
using namespace std::chrono_literals;
/**
 * The @compaction_coordinator component
 * - receives maximum cleanly compacted offset (MCCO) from storage subsystem;
 * - distributes MCCO from each group member to the leader;
 * - calculates maximum tombstone removal offset (MTRO) on the leader;
 * - propagates MTRO to followers;
 * - lets @storage::stm_manager know about the current MTRO.
 *
 * MCCO and MTRO are not inclusive, i.e. denote first non-cleanly-compacted and
 * first non-tombstone-removable offsets respectively.
 */

class compaction_coordinator {
    using clock_t = ss::lowres_clock;
    static constexpr auto timeout = 10s;
    static constexpr auto mtro_send_delay = 3s;

public:
    compaction_coordinator(
      features::feature_table& features,
      follower_stats& fstats,
      ss::shared_ptr<storage::log> log,
      vnode self,
      ctx_log& logger,
      group_id group,
      consensus_client_protocol& client_protocol,
      ss::abort_source& as,
      ss::gate& bg);

    // handle leadership changes
    void on_leadership_change(std::optional<vnode> new_leader_id);

    // handle group configuration changes (e.g. new nodes added to the group)
    void on_group_configuration_change();

    // Changes the frequency of local MCCO updates based on NTP config.
    // Should be called after each NTP config update.
    void on_ntp_config_change();

    // process an RPC from a leader: tell them our MCCO
    get_compaction_mcco_reply
    do_get_compaction_mcco(get_compaction_mcco_request req);

    // process an RPC from a leader: update our MTRO
    distribute_compaction_mtro_reply
    do_distribute_compaction_mtro(distribute_compaction_mtro_request req);

    // Returns current MTRO.
    model::offset get_max_tombstone_remove_offset() const;

    // Returns current MCCO of the replica.
    model::offset get_local_max_cleanly_compacted_offset() const;

private:
    // from leader (on a follower) or from calculated value (on leader)
    void update_mtro(model::offset new_mtro);

    // notify storage and followers (if leader) about MTRO update
    void on_mtro_update();

    // Ideally should be push-, not pull-based, but currently storage doesn't
    // provide such functionality. This is the entry point for periodic MCCO
    // collection, which triggers MTRO update in turn.
    void update_local_mcco();

    // both locally and from followers
    void collect_mcco_from_all_members();

    // the next 3 functions are for getting MCCO from followers
    ss::future<>
    get_and_process_compaction_mcco(vnode node_id, ss::abort_source& op_as);
    ss::future<std::optional<model::offset>> get_compaction_mcco(vnode node_id);
    void on_local_mcco_update(model::offset new_mcco);

    // the next 2 functions are for sending MTRO to followers
    void send_mtro_to_followers();
    ss::future<ss::stop_iteration> send_mtro_to_follower(vnode node_id);

    // calculation of MTRO on the leader
    void recalculate_mtro();

    bool is_leader() const;
    void arm_timer_if_needed(bool jitter_only);
    void cancel_timer();

    clock_t::duration base_interval() const;
    static clock_t::duration retry_interval(clock_t::duration base);

    template<typename Func>
    auto repeat(Func func, ss::abort_source& op_as) -> decltype(func()) {
        using ret_value_t = decltype(func())::value_type;
        auto sub = ssx::subscribe_or_trigger(
          _raft_as, [&op_as] noexcept { op_as.request_abort(); });
        while (true) {
            if (op_as.abort_requested() || _raft_bg.is_closed()) {
                co_return ret_value_t{};
            }
            auto result = co_await func();
            if (result) {
                co_return result;
            }
            if (op_as.abort_requested() || _raft_bg.is_closed()) {
                co_return ret_value_t{};
            }
            vlog(
              _logger.trace, "waiting {} before retrying RPC", _retry_interval);
            co_await ss::sleep_abortable(_retry_interval, op_as)
              .handle_exception_type(
                [](const ss::abort_requested_exception&) { return; });
        }
    }

    ss::shared_ptr<storage::log> _log;
    mutable ctx_log _logger;
    // invariant: armed iff ALL of the following hold:
    // 1) coordinated compaction feature is active
    // 2) consensus' abort source is not triggered and gate is not closed
    // 3) timer callback is not running
    // 4) this node is leader
    // Do NOT make the callback async without proper synchronization.
    ss::timer<clock_t> _timer;
    using jitter_t = simple_time_jitter<clock_t>;
    jitter_t _jitter;
    clock_t::duration _retry_interval;
    follower_stats& _fstats;
    vnode _self;
    raft::group_id _group;
    consensus_client_protocol& _client_protocol;
    ss::abort_source& _raft_as;
    ss::gate& _raft_bg;

    // model::offset{} means never calculated
    model::offset _local_mcco;
    // model::offset{} if never successfully calculated, pending remote MCCOs
    model::offset _mtro;

    // current leader, std::nullopt if no leader
    bool _is_leader{false};

    // cancels the timer when consensus' abort source is triggered
    ss::optimized_optional<ss::abort_source::subscription> _as_sub;

    // binding to tombstone retention config, to adjust timer interval
    config::binding<std::optional<std::chrono::milliseconds>>
      _tombstone_retention_ms_binding;

    // prevent RPC storm at startup
    bool _has_seen_a_leader{false};

    // force sending the same MTRO to followers, as recipients may have changed
    bool _need_force_update{false};

    bool _started{false};

public:
    struct test_accessor {
        static clock_t::duration
        mcco_getting_delay(const compaction_coordinator& coco);

        static clock_t::duration mtro_distribution_delay();
    };
};
} // namespace raft
