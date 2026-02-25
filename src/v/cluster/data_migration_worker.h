/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/outcome.h"
#include "cluster/data_migration_group_proxy.h"
#include "cluster/data_migration_types.h"
#include "cluster/fwd.h"
#include "cluster/notification.h"
#include "container/chunked_hash_map.h"
#include "errc.h"
#include "model/fundamental.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <memory>
#include <optional>

namespace cluster::data_migrations {

/*
 * This service performs data migration operations on individual partitions
 */
class worker : public ss::peering_sharded_service<worker> {
public:
    worker(
      model::node_id,
      partition_leaders_table&,
      partition_manager&,
      ss::shared_ptr<group_proxy>,
      ss::abort_source&);
    ss::future<> stop();

    ss::future<errc>
    perform_partition_work(model::ntp&& ntp, partition_work&& work);
    void
    abort_partition_work(model::ntp&& ntp, id migration_id, state sought_state);

private:
    // state of an ntp in a migration
    struct mntp_state_t {
        struct requested_t {
            ss::lw_shared_ptr<partition_work> work;
            ss::promise<errc> promise;

            explicit requested_t(partition_work&&);
            requested_t(const requested_t&) = delete;
            requested_t& operator=(const requested_t&) = delete;
            requested_t(requested_t&&) = default;
            requested_t& operator=(requested_t&&) = default;
            ~requested_t() = default;
        };
        struct running_t {
            ss::lw_shared_ptr<partition_work> work;
            seastar::abort_source as;

            explicit running_t(ss::lw_shared_ptr<partition_work>);
            // not movable or copyable:
            // work functions use references to _as and promise
            running_t(const running_t&) = delete;
            running_t& operator=(const running_t&) = delete;
            running_t(running_t&&) = delete;
            running_t& operator=(running_t&&) = delete;
            ~running_t() = default;
        };

        // At least one of `last_requested` and `running` must be set. If both,
        // `running->work` and `last_requested->work` of the same mntp_state may
        // or may not point to the same object. For different keys (where a key
        // is NTP + migration id) they all must be distinct.
        std::optional<requested_t> last_requested;
        // set iff `work_fiber` is running
        std::optional<running_t> running;

        explicit mntp_state_t(partition_work&& work);

        [[nodiscard]] bool still_needed() const;
        void report_back(errc ec);
    };

    using mntp_state_ptr = std::unique_ptr<mntp_state_t>;

    // For offset partitions the key is migration id, as their works may run
    // concurrently; for data partitions the key is nullopt, as these must run
    // exclusively. This helper function creates such keys to be used in a map
    // that tracks running and requested works for an ntp.
    //
    // Since revision ids are passed and checked for partition operations, using
    // nullopt for them as a key (i.e. making them kick out each other as
    // requests come) is not necessary for correctness, but rather serves as an
    // optimization agains excessive work.
    using migration_id_key = std::optional<id>;
    static migration_id_key
    make_migration_id_key(const model::ntp& ntp, id migration_id);

    using mntps_map_t = chunked_hash_map<migration_id_key, mntp_state_ptr>;
    struct ntp_state_t {
        bool is_leader;
        notification_id_type leadership_subscription;
        mntps_map_t migration_states;

        ntp_state_t(
          bool is_leader, notification_id_type leadership_subscription);
    };

    using ntp_state_ptr = std::unique_ptr<ntp_state_t>;
    using managed_ntps_map_t = chunked_hash_map<model::ntp, ntp_state_ptr>;

    void abort_all() noexcept;
    void handle_leadership_update(const model::ntp& ntp, bool is_leader);
    void unmanage_ntp(const model::ntp& ntp, migration_id_key midkey);
    void unmanage_ntp(
      managed_ntps_map_t::iterator ntp_state_it,
      mntps_map_t::const_iterator it);
    void spawn_work_fiber_if_needed(
      const model::ntp& ntp,
      migration_id_key midkey,
      ntp_state_t& ntp_state,
      mntp_state_t& mntp_state);
    ss::future<> work_fiber(
      model::ntp ntp,
      migration_id_key midkey,
      ntp_state_t& ntp_state,
      mntp_state_t& mntp_state);

    // also resulting future cannot throw when co_awaited
    ss::future<errc> do_work(
      const model::ntp& ntp, mntp_state_t::running_t& running_work) noexcept;
    ss::future<errc> do_work(
      const model::ntp& ntp,
      mntp_state_t::running_t& running_work,
      const inbound_partition_work_info& itwi);
    ss::future<errc> do_work(
      const model::ntp& ntp,
      mntp_state_t::running_t& running_work,
      const outbound_partition_work_info& otwi);

    ss::future<result<model::offset, errc>> block_partition(
      ss::lw_shared_ptr<partition> partition,
      bool block,
      model::revision_id revision_id);

    ss::future<result<model::offset, errc>> block_groups(
      const model::ntp& ntp,
      const chunked_vector<kafka::group_id>& groups,
      bool block,
      model::revision_id revision_id);

    model::node_id _self;
    partition_leaders_table& _leaders_table;
    partition_manager& _partition_manager;
    ss::shared_ptr<group_proxy> _group_proxy;
    ss::abort_source& _as;
    ss::optimized_optional<ss::abort_source::subscription> _as_sub;

    std::chrono::milliseconds _operation_timeout;
    std::chrono::milliseconds _cooldown_period;

    managed_ntps_map_t _managed_ntps;
    ss::gate _gate;
};

} // namespace cluster::data_migrations
