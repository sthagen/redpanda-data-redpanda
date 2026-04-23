/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "base/format_to.h"
#include "base/seastarx.h"
#include "cluster/drain_status.h"
#include "cluster/fwd.h"
#include "reflection/adl.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/log.hh>

#include <chrono>

namespace cluster {

/*
 * The drain manager is responsible for managing the draining of leadership from
 * a node. It is a core building block for implementing node maintenance mode.
 */
class drain_manager : public ss::peering_sharded_service<drain_manager> {
    static constexpr size_t max_parallel_transfers = 25;
    static constexpr std::chrono::duration transfer_throttle
      = std::chrono::seconds(5);

public:
    explicit drain_manager(ss::sharded<cluster::partition_manager>&);

    ss::future<> start();
    ss::future<> stop();

    /*
     * Start draining this broker.
     */
    ss::future<> drain();

    /*
     * Restore broker to a non-drain[ing] state.
     */
    ss::future<> restore();

    /*
     * Check the status of the draining process.
     *
     * This performs a global reduction across cores.
     */
    ss::future<std::optional<drain_status>> status();

private:
    ss::future<> task();
    ss::future<> do_drain();
    ss::future<> do_restore();

    ss::sharded<cluster::partition_manager>& _partition_manager;
    std::optional<ss::future<>> _drain;
    bool _draining_requested{false};
    bool _restore_requested{false};
    bool _drained{false};
    ssx::semaphore _sem{0, "c/drain-mgr"};
    drain_status _status;
    ss::abort_source _abort;
};

} // namespace cluster

namespace reflection {
template<>
struct adl<cluster::drain_status> {
    void to(iobuf& out, cluster::drain_status&& r) {
        serialize(
          out,
          r.finished,
          r.errors,
          r.partitions,
          r.eligible,
          r.transferring,
          r.failed);
    }
    cluster::drain_status from(iobuf_parser& in) {
        auto finished = adl<bool>{}.from(in);
        auto errors = adl<bool>{}.from(in);
        auto partitions = adl<std::optional<size_t>>{}.from(in);
        auto eligible = adl<std::optional<size_t>>{}.from(in);
        auto transferring = adl<std::optional<size_t>>{}.from(in);
        auto failed = adl<std::optional<size_t>>{}.from(in);
        return {
          .finished = finished,
          .errors = errors,
          .partitions = partitions,
          .eligible = eligible,
          .transferring = transferring,
          .failed = failed,
        };
    }
};
} // namespace reflection
