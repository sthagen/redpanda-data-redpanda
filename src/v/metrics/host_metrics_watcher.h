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

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <base/seastarx.h>
#include <metrics/metrics.h>

#include <cstdint>
#include <unordered_map>

namespace metrics {

// Wrapper class that sets up metrics that export /proc/diskstats and a few
// values from /proc/net/snmp. Usually this is a node_exporter job but often
// it's hard to join metrics or we don't have access to that at all. Hence we
// export the most important metrics ourselves.
class host_metrics_watcher {
public:
    host_metrics_watcher(host_metrics_watcher&&) = delete;
    host_metrics_watcher(const host_metrics_watcher&) = delete;
    host_metrics_watcher& operator=(host_metrics_watcher&&) = delete;
    host_metrics_watcher& operator=(const host_metrics_watcher&) = delete;

    // https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats
    // As of 5.5
    // Not counting major number, minor number and device name
    constexpr static uint64_t diskstats_field_count = 17;

    using diskstats = std::array<uint64_t, diskstats_field_count>;

    // diskname -> diskstats
    using diskstats_map = std::unordered_map<ss::sstring, diskstats>;

    struct netstat_stats {
        uint64_t bytes_received = 0;
        uint64_t bytes_sent = 0;
    };

    struct snmp_stats {
        uint64_t tcp_established = 0;
        uint64_t packets_received = 0;
        uint64_t packets_sent = 0;
    };

    explicit host_metrics_watcher(ss::logger& logger);

    // needed for construct_service stuff in `application`
    ss::future<> stop() { return ss::make_ready_future(); };

    // parse the actual /proc/diskstats file. static method for testability
    static void
    parse_diskstats(std::string_view diskstats_lines, diskstats_map& stats);

    // parse the actual /proc/net/netstat file. static method for testability
    static void
    parse_netstat(std::string_view netstat_lines, netstat_stats& stats);

    // parse the actual /proc/net/snmp file. static method for testability
    static void parse_snmp(std::string_view snmp_lines, snmp_stats& stats);

private:
    void setup_metrics_for_disk(const std::string& diskname);
    void setup_netstat_metrics();
    void setup_snmp_metrics();
    void maybe_refresh_diskstats();
    void maybe_refresh_netstat();
    void maybe_refresh_snmp();

    ss::logger& _logger;

    template<typename Stats>
    struct metrics_file_info {
        // fd to metrics file - optional because ss::file_desc not default
        // constructible
        std::optional<ss::file_desc> fd;
        // timestamp of last read
        ss::lowres_clock::time_point last_read
          = ss::lowres_clock::time_point::min();
        // indicating whether reading has errored out. No further reading will
        // take place
        bool errored = false;
        // cached values from last read
        Stats stats;
    };

    // /proc/diskstats
    metrics_file_info<diskstats_map> _diskstats;

    // /proc/net/netstat
    metrics_file_info<netstat_stats> _netstats;

    // /proc/net/snmp
    metrics_file_info<snmp_stats> _snmp_stats;

    metrics::internal_metric_groups _metrics;
};

} // namespace metrics
