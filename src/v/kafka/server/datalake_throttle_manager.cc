/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "kafka/server/datalake_throttle_manager.h"

#include "config/configuration.h"
#include "kafka/server/logger.h"
#include "ssx/future-util.h"
namespace kafka {
using namespace std::chrono_literals;
namespace {

static constexpr std::string_view anonymous_client_id = "";

static constexpr std::chrono::milliseconds no_throttling{0};
std::string_view
get_effective_client_id(const std::optional<std::string_view>& client_id) {
    if (client_id.has_value()) {
        return *client_id;
    }
    return anonymous_client_id;
}

} // namespace
void datalake_throttle_manager::merge_producer_maps(
  producers_map_t& target, const producers_map_t& source) {
    for (const auto& [client_id, state] : source) {
        auto [it, emplaced] = target.try_emplace(client_id, state);
        if (!emplaced) {
            it->second.last_datalake_produce = std::max(
              it->second.last_datalake_produce, state.last_datalake_produce);
        }
    }
}

datalake_throttle_manager::backlog_status
datalake_throttle_manager::backlog_status::operator+(
  const datalake_throttle_manager::backlog_status& o) const {
    return datalake_throttle_manager::backlog_status{
      .partitions_backlog_limit_breached
      = partitions_backlog_limit_breached + o.partitions_backlog_limit_breached,
      .partitions_translation_blocked = partitions_translation_blocked
                                        + o.partitions_translation_blocked};
}

bool datalake_throttle_manager::backlog_status::has_no_issues() const {
    return partitions_backlog_limit_breached == 0
           && partitions_translation_blocked == 0;
}

std::ostream& operator<<(
  std::ostream& o, const datalake_throttle_manager::backlog_status& s) {
    fmt::print(
      o,
      "{{partitions_backlog_limit_breached: {}, "
      "partitions_translation_blocked: {}}}",
      s.partitions_backlog_limit_breached,
      s.partitions_translation_blocked);
    return o;
}

datalake_throttle_manager::datalake_throttle_manager(
  status_provider_fn status_provider,
  config::binding<std::chrono::milliseconds> producer_gc_threshold,
  config::binding<std::chrono::milliseconds> max_kafka_throttle)
  : _shard_status_provider(std::move(status_provider))
  , _producer_gc_threshold(std::move(producer_gc_threshold))
  , _max_kafka_throttle(std::move(max_kafka_throttle)) {
    _update_timer.set_callback([this] {
        ssx::spawn_with_gate(
          _gate, [this] { return gc_and_update_global_producers_map(); });
    });
}

void datalake_throttle_manager::start() {
    using namespace std::chrono_literals;
    setup_metrics();
    /**
     * Only run update timer on shard 0
     */
    if (ss::this_shard_id() == 0) {
        _update_timer.arm_periodic(100ms);
    }
}

ss::future<> datalake_throttle_manager::stop() {
    _update_timer.cancel();
    return _gate.close();
}

void datalake_throttle_manager::mark_datalake_producer(
  const std::optional<std::string_view>& client_id,
  clock_type::time_point now) {
    _shard_local_producers.insert_or_assign(
      ss::sstring(get_effective_client_id(client_id)),
      producer_state{.last_datalake_produce = now});
}

ss::future<> datalake_throttle_manager::gc_and_update_global_producers_map() {
    vassert(
      ss::this_shard_id() == 0,
      "Global producers map should be updated only on shard 0");

    _translation_status = co_await container().map_reduce0(
      [](datalake_throttle_manager& instance) {
          return instance._shard_status_provider();
      },
      backlog_status{},
      [](backlog_status acc, backlog_status shard_status) {
          return acc + shard_status;
      });
    if (_translation_status.has_no_issues()) {
        _last_no_issues_timestamp = clock_type::now();
    }
    /**
     * Collect shard local maps and while iterating over the shards update the
     * total backlog
     */
    auto shard_local_maps = co_await ssx::parallel_transform(
      boost::irange(ss::smp::count), [this](auto shard_id) {
          return container().invoke_on(
            shard_id,
            [status = _translation_status](datalake_throttle_manager& other) {
                other._translation_status = status;
                return std::exchange(other._shard_local_producers, {});
            });
      });

    for (auto& shard_map : shard_local_maps) {
        merge_producer_maps(_global_producers, shard_map);
    }

    std::erase_if(_global_producers, [this](const auto& p) {
        return (clock_type::now() - p.second.last_datalake_produce)
               > _producer_gc_threshold();
    });
}

ss::future<std::chrono::milliseconds>
datalake_throttle_manager::maybe_throttle_producer(
  std::optional<std::string_view> client_id) {
    // fast path, backlog is below the threshold

    if (_translation_status.has_no_issues()) [[likely]] {
        co_return no_throttling;
    }

    auto throttle_ms = co_await container().invoke_on(
      0, [&client_id](datalake_throttle_manager& shard_0_manager) {
          auto throttle_ms = shard_0_manager.get_producer_throttle(client_id);
          if (throttle_ms > 0ms) {
              vlog(
                client_quota_log.debug,
                "Throttling producer {} for {}ms, current "
                "status: {}",
                client_id,
                throttle_ms,
                shard_0_manager._translation_status);
          }
          return throttle_ms;
      });
    // record throttle for exposing a metric
    _total_throttle += throttle_ms.count();
    co_return throttle_ms;
}

std::chrono::milliseconds datalake_throttle_manager::get_producer_throttle(
  const std::optional<std::string_view>& client_id) {
    vassert(
      ss::this_shard_id() == 0, "Throttle can only be calculate on shard 0");
    if (_translation_status.has_no_issues()) [[likely]] {
        return no_throttling;
    }
    auto effective_client_id = get_effective_client_id(client_id);

    auto it = _global_producers.find(effective_client_id);
    // do not throttle non datalake producers
    if (it == _global_producers.end()) {
        return no_throttling;
    }

    return calculate_throttle();
}

std::chrono::milliseconds
datalake_throttle_manager::calculate_throttle() const {
    vassert(
      ss::this_shard_id() == 0, "Throttle can only be calculate on shard 0");
    if (_translation_status.partitions_translation_blocked > 0) {
        // if translation is blocked (this should never really be the case),
        // use max throttle
        return _max_kafka_throttle();
    }
    // Heuristic:
    // we apply the incremental throttle based on time spent in the degraded
    // translation state, the longer the time the more we throttle. The
    // throttle is set to max after 5 minutes

    if (_translation_status.partitions_backlog_limit_breached > 0) {
        auto time_in_degraded_state
          = std::chrono::duration_cast<std::chrono::milliseconds>(
            ss::lowres_clock::now() - _last_no_issues_timestamp);

        const auto max_throttle_ratio = std::min(
          double(time_in_degraded_state.count()) / 300000, 1.0);

        return std::chrono::milliseconds(static_cast<size_t>(
          max_throttle_ratio * _max_kafka_throttle().count()));
    }

    return 0ms;
}

void datalake_throttle_manager::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    _metrics.add_group(
      "kafka:datalake:throttle",
      {
        sm::make_counter(
          "total_throttle",
          [this] { return _total_throttle; },
          sm::description(
            "Total datalake producer throttle time in milliseconds")),
      });
}

} // namespace kafka
