/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/backlog_controller.h"

#include "base/vlog.h"
#include "config/configuration.h"
#include "datalake/logger.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/reactor.hh>
using namespace std::chrono_literals; // NOLINT

namespace datalake {

backlog_controller::backlog_controller(
  sampling_fn sampling_fn, ss::scheduling_group sg)
  : _sampling_f(std::move(sampling_fn))
  , _scheduling_group(sg)
  , _proportional_coeff(
      config::shard_local_cfg().iceberg_backlog_controller_p_coeff.bind())
  , _setpoint(config::shard_local_cfg().iceberg_target_backlog_size.bind())
  , _sampling_interval(5s) {}

ss::future<> backlog_controller::start() {
    setup_metrics();
    _sampling_timer.set_callback([this] {
        update();
        if (!_as.abort_requested()) {
            _sampling_timer.arm(_sampling_interval);
        }
    });

    _sampling_timer.arm(_sampling_interval);
    co_return;
}

ss::future<> backlog_controller::stop() {
    _sampling_timer.cancel();
    _as.request_abort();
    co_return;
}

void backlog_controller::update() {
    using namespace std::chrono_literals;

    _current_sample = _sampling_f();

    auto current_err = _setpoint() - _current_sample;
    auto update = _proportional_coeff() * current_err;

    update = std::clamp(static_cast<int>(update), _min_shares, _max_shares);

    vlog(
      datalake_log.trace,
      "state update: {{setpoint: {}, current_backlog: {:2f}, current_error: "
      "{:2f}, shares_update: {:2f}}}",
      _setpoint(),
      _current_sample,
      current_err,
      update);

    _scheduling_group.set_shares(static_cast<float>(update));
}

void backlog_controller::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("iceberg:backlog:controller"),
      {
        sm::make_gauge(
          "backlog_size",
          [this] { return _current_sample; },
          sm::description("Iceberg controller current backlog - averaged size "
                          "of the backlog per partition")),

      });
}

} // namespace datalake
