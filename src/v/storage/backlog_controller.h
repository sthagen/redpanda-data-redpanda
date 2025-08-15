// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once
#include "base/seastarx.h"
#include "config/property.h"
#include "metrics/metrics.h"

#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/log.hh>

namespace storage {
struct backlog_controller_config {
    backlog_controller_config(
      config::binding<double> proportional_coeff,
      config::binding<double> integral_coeff,
      config::binding<double> derivative_coeff,
      int64_t normalization_factor,
      std::function<int64_t()> sp_function,
      int initial_shares,
      config::binding<std::chrono::milliseconds> sampling_interval,
      ss::scheduling_group sg,
      config::binding<int16_t> min_shares,
      config::binding<int16_t> max_shares);

    config::binding<double> proportional_coeff;
    config::binding<double> integral_coeff;
    config::binding<double> derivative_coeff;
    int64_t normalization_factor;
    std::function<int64_t()> setpoint_cb;
    int initial_shares;
    config::binding<std::chrono::milliseconds> sampling_interval;
    ss::scheduling_group scheduling_group;
    config::binding<int16_t> min_shares;
    config::binding<int16_t> max_shares;
};
/**
 * Backlog controller is PID controller implementation to manage amount of
 * seastar CPU and IO shares. This contoller works in an negative feedback loop
 * using a backlog size as process value and amount of shares as a control
 * value.
 *
 *                    ┌────────────────────┐            ┌──────────────┐
 *   set backlog size │                    │ set shares │              │
 *       ───────────┬─┤ backlog_controller ├────────────┤ some process │
 *                - │ │                    │            │              │
 *                  │ └────────────────────┘            └──────┬───────┘
 *                  │                                          │
 *                  │                                          │
 *                  └──────────────────────────────────────────┘
 *                                 current backlog size
 *
 * Backlog controller uses PID controller design to calculate the output shares.
 *
 *
 *                 ┌────────────────────────┐
 *                 │                        │
 *               P │          error         ├─────┐
 *                 │                        │     │
 *                 └────────────────────────┘     │
 *                                                │
 *                 ┌────────────────────────┐     │
 *                 │                        │    ┌▼┐      ┌─────┐ shares
 *               I │ k_i*(aggregated_error) ├───►│+├─────►│ k_p ├──────►
 *                 │                        │    └▲┘      └─────┘
 *                 └────────────────────────┘     │
 *                                                │
 *                 ┌────────────────────────┐     │
 *                 │                        │     │
 *               D │  k_d*(err - prev_err)  ├─────┘
 *                 │                        │
 *                 └────────────────────────┘
 *
 * Both the integral and derivative actions of the controller can be disabled
 * by setting corresponding coefficient to 0.
 */
class backlog_controller {
public:
    struct sampler {
        virtual ss::future<int64_t> sample_backlog() = 0;
        virtual ~sampler() noexcept = default;
    };

    backlog_controller(
      std::unique_ptr<sampler>, ss::logger&, backlog_controller_config);

    ss::future<> start();
    ss::future<> stop();

    void setup_metrics(const ss::sstring&);

private:
    ss::future<> set();
    ss::future<> update();

    void accumulate_error(int64_t);

    std::unique_ptr<sampler> _sampler;
    ss::logger& _log;
    ss::timer<> _sampling_timer;
    // state
    int _current_shares;
    int64_t _current_backlog{0};
    int64_t _prev_error{0};
    int64_t _error_integral{0};
    // configuration
    backlog_controller_config _cfg;
    ss::gate _gate;
    metrics::internal_metric_groups _metrics;
};
} // namespace storage
