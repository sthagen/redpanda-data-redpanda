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
#include "base/seastarx.h"
#include "config/property.h"
#include "metrics/metrics.h"

#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/log.hh>

namespace datalake {
/**
 * Class responsible for adjusting shares to maintain a target translation
 * backlog size. This simple proportional controller uses a sampling function to
 * collect the size of the backlog and adjust the shares of the datalake
 * scheduling group to keep the average backlog size close to the setpoint.
 */
class backlog_controller {
public:
    using sampling_fn = ss::noncopyable_function<long double()>;
    backlog_controller(sampling_fn, ss::scheduling_group sg);
    /**
     * Returns true if the datalake scheduling group has reached the maximum
     * allowed priority. Indicates that translation backlog is growing and
     * translators can not keep up.
     */
    bool max_shares_assigned() const {
        return static_cast<int>(_scheduling_group.get_shares()) == _max_shares;
    }

    ss::future<> start();
    ss::future<> stop();

private:
    void update();
    void setup_metrics();

    sampling_fn _sampling_f;
    ss::scheduling_group _scheduling_group;
    /**
     * Proportionality coefficient for the controller, this value controls how
     * eager the controller is to adjust the shares of the datalake scheduling
     * group.
     */
    config::binding<double> _proportional_coeff;
    config::binding<double> _integral_coeff;
    /**
     * The target backlog size for the controller to keep. The controller will
     * adjust the scheduling group shares keep the average backlog size close to
     * this value.
     */
    config::binding<uint32_t> _setpoint;
    std::chrono::steady_clock::duration _sampling_interval;
    ss::timer<> _sampling_timer;
    // Current value of the backlog (currently it is average size of data to
    // translate for datalake enabled partitions)
    long double _current_sample{0};
    long double _total_err{0};
    std::chrono::steady_clock::time_point _last_sample_time
      = std::chrono::steady_clock::now();
    /**
     * Limits for controlled scheduling group shares.
     */
    int _min_shares{1};
    int _max_shares{1000};
    ss::abort_source _as;
    metrics::internal_metric_groups _metrics;
    void reset();
};
} // namespace datalake
