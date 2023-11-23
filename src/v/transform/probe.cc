/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/probe.h"

#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace transform {

void probe::setup_metrics(ss::sstring transform_name) {
    wasm::transform_probe::setup_metrics(transform_name);
    namespace sm = ss::metrics;

    auto name_label = sm::label("function_name");
    const std::vector<sm::label_instance> labels = {
      name_label(std::move(transform_name)),
    };
    std::vector<sm::metric_definition> metric_defs;
    metric_defs.emplace_back(
      sm::make_counter(
        "read_bytes",
        [this] { return _read_bytes; },
        sm::description("The number of bytes input to the transform"),
        labels)
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "write_bytes",
        [this] { return _write_bytes; },
        sm::description("The number of bytes output by the transform"),
        labels)
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_gauge(
        "lag",
        [this] { return _lag; },
        sm::description(
          "The number of pending records on the input topic that have "
          "not yet been processed by the transform"),
        labels)
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "failures",
        [this] { return _failures; },
        sm::description("The number of transform failures"),
        labels)
        .aggregate({sm::shard_label}));

    auto state_label = sm::label("state");
    using state = model::transform_report::processor::state;
    for (const auto& s : {state::running, state::inactive, state::errored}) {
        std::vector<sm::label_instance> state_labels = labels;
        state_labels.push_back(
          state_label(model::processor_state_to_string(s)));
        metric_defs.emplace_back(
          sm::make_gauge(
            "state",
            [this, s] { return _processor_state[s]; },
            sm::description("The number of transforms in a specific state"),
            state_labels)
            .aggregate({sm::shard_label}));
    }
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("transform"), metric_defs);
}
void probe::increment_write_bytes(uint64_t bytes) { _write_bytes += bytes; }
void probe::increment_read_bytes(uint64_t bytes) { _read_bytes += bytes; }
void probe::increment_failure() { ++_failures; }
void probe::state_change(processor_state_change change) {
    if (change.from) {
        _processor_state[*change.from] -= 1;
    }
    if (change.to) {
        _processor_state[*change.to] += 1;
    }
}
void probe::report_lag(int64_t delta) { _lag += delta; }

} // namespace transform
