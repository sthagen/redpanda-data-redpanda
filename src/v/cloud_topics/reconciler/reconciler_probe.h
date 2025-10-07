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

#include "metrics/metrics.h"

#include <seastar/core/metrics_registration.hh>

#include <cstdint>

namespace cloud_topics::reconciler {

class reconciler_probe {
public:
    reconciler_probe() = default;

    reconciler_probe(const reconciler_probe&) = delete;
    reconciler_probe& operator=(const reconciler_probe&) = delete;
    reconciler_probe(reconciler_probe&&) = delete;
    reconciler_probe& operator=(reconciler_probe&&) = delete;
    ~reconciler_probe() = default;

    void setup_metrics();

    void increment_rounds() { ++_rounds; }
    void increment_objects_uploaded() { ++_objects_uploaded; }
    void add_bytes_reconciled(uint64_t bytes) { _bytes_reconciled += bytes; }
    void add_batches_reconciled(uint64_t batches) {
        _batches_reconciled += batches;
    }
    void increment_partitions_reconciled() { ++_partitions_reconciled; }
    void increment_object_build_failed() { ++_object_build_failed; }
    void increment_object_upload_failed() { ++_object_upload_failed; }
    void increment_empty_objects_skipped() { ++_empty_objects_skipped; }

private:
    metrics::internal_metric_groups _metrics;

    uint64_t _rounds{0};
    uint64_t _objects_uploaded{0};
    uint64_t _bytes_reconciled{0};
    uint64_t _batches_reconciled{0};
    uint64_t _partitions_reconciled{0};
    uint64_t _object_build_failed{0};
    uint64_t _object_upload_failed{0};
    uint64_t _empty_objects_skipped{0};
};

} // namespace cloud_topics::reconciler
