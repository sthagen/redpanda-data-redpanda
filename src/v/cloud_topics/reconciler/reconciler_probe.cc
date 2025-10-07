/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/reconciler/reconciler_probe.h"

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics::reconciler {

void reconciler_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics:reconciler"),
      {
        sm::make_counter(
          "rounds",
          [this] { return _rounds; },
          sm::description("Number of reconciliation rounds")),
        sm::make_counter(
          "objects_uploaded",
          [this] { return _objects_uploaded; },
          sm::description("Total objects uploaded, including orphans")),
        sm::make_counter(
          "bytes_reconciled",
          [this] { return _bytes_reconciled; },
          sm::description("Total bytes produced, including metadata")),
        sm::make_counter(
          "batches_reconciled",
          [this] { return _batches_reconciled; },
          sm::description("Total record batches reconciled")),
        sm::make_counter(
          "partitions_reconciled",
          [this] { return _partitions_reconciled; },
          sm::description("Counts each time a partition contributed a batch")),
        sm::make_counter(
          "object_build_failed",
          [this] { return _object_build_failed; },
          sm::description("Total objects that failed to build")),
        sm::make_counter(
          "object_upload_failed",
          [this] { return _object_upload_failed; },
          sm::description("Total objects that failed to upload")),
        sm::make_counter(
          "empty_objects_skipped",
          [this] { return _empty_objects_skipped; },
          sm::description(
            "Total objects skipped because no data was available")),
      });
}

} // namespace cloud_topics::reconciler
