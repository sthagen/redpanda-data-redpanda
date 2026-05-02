/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "compaction/reducer.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "utils/prefix_logger.h"

namespace cloud_topics::l1 {

class l1_object_sink : public compaction::sliding_window_reducer::sink {
public:
    l1_object_sink(
      model::topic_id_partition,
      l1::io*,
      l1::metastore*,
      ss::abort_source&,
      config::binding<size_t>,
      size_t,
      prefix_logger&,
      object_builder::options = {});

    // Called by the `source` before batches in a new extent range are provided
    // to the `sink`. This is an asynchronous function because the active L1
    // object may need to be rolled, in case that the next extent range provided
    // is non-contiguous.
    ss::future<> prepare_iteration(kafka::offset) final;

    // Called by the `source` after batches in an extent range are provided
    // to the `sink`.
    ss::future<> finish_iteration(kafka::offset, kafka::offset) final;

protected:
    /// Creates the `object_metadata_builder` from the metastore.
    ss::future<> init_metadata_builder();

    // Initializes the `_inflight_object` with a multipart upload.
    ss::future<> initialize_builder(kafka::offset);

    // Finalizes the `_inflight_object`, completes the multipart upload,
    // and registers the result with the metadata builder.
    ss::future<> flush(kafka::offset);

    // Aborts the multipart upload, closes the builder, and removes the
    // pending object from the metadata builder.
    ss::future<> discard_object(
      cloud_storage_clients::multipart_upload_ref,
      std::unique_ptr<object_builder>,
      object_id);

    // Handles the common finalize preamble: flushes or discards inflight
    // objects, checks if the builder is empty. Returns true if derived class
    // should proceed with commit logic, false if there is nothing to commit.
    ss::future<bool> finalize_inflight(bool success);

protected:
    model::topic_id_partition _tp;
    io* _io;
    metastore* _metastore;
    ss::abort_source& _as;
    // The target maximum L1 object size that will be built.
    config::binding<size_t> _max_object_size;
    prefix_logger& _ctxlog;
    // The part size used for multipart uploads.
    size_t _upload_part_size;
    const object_builder::options _opts;

    // The metadata builder for the current compaction job, created during
    // `initialize()` from the metastore and used to track new objects.
    std::unique_ptr<metastore::object_metadata_builder> _metadata_builder;

    // Tracks whether any upload failed during this job.
    bool _any_object_failed{false};

    // The L1 object currently being built via multipart upload.
    struct inflight_object_t {
        cloud_storage_clients::multipart_upload_ref upload;
        std::unique_ptr<object_builder> builder{nullptr};
        object_id oid;
        kafka::offset object_base_offset{};
    };

    std::unique_ptr<inflight_object_t> _inflight_object{nullptr};

    // The interval set that is populated by extents which have been read by the
    // `source` and written by the `sink`.
    offset_interval_set _processed_extents;
};

} // namespace cloud_topics::l1
