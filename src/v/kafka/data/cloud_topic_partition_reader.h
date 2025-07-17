/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cloud_topics/extent_meta.h"
#include "model/record_batch_reader.h"
#include "storage/log_reader.h"

#include <chrono>

namespace cluster {
class partition;
}

namespace experimental::cloud_topics {
class data_plane_api;
class app;
} // namespace experimental::cloud_topics

namespace kafka {

/*
This class implements a record batch reader for cloud topics partitions.

The reader is a state machine. These are the states in which it can be
- empty_state, no metadata is cached, no data is materialized;
- ready_state, metadata is available but no data is materialized
- materialized_state, the reader contains materialized batches
- end of stream.

While in first two states the reader can consume data from the record
batch cache if the data is available.
             ┌───┐
             │EOS├───┤Terminate│
             └───┘
               ▲
               │
             ┌─┴───┐   ┌─────┐
│Init├──────►│empty├──►│ready│
             └─────┘   └──┬──┘
                 ▲        │
                 │        ▼
               ┌─┴──────────┐
               │materialized│
               └────────────┘

The reader starts with 'empty' state.

When in 'emtpy' state the reader initiates fetching of metadata from the
underlying partition. When the metadata is fetched it transitions to
'ready' state. If it's impossible to continue the reader transitions to
the 'EOS' state.

In 'ready' state the reader invokes the cloud topics api and asks to
materialize metadata. When the batches are materialized the reader transitions
to the 'materialized' state.

The data can be consumed while the reader is in the 'materialized' state.
When all materialized record batches are consumed the reader transitions
back to 'empty' state.
*/
class cloud_topic_partition_reader_impl
  : public model::record_batch_reader::impl {
public:
    cloud_topic_partition_reader_impl(
      storage::log_reader_config& cfg,
      ss::lw_shared_ptr<cluster::partition> underlying,
      ss::shared_ptr<experimental::cloud_topics::data_plane_api> ct_api);

    bool is_end_of_stream() const final;

    ss::future<model::record_batch_reader::storage_t>
      do_load_slice(model::timeout_clock::time_point) final;

    void print(std::ostream& o) final;

private:
    // States
    enum class state {
        empty_state,
        ready_state,
        materialized_state,
        end_of_stream_state,
    };

    bool cache_enabled() const;

    // Fetch L0 meta batches from the underlying partition
    ss::future<> fetch_metadata(model::timeout_clock::time_point deadline);
    ss::future<> materialize_batches(model::timeout_clock::time_point deadline);
    void consume_materialized_batches(
      chunked_circular_buffer<model::record_batch>* dest);
    // Return data from the record batch cache.
    // This method could change state of the reader to end_of_stream_state
    // when it reaches committed offset.
    std::optional<chunked_circular_buffer<model::record_batch>>
    maybe_load_slices_from_cache();

    state _current{state::empty_state};

    chunked_circular_buffer<experimental::cloud_topics::extent_meta> _meta;
    chunked_circular_buffer<model::record_batch_header> _headers;
    chunked_vector<model::record_batch> _batches;
    storage::log_reader_config _config;
    ss::lw_shared_ptr<cluster::partition> _underlying;
    ss::shared_ptr<experimental::cloud_topics::data_plane_api> _ct_api;
};

} // namespace kafka
