// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/format_to.h"
#include "storage/mvlog/entry_stream.h"
#include "storage/mvlog/reader_outcome.h"

namespace storage::experimental::mvlog {

class batch_collector;
class entry_stream;

enum class collect_stream_outcome {
    // The batch collector is full.
    buffer_full,

    // There are no more entries in the stream.
    end_of_stream,

    // The batch collection is complete, e.g. because we have reached the
    // desired offset.
    stop,
};
inline fmt::iterator format_to(collect_stream_outcome o, fmt::iterator out) {
    switch (o) {
    case collect_stream_outcome::buffer_full:
        return fmt::format_to(out, "collect_stream_outcome::buffer_full");
    case collect_stream_outcome::end_of_stream:
        return fmt::format_to(out, "collect_stream_outcome::end_of_stream");
    case collect_stream_outcome::stop:
        return fmt::format_to(out, "collect_stream_outcome::stop");
    }
    return fmt::format_to(out, "");
}

// Parses and collects the record batches from the given entry stream.
// Ignores other kinds of entries.
ss::future<result<collect_stream_outcome, errc>>
collect_batches_from_stream(entry_stream& entries, batch_collector& collector);

// Parses a record_batch_entry_body from the given iobuf, validates its
// contents, and collects the underlying record batch.
result<reader_outcome, errc>
collect_batch_from_buf(iobuf body_buf, batch_collector& collector);

} // namespace storage::experimental::mvlog
