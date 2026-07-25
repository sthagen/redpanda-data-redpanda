// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "model/compression.h"
#include "model/record.h"

#include <seastar/core/future.hh>

namespace model {

/// \brief batch decompression
///
/// Throws if the batch is not compressed.
ss::future<model::record_batch> decompress_batch(const model::record_batch&);

/// \brief Decompress only the records payload of a compressed batch.
///
/// Cheaper than `decompress_batch()` when a well-formed uncompressed batch is
/// not needed: no header is rebuilt and no checksum is computed. Throws if
/// the batch is not compressed.
ss::future<iobuf> decompress_payload(const model::record_batch&);

/// \brief Build a batch compressed per `target` from an uncompressed records
/// payload and the header of the batch the payload originated from.
///
/// The originating batch may have been compressed with any codec: `header`'s
/// compression bits are cleared and re-set per `target` (`none` produces an
/// uncompressed batch), and its size and checksum metadata are recomputed,
/// exactly once, over the final payload.
ss::future<model::record_batch> recompress_batch(
  model::compression target,
  model::record_batch_header header,
  iobuf uncompressed_payload);

/// \brief synchronous batch decompression
model::record_batch decompress_batch_sync(const model::record_batch&);

// Compress the batch according to the specified compression type.
//
// This method may only be called if the compression type passed in is a valid
// compression (snappy, gzip, lz4, zstd).
//
// The batch passed in must be uncompressed and *not* have the compression flags
// set in the header.
//
// The CRC and size metadata of the batch header are updated to reflect the
// compressed batch.
ss::future<model::record_batch>
  compress_batch(model::compression, model::record_batch);

// The same as above, but synchronous.
//
// Only use in test code.
model::record_batch
  compress_batch_sync(model::compression, model::record_batch);

} // namespace model
