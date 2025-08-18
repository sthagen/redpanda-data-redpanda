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
