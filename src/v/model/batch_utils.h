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
#include "model/record.h"

namespace model {
/**
 * Creates a single ghost batch that fills the gap between start_offset and the
 * end offset.
 */
record_batch
make_ghost_batch(offset start_offset, offset end_offset, term_id term);

/**
 * makes multiple ghost batches required to fill the gap in a way that max batch
 * size (max of int32_t) is not exceeded
 */
std::vector<record_batch>
make_ghost_batches(offset start_offset, offset end_offset, term_id term);
} // namespace model
