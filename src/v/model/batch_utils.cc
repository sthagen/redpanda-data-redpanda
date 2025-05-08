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

#include "model/batch_utils.h"
namespace model {

model::record_batch make_ghost_batch(
  model::offset start_offset, model::offset end_offset, model::term_id term) {
    auto delta = end_offset - start_offset;
    auto now = model::timestamp::now();
    model::record_batch_header header = {
      .size_bytes = model::packed_record_batch_header_size,
      .base_offset = start_offset,
      .type = model::record_batch_type::ghost_batch,
      .crc = 0, // crc computed later
      .attrs = model::record_batch_attributes{} |= model::compression::none,
      .last_offset_delta = static_cast<int32_t>(delta),
      .first_timestamp = now,
      .max_timestamp = now,
      .producer_id = -1,
      .producer_epoch = -1,
      .base_sequence = -1,
      .record_count = static_cast<int32_t>(delta() + 1),
      .ctx = model::record_batch_header::context(term, ss::this_shard_id())};

    model::record_batch batch(
      std::move(header), model::record_batch::compressed_records{});

    batch.header().crc = model::crc_record_batch(batch);
    batch.header().header_crc = model::internal_header_only_crc(batch.header());
    return batch;
}

/**
 * makes multiple ghost batches required to fill the gap in a way that max batch
 * size (max of int32_t) is not exceeded
 */
std::vector<model::record_batch> make_ghost_batches(
  model::offset start_offset, model::offset end_offset, model::term_id term) {
    std::vector<model::record_batch> batches;
    while (start_offset <= end_offset) {
        static constexpr model::offset max_batch_size{
          std::numeric_limits<int32_t>::max()};
        // limit max batch size
        const model::offset delta = std::min<model::offset>(
          max_batch_size, end_offset - start_offset);

        batches.push_back(
          make_ghost_batch(start_offset, delta + start_offset, term));
        start_offset = next_offset(batches.back().last_offset());
    }

    return batches;
}
} // namespace model
