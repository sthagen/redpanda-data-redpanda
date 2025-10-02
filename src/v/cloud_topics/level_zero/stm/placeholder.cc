/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/stm/placeholder.h"

#include "storage/record_batch_builder.h"

namespace cloud_topics {

model::record_batch encode_placeholder_batch(
  model::record_batch_header header, extent_meta extent) {
    vassert(
      header.record_count > 0, "Empty record batch not allowed {}", header);

    cloud_topics::ctp_placeholder placeholder{
      .id = extent.id,
      .offset = extent.first_byte_offset,
      .size_bytes = extent.byte_range_size,
    };

    storage::record_batch_builder builder(
      model::record_batch_type::ctp_placeholder, header.base_offset);

    builder.set_producer_identity(header.producer_id, header.producer_epoch);
    if (header.attrs.is_control()) {
        builder.set_control_type();
    }
    if (header.attrs.is_transactional()) {
        builder.set_transactional_type();
    }

    // In case of a placeholder batch the first record contains the
    // actual placeholder and the remaining records are empty. The remaining
    // records are added to avoid confusing any other code that may expect
    // that the number of records in the batch is equal to the number of
    // offsets in the header.
    auto first_value = serde::to_iobuf(placeholder);
    builder.add_raw_kv(std::nullopt, std::move(first_value));

    for (int i = 1; i < header.record_count; ++i) {
        builder.add_raw_kv(std::nullopt, std::nullopt);
    }

    auto ph = std::move(builder).build();
    ph.header().first_timestamp = header.first_timestamp;
    ph.header().max_timestamp = header.max_timestamp;
    ph.header().base_sequence = header.base_sequence;
    ph.header().reset_size_checksum_metadata(ph.data());
    return ph;
}

ctp_placeholder parse_placeholder_batch(model::record_batch batch) {
    iobuf payload = std::move(batch).release_data();
    iobuf_parser parser(std::move(payload));
    auto record = model::parse_one_record_from_buffer(parser);
    iobuf value = std::move(record).release_value();
    auto placeholder = serde::from_iobuf<cloud_topics::ctp_placeholder>(
      std::move(value));
    return placeholder;
}

} // namespace cloud_topics
