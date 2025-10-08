/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/types.h"
#include "model/record.h"
#include "serde/envelope.h"
#include "serde/rw/named_type.h"
#include "serde/rw/uuid.h"

/*
 * A placeholder is a batch that is replicated into the CTP with its payload
 * replaced by an address to where the payload is stored. For example, if a
 * client application produces a batch with a 1 MB payload, a small placeholder
 * batch may be replicated to the CTP (~100 bytes) with the larger payload
 * stored in object storage.
 *
 * Structure of placeholder batch
 * ==============================
 *
 * - Header: logically identical to the header which would exist if the payload
 * had been replicatedl. Physically, fields such as checksums and size will
 * differ.
 *
 * - Record 1: placeholder metadata (e.g. address of payload)
 *
 * - Record 2+: empty records to give the placeholder batch the same shape as
 * the original batch. this allows the batch to flow through existing machinery
 * without requiring special casing to be introduced.
 */
namespace cloud_topics {

struct ctp_placeholder
  : serde::
      envelope<ctp_placeholder, serde::version<0>, serde::compat_version<0>> {
    /*
     * The object containing the payload of the batch represented by this
     * placeholder.
     */
    object_id id;

    /*
     * The byte extent [offset, size) of the payload in the object.
     */
    first_byte_offset_t offset;
    byte_range_size_t size_bytes;

    auto serde_fields() { return std::tie(id, offset, size_bytes); }
};

model::record_batch
  encode_placeholder_batch(model::record_batch_header, extent_meta);

ctp_placeholder parse_placeholder_batch(model::record_batch);

// Create a batch for a reader from the uploaded batch and a replicated
// placeholder batch header.
model::record_batch apply_placeholder_to_batch(
  const model::record_batch_header& placeholder_batch_header,
  model::record_batch uploaded_batch);

} // namespace cloud_topics
