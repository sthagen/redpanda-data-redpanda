/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/state.h"

#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/named_type.h"
#include "serde/rw/optional.h"
#include "serde/rw/sstring.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

namespace {
std::deque<volatile_row> copy_rows(const std::deque<volatile_row>& rows) {
    std::deque<volatile_row> copy;
    for (const auto& r : rows) {
        copy.push_back(
          volatile_row{
            .seqno = r.seqno,
            .row = write_batch_row{
              .key = r.row.key, .value = r.row.value.copy()}});
    }
    return copy;
}
} // namespace

lsm_state::serialized_manifest lsm_state::serialized_manifest::copy() const {
    return serialized_manifest{
      .buf = buf.copy(),
      .last_seqno = last_seqno,
      .database_epoch = database_epoch,
    };
}

lsm_state lsm_state::copy() const {
    std::optional<serialized_manifest> manifest_copy;
    if (persisted_manifest.has_value()) {
        manifest_copy = persisted_manifest->copy();
    }
    return lsm_state{
      .domain_uuid = domain_uuid,
      .seqno_delta = seqno_delta,
      .db_epoch_delta = db_epoch_delta,
      .volatile_buffer = copy_rows(volatile_buffer),
      .persisted_manifest = std::move(manifest_copy),
    };
}

} // namespace cloud_topics::l1
