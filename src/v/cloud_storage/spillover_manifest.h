/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/partition_manifest.h"
#include "model/metadata.h"

#include <stdexcept>

namespace cloud_storage {

struct spillover_manifest_path_components {
    model::offset base;
    model::offset last;
    kafka::offset base_kafka;
    kafka::offset next_kafka;
    model::timestamp base_ts;
    model::timestamp last_ts;
};

inline std::ostream&
operator<<(std::ostream& o, const spillover_manifest_path_components& c) {
    fmt::print(
      o,
      "{{base: {}, last: {}, base_kafka: {}, next_kafka: {}, base_ts: {}, "
      "last_ts: {}}}",
      c.base,
      c.last,
      c.base_kafka,
      c.next_kafka,
      c.base_ts,
      c.last_ts);
    return o;
}

namespace {

remote_manifest_path generate_spillover_manifest_path(
  const model::ntp& ntp,
  model::initial_revision_id rev,
  const spillover_manifest_path_components& c) {
    auto path = generate_partition_manifest_path(
      ntp, rev, manifest_format::serde);
    // Given the topic name size limit the name should fit into
    // the AWS S3 size limit.
    return remote_manifest_path(fmt::format(
      "{}.{}.{}.{}.{}.{}.{}",
      path().string(),
      c.base(),
      c.last(),
      c.base_kafka(),
      c.next_kafka(),
      c.base_ts.value(),
      c.last_ts.value()));
}
} // namespace

/// The section of the partition manifest
///
/// The only purpose of this class is to provide different implementation of the
/// 'get_manifest_path' method which includes base offset and last offset of the
/// manifest. These fields are used to collect all manifests stored in the
/// bucket. The name changes when the content of the manifest changes.
class spillover_manifest final : public partition_manifest {
public:
    spillover_manifest(const model::ntp& ntp, model::initial_revision_id rev)
      : partition_manifest(ntp, rev) {}

    remote_manifest_path get_manifest_path() const override {
        const auto ls = last_segment();
        vassert(ls.has_value(), "Spillover manifest can't be empty");
        const auto fs = *begin();
        spillover_manifest_path_components smc{
          .base = fs.base_offset,
          .last = ls->committed_offset,
          .base_kafka = fs.base_kafka_offset(),
          .next_kafka = ls->next_kafka_offset(),
          .base_ts = fs.base_timestamp,
          .last_ts = ls->max_timestamp,
        };
        return generate_spillover_manifest_path(
          get_ntp(), get_revision_id(), smc);
    }
};

} // namespace cloud_storage