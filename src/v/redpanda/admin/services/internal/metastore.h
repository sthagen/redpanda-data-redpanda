/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "cluster/topic_table.h"
#include "proto/redpanda/core/admin/internal/cloud_topics/v1/metastore.proto.h"

#include <seastar/core/distributed.hh>

namespace admin {

class metastore_service_impl
  : public proto::admin::metastore::metastore_service {
public:
    explicit metastore_service_impl(
      ss::sharded<cloud_topics::l1::replicated_metastore>* m,
      ss::sharded<cluster::topic_table>* tt)
      : _topic_table(tt)
      , _metastore(m) {}

    seastar::future<proto::admin::metastore::get_offsets_response> get_offsets(
      serde::pb::rpc::context,
      proto::admin::metastore::get_offsets_request) override;

private:
    ss::sharded<cluster::topic_table>* _topic_table;
    ss::sharded<cloud_topics::l1::replicated_metastore>* _metastore;
};

} // namespace admin
