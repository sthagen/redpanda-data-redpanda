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

#include "base/seastarx.h"
#include "datalake/coordinator/snapshot_remover.h"
#include "iceberg/catalog.h"
#include "iceberg/manifest_io.h"
#include "iceberg/table_identifier.h"

#include <seastar/core/future.hh>

namespace datalake::coordinator {

class iceberg_snapshot_remover : public snapshot_remover {
public:
    iceberg_snapshot_remover(
      iceberg::catalog& catalog, iceberg::manifest_io& io)
      : catalog_(catalog)
      , io_(io) {}
    ~iceberg_snapshot_remover() override = default;

    ss::future<checked<std::nullopt_t, errc>> remove_expired_snapshots(
      model::topic, const topics_state&, model::timestamp) const final;

private:
    // Remove snapshots that have expired as of `ts` for the given table within
    // the context of the given topic. Is a no-op if the table doesn't exist.
    ss::future<checked<std::nullopt_t, errc>> remove_expired_snapshots(
      model::topic, iceberg::table_identifier, model::timestamp ts) const;

    // TODO: pull this out into some helper? Seems useful for other actions.
    iceberg::table_identifier table_id_for_topic(const model::topic& t) const;
    iceberg::table_identifier
    dlq_table_id_for_topic(const model::topic& t) const;

    // Must outlive this remover.
    iceberg::catalog& catalog_;
    iceberg::manifest_io& io_;
};

} // namespace datalake::coordinator
