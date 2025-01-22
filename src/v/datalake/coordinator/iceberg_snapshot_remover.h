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

#include <seastar/core/future.hh>

namespace datalake::coordinator {

class iceberg_snapshot_remover : public snapshot_remover {
public:
    iceberg_snapshot_remover(
      iceberg::catalog& catalog, iceberg::manifest_io& io)
      : catalog_(catalog)
      , io_(io) {}
    ~iceberg_snapshot_remover() override = default;

    ss::future<checked<std::nullopt_t, errc>>
      remove_expired_snapshots(model::topic, model::timestamp) const final;

private:
    // TODO: pull this out into some helper? Seems useful for other actions.
    iceberg::table_identifier table_id_for_topic(const model::topic& t) const;

    // Must outlive this remover.
    iceberg::catalog& catalog_;
    iceberg::manifest_io& io_;
};

} // namespace datalake::coordinator
