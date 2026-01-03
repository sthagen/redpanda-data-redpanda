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

#include "container/chunked_hash_map.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/table_cache.h"
#include "lsm/io/persistence.h"
#include "ssx/actor.h"

namespace lsm::db {

// A request to the GC actor to remove old unused files.
struct gc_message {
    // The ID of the highest file that has been committed to the database.
    // Files with higher IDs are ignored as they could be currently built.
    internal::file_id highest_used_file_id;
    // All the of the live files in the database at this time.
    chunked_hash_set<internal::file_handle> live_files;
};

// An actor to remove old files from the database that are no longer being used.
//
// This actor only has a mailbox size of 1 and drops old requests when new ones
// come in because new requests will be inclusive of old ones.
class gc_actor
  : public ssx::actor<gc_message, 1, ssx::overflow_policy::drop_oldest> {
public:
    gc_actor(
      io::data_persistence* persistence,
      ss::lw_shared_ptr<internal::options> opts,
      table_cache* table_cache)
      : _persistence(persistence)
      , _opts(std::move(opts))
      , _table_cache(table_cache) {}

    size_t pending_delete_count() const { return _pending_deletes.size(); }

protected:
    ss::future<> process(gc_message msg) override;

    void on_error(std::exception_ptr ex) noexcept override;

private:
    io::data_persistence* _persistence;
    ss::lw_shared_ptr<internal::options> _opts;
    table_cache* _table_cache;
    chunked_hash_map<internal::file_handle, ss::lowres_clock::time_point>
      _pending_deletes;
};

} // namespace lsm::db
