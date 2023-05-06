/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "storage/api.h"

namespace storage {

ss::future<usage_report> api::disk_usage() {
    co_return co_await container().map_reduce0(
      [](api& api) {
          return ss::when_all_succeed(
                   api._log_mgr->disk_usage(), api._kvstore->disk_usage())
            .then([](std::tuple<usage_report, usage_report> usage) {
                const auto& [disk, kvs] = usage;
                return disk + kvs;
            });
      },
      usage_report{},
      [](usage_report acc, usage_report update) { return acc + update; });
}

} // namespace storage
