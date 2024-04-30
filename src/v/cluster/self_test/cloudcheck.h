/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/self_test/metrics.h"
#include "cluster/self_test_rpc_types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>

#include <exception>
#include <optional>
#include <vector>

namespace cluster::self_test {

class cloudcheck_exception : public std::runtime_error {
public:
    explicit cloudcheck_exception(const std::string& msg)
      : std::runtime_error(msg) {}
};

class cloudcheck {
public:
    cloudcheck(ss::sharded<cloud_storage::remote>& cloud_storage_api);

    /// Initialize the benchmark
    ss::future<> start();

    /// Stops the benchmark
    ss::future<> stop();

    /// Sets member variables and runs the cloud benchmark.
    ss::future<std::vector<self_test_result>> run(cloudcheck_opts opts);

    /// Signal to stop all work as soon as possible
    ///
    /// Immediately returns, waiter can expect to wait on the results to be
    /// returned by \run to be available shortly
    void cancel();

private:
    // Invokes the various cloud storage operations for testing.
    ss::future<std::vector<self_test_result>> run_benchmarks();

    // Make a random payload to be uploaded to cloud storage, and to be verified
    // once it is read back.
    iobuf make_random_payload(size_t size = 1024) const;

    // Generate an upload request.
    cloud_storage::upload_request make_upload_request(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage_clients::object_key& key,
      iobuf payload,
      retry_chain_node& rtc);

    // Generate a download request.
    cloud_storage::download_request make_download_request(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage_clients::object_key& key,
      iobuf& payload,
      retry_chain_node& rtc);

    // Verify that uploading (write operation) to cloud storage works.
    ss::future<self_test_result> verify_upload(
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key key,
      const std::optional<iobuf>& payload);

    // Verify that listing (read operation) from cloud storage works.
    ss::future<std::pair<cloud_storage::remote::list_result, self_test_result>>
    verify_list(
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix);

    // Verify that downloading (read operation) from cloud storage works.
    ss::future<std::pair<std::optional<iobuf>, self_test_result>>
    verify_download(
      cloud_storage_clients::bucket_name bucket,
      std::optional<cloud_storage_clients::object_key> key);

    // Verify that deleting (write operation) from cloud storage works.
    ss::future<self_test_result> verify_delete(
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key key);

private:
    bool _remote_read_enabled{false};
    bool _remote_write_enabled{false};
    bool _cancelled{false};
    ss::abort_source _as;
    ss::gate _gate;
    retry_chain_node _rtc;
    cloudcheck_opts _opts;

private:
    ss::sharded<cloud_storage::remote>& _cloud_storage_api;
};

} // namespace cluster::self_test
