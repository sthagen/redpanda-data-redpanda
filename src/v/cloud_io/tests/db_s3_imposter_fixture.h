/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cloud_storage_clients/configuration.h"
#include "lsm/lsm.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/http/httpd.hh>

#include <filesystem>
#include <memory>

/// \brief S3 mock backed by an LSM database on disk.
///
/// Drop-in replacement for s3_imposter_fixture when tests need working
/// S3 storage without request tracking or failure injection. Objects
/// are stored in SST files via the LSM engine, removing the memory
/// scaling bottleneck of the in-memory map.
class db_s3_imposter_fixture {
public:
    db_s3_imposter_fixture();
    ~db_s3_imposter_fixture() noexcept;

    db_s3_imposter_fixture(const db_s3_imposter_fixture&) = delete;
    db_s3_imposter_fixture& operator=(const db_s3_imposter_fixture&) = delete;
    db_s3_imposter_fixture(db_s3_imposter_fixture&&) = delete;
    db_s3_imposter_fixture& operator=(db_s3_imposter_fixture&&) = delete;

    ss::future<> start();
    ss::future<> stop();

    cloud_storage_clients::s3_configuration conf;
    const cloud_storage_clients::bucket_name bucket_name{"test-bucket"};

private:
    struct handler;
    struct forwarding_handler;

    std::filesystem::path _data_dir;
    std::optional<lsm::database> _db;
    lsm::sequence_number _seqno{0};
    ss::shared_ptr<ss::httpd::http_server_control> _server;
    std::vector<std::unique_ptr<ss::httpd::handler_base>> _handlers;
};
