/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "bytes/iobuf.h"
#include "cloud_storage_clients/bucket_name_parts.h"
#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_storage_clients/types.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include <vector>

namespace cloud_storage_clients {

class client_provider;

/// S3-specific multipart upload state. Holds the connection-independent
/// protocol state (upload id and part etags) and leases a client per request
/// from the provider, so a long-running upload never pins one client.
class s3_multipart_state : public multipart_upload_state {
public:
    s3_multipart_state(
      ss::shared_ptr<client_provider> provider,
      plain_bucket_name bucket,
      object_key key,
      ss::lowres_clock::duration timeout);

    ss::future<> initialize_multipart() override;
    ss::future<> upload_part(size_t part_num, iobuf data) override;
    ss::future<> complete_multipart_upload() override;
    ss::future<> abort_multipart_upload() override;
    ss::future<> upload_as_single_object(iobuf data) override;

    bool is_multipart_initialized() const override {
        return !_upload_id.empty();
    }

    ss::sstring upload_id() const override { return _upload_id; }

private:
    ss::shared_ptr<client_provider> _provider;
    plain_bucket_name _bucket;
    object_key _key;
    ss::lowres_clock::duration _timeout;
    ss::sstring _upload_id;
    std::vector<ss::sstring> _etags;
};

} // namespace cloud_storage_clients
