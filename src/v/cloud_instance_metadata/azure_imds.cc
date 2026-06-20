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

#include "cloud_instance_metadata/azure_imds.h"

#include <boost/beast/http/verb.hpp>

namespace cloud_instance_metadata::azure_imds {

boost::beast::http::request_header<> get(std::string_view path) {
    boost::beast::http::request_header<> req;
    req.method(boost::beast::http::verb::get);
    req.target({path.data(), path.size()});
    req.set(
      {metadata_header.data(), metadata_header.size()},
      {metadata_value.data(), metadata_value.size()});
    return req;
}

} // namespace cloud_instance_metadata::azure_imds
