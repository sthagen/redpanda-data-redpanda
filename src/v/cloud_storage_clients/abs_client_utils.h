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
#include "cloud_storage_clients/abs_error.h"
#include "cloud_storage_clients/util.h"

#include <boost/beast/http/status.hpp>

namespace cloud_storage_clients {

abs_rest_error_response parse_rest_error_response(
  response_content_type type, boost::beast::http::status result, iobuf buf);

} // namespace cloud_storage_clients
