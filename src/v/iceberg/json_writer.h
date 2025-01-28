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

#include "json/chunked_buffer.h"
#include "json/iobuf_writer.h"
#include "strings/utf8.h"

namespace iceberg {

using json_writer = json::iobuf_writer<json::chunked_buffer>;

// NOTE: uses std::string since we're likely parsing JSON alongside a
// thirdparty library that uses std::string.
template<typename T>
std::string to_json_str(const T& t) {
    json::chunked_buffer buf;
    iceberg::json_writer w(buf);
    rjson_serialize(w, t);
    auto p = iobuf_parser(std::move(buf).as_iobuf());
    std::string str;
    str.resize(p.bytes_left());
    p.consume_to(p.bytes_left(), str.data());
    validate_utf8(str);
    return str;
}

} // namespace iceberg
