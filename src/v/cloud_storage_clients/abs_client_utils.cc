/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/abs_client_utils.h"

#include "base/vlog.h"
#include "bytes/streambuf.h"
#include "cloud_storage_clients/logger.h"
#include "json/document.h"
#include "json/istreamwrapper.h"
#include "ssx/sformat.h"
#include "utils/xml.h"

#include <exception>
#include <istream>
#include <optional>
#include <stdexcept>

namespace cloud_storage_clients {

namespace {

abs_rest_error_response
parse_xml_rest_error_response(boost::beast::http::status result, iobuf buf) {
    try {
        auto resp = xml::iobuf_to_ptree(std::move(buf), abs_log);
        auto code = xml::get_from_ptree<std::string>(
          resp, "Error.Code", "Unknown");
        auto msg = xml::get_from_ptree<std::string>(resp, "Error.Message", "");
        return {code, msg, result};
    } catch (...) {
        vlog(
          abs_log.error,
          "Failed to parse ABS error response {}",
          std::current_exception());
        throw;
    }
}

abs_rest_error_response
parse_json_rest_error_response(boost::beast::http::status result, iobuf buf) {
    iobuf_istreambuf strbuf{buf};
    std::istream stream{&strbuf};
    json::IStreamWrapper wrapper{stream};

    json::Document doc;
    if (doc.ParseStream(wrapper).HasParseError()) {
        vlog(
          abs_log.error,
          "Failed to parse ABS error response: {}",
          doc.GetParseError());

        throw std::runtime_error(
          ssx::sformat(
            "Failed to parse JSON ABS error response: {}",
            doc.GetParseError()));
    }

    std::optional<ss::sstring> code;
    std::optional<ss::sstring> member;
    if (auto error_it = doc.FindMember("error"); error_it != doc.MemberEnd()) {
        const auto& error = error_it->value;
        if (
          auto code_it = error.FindMember("code");
          code_it != error.MemberEnd()) {
            code = code_it->value.GetString();
        }

        if (
          auto member_it = error.FindMember("member");
          member_it != error.MemberEnd()) {
            member = member_it->value.GetString();
        }
    }

    return {code.value_or("Unknown"), member.value_or(""), result};
}

} // namespace

abs_rest_error_response parse_rest_error_response(
  response_content_type type, boost::beast::http::status result, iobuf buf) {
    if (type == response_content_type::xml) {
        return parse_xml_rest_error_response(result, std::move(buf));
    }

    if (type == response_content_type::json) {
        return parse_json_rest_error_response(result, std::move(buf));
    }

    return abs_rest_error_response{"Unknown", "", result};
}

} // namespace cloud_storage_clients
