// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/leaders_preference.h"

#include <boost/algorithm/string.hpp>
#include <fmt/format.h>

#include <ranges>
#include <unordered_set>

namespace config {

fmt::iterator leaders_preference::format_to(fmt::iterator it) const {
    auto prefix = type_to_prefix(type);
    return fmt::format_to(it, "{}{}", prefix, fmt::join(racks, ","));
}

leaders_preference leaders_preference::parse(std::string_view s) {
    leaders_preference::type_t found_type{leaders_preference::type_t::none};
    size_t prefix_length{0};
    if (s == leaders_preference::none_prefix) {
        return leaders_preference{};
    } else if (s.starts_with(racks_prefix)) {
        found_type = leaders_preference::type_t::racks;
        prefix_length = racks_prefix.length();
    } else if (s.starts_with(ordered_racks_prefix)) {
        found_type = leaders_preference::type_t::ordered_racks;
        prefix_length = ordered_racks_prefix.length();
    } else {
        throw std::runtime_error(
          "couldn't parse leaders_preference: should be "
          "\"none\" or start with \"racks:\" or \"ordered_racks:\"");
    }

    std::vector<ss::sstring> tokens;
    boost::algorithm::split(
      tokens, s.substr(prefix_length), [](char c) { return c == ','; });

    leaders_preference ret;
    ret.type = found_type;
    ret.racks.reserve(tokens.size());
    for (auto& tok : tokens) {
        boost::algorithm::trim(tok);
        if (tok.empty()) {
            throw std::runtime_error(
              "couldn't parse leaders_preference: "
              "empty rack token");
        }
        ret.racks.emplace_back(std::move(tok));
    }

    // back compat for unordered, but enforce no duplicates for ordered
    if (ret.type == type_t::ordered_racks) {
        auto no_duplicates_set = std::unordered_set<model::rack_id>{
          std::from_range, ret.racks};
        if (no_duplicates_set.size() != ret.racks.size()) {
            throw std::runtime_error(
              "couldn't parse leaders_preference: preference list should not "
              "contain duplicates");
        }
    }
    return ret;
}

std::istream& operator>>(std::istream& is, leaders_preference& res) {
    std::stringstream ss;
    ss << is.rdbuf();
    res = leaders_preference::parse(ss.str());
    return is;
}

} // namespace config
