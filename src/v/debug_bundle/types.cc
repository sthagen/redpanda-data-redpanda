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

#include "types.h"

#include "absl/strings/str_split.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "re2/re2.h"
#include "re2/stringpiece.h"
#include "strings/string_switch.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/chrono.h>
#include <fmt/core.h>

#include <charconv>
#include <system_error>

namespace debug_bundle {

std::istream& operator>>(std::istream& i, special_date& d) {
    ss::sstring s;
    i >> s;
    d = string_switch<special_date>(s)
          .match(
            to_string_view(special_date::yesterday), special_date::yesterday)
          .match(to_string_view(special_date::today), special_date::today)
          .match(to_string_view(special_date::now), special_date::now)
          .match(
            to_string_view(special_date::tomorrow), special_date::tomorrow);
    return i;
}

std::optional<partition_selection>
partition_selection::from_string_view(std::string_view str) {
    try {
        re2::RE2 pattern{
          R"(^(?:(?P<namespace>[a-zA-Z0-9._-]+)/)?(?P<topic>[a-zA-Z0-9._-]+)/(?P<partitions>\d+(?:,\d+)*)$)"};

        re2::StringPiece ns;
        re2::StringPiece tp;
        re2::StringPiece parts_str;

        if (RE2::FullMatch(str, pattern, &ns, &tp, &parts_str)) {
            partition_selection ps;
            ps.tn = {
              ns.empty() ? model::kafka_namespace : model::ns{ns},
              model::topic{tp}};

            std::string_view parts_sv{parts_str.data(), parts_str.length()};
            for (const auto& part : absl::StrSplit(parts_sv, ',')) {
                model::partition_id::type part_id{};
                auto [_, ec] = std::from_chars(
                  part.data(), part.data() + part.size(), part_id);
                if (ec != std::errc()) {
                    return std::nullopt;
                }
                ps.partitions.emplace(part_id);
            }
            return std::move(ps);
        }
    } catch (const std::exception&) {
    }
    return std::nullopt;
}

} // namespace debug_bundle
