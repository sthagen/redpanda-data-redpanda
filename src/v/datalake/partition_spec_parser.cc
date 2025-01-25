/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/partition_spec_parser.h"

namespace datalake {

namespace {

template<typename T>
struct parse_result {
    T val;
    std::string_view unparsed;
};

struct parse_ctx {
    std::string_view original;
    ss::sstring last_error;

    void report_expected(std::string_view unparsed, std::string_view expected) {
        last_error = fmt::format(
          "col {}: expected {} (got instead: `{}')",
          unparsed.data() - original.data(),
          expected,
          unparsed);
    }
};

bool skip_space(std::string_view& str) {
    auto it = str.begin();
    while (it != str.end() && std::isspace(*it)) {
        ++it;
    }
    const bool skipped = it != str.begin();
    str = std::string_view{it, str.end()};
    return skipped;
}

bool skip_expected(std::string_view& str, const std::string_view& expected) {
    if (!str.starts_with(expected)) {
        return false;
    }
    str.remove_prefix(expected.length());
    return true;
}

std::optional<parse_result<ss::sstring>>
parse_identifier(const std::string_view& str) {
    auto it = str.begin();
    // TODO: implement quoted identifiers and case-insensitivity
    while (it != str.end() && (*it == '_' || std::isalnum(*it))) {
        ++it;
    }

    if (it == str.begin()) {
        return std::nullopt;
    }

    return parse_result<ss::sstring>{
      .val = ss::sstring{str.begin(), it},
      .unparsed = std::string_view{it, str.end()},
    };
}

std::optional<parse_result<std::vector<ss::sstring>>>
parse_qualified_identifier(const std::string_view& str) {
    auto unparsed = str;

    std::vector<ss::sstring> result;
    while (true) {
        if (!result.empty()) {
            if (!skip_expected(unparsed, ".")) {
                break;
            }
        }

        auto id = parse_identifier(unparsed);
        if (!id) {
            break;
        }
        result.push_back(id->val);
        unparsed = id->unparsed;
    }

    if (result.empty()) {
        return std::nullopt;
    }

    return parse_result<std::vector<ss::sstring>>{
      .val = std::move(result),
      .unparsed = unparsed,
    };
}

struct transform_field {
    std::vector<ss::sstring> source;
    iceberg::transform transform;
};

std::optional<parse_result<transform_field>>
parse_transform_field(const std::string_view& str, parse_ctx& ctx) {
    auto unparsed = str;

    auto transform_id = parse_identifier(unparsed);
    if (!transform_id) {
        return std::nullopt;
    }
    iceberg::transform transform;
    if (transform_id->val == "hour") {
        transform = iceberg::hour_transform{};
    } else if (transform_id->val == "day") {
        transform = iceberg::day_transform{};
    } else if (transform_id->val == "identity") {
        transform = iceberg::identity_transform{};
    } else {
        // TODO: parse all transforms
        ctx.report_expected(unparsed, "known transform name");
        return std::nullopt;
    }
    unparsed = transform_id->unparsed;

    skip_space(unparsed);
    if (!skip_expected(unparsed, "(")) {
        ctx.report_expected(unparsed, "'('");
        return std::nullopt;
    }

    auto source = parse_qualified_identifier(unparsed);
    if (!source) {
        ctx.report_expected(unparsed, "qualified identifier");
        return std::nullopt;
    }
    unparsed = source->unparsed;

    skip_space(unparsed);
    if (!skip_expected(unparsed, ")")) {
        ctx.report_expected(unparsed, "')'");
        return std::nullopt;
    }

    auto result = transform_field{
      .source = std::move(source->val),
      .transform = transform,
    };

    return parse_result<transform_field>{
      .val = std::move(result),
      .unparsed = unparsed,
    };
}

std::optional<parse_result<iceberg::unresolved_partition_spec::field>>
parse_partition_field(const std::string_view& str, parse_ctx& ctx) {
    auto unparsed = str;
    skip_space(unparsed);

    transform_field tf;
    if (auto parsed_tf = parse_transform_field(unparsed, ctx); parsed_tf) {
        tf = std::move(parsed_tf->val);
        unparsed = parsed_tf->unparsed;
    } else if (auto parsed_sf = parse_qualified_identifier(unparsed);
               parsed_sf) {
        tf.source = std::move(parsed_sf->val);
        tf.transform = iceberg::identity_transform{};
        unparsed = parsed_sf->unparsed;
    } else {
        ctx.report_expected(unparsed, "qualified identifier or transform");
        return std::nullopt;
    }

    ss::sstring source_field_str;
    if (
      skip_space(unparsed)
      && (skip_expected(unparsed, "AS") || skip_expected(unparsed, "as"))) {
        if (!skip_space(unparsed)) {
            ctx.report_expected(unparsed, "whitespace");
            return std::nullopt;
        }

        auto id = parse_identifier(unparsed);
        if (!id) {
            ctx.report_expected(unparsed, "identifier");
            return std::nullopt;
        }
        source_field_str = std::move(id->val);
        unparsed = id->unparsed;
    } else {
        source_field_str = fmt::format("{}", fmt::join(tf.source, "."));
        if (tf.transform != iceberg::identity_transform{}) {
            source_field_str += fmt::format("_{}", tf.transform);
        }
    }

    iceberg::unresolved_partition_spec::field val{
      .source_name = std::move(tf.source),
      .transform = tf.transform,
      .name = std::move(source_field_str),
    };

    return parse_result<iceberg::unresolved_partition_spec::field>{
      .val = std::move(val),
      .unparsed = unparsed,
    };
}

std::optional<parse_result<iceberg::unresolved_partition_spec>>
parse_partition_field_list(const std::string_view& str, parse_ctx& ctx) {
    auto unparsed = str;
    skip_space(unparsed);

    if (!skip_expected(unparsed, "(")) {
        ctx.report_expected(unparsed, "'('");
        return std::nullopt;
    }

    iceberg::unresolved_partition_spec result;
    while (true) {
        if (!result.fields.empty()) {
            skip_space(unparsed);
            if (!skip_expected(unparsed, ",")) {
                ctx.report_expected(unparsed, ",");
                break;
            }
        }

        auto field = parse_partition_field(unparsed, ctx);
        if (!field) {
            break;
        }
        result.fields.push_back(field->val);
        unparsed = field->unparsed;
    }

    skip_space(unparsed);
    if (!skip_expected(unparsed, ")")) {
        ctx.report_expected(unparsed, "')'");
        return std::nullopt;
    }

    return parse_result<iceberg::unresolved_partition_spec>{
      .val = std::move(result),
      .unparsed = unparsed,
    };
}

} // namespace

checked<iceberg::unresolved_partition_spec, ss::sstring>
parse_partition_spec(const std::string_view& str) {
    parse_ctx ctx{.original = str};
    auto res = parse_partition_field_list(str, ctx);
    if (!res) {
        return ctx.last_error;
    }
    skip_space(res->unparsed);
    if (!res->unparsed.empty()) {
        return fmt::format("unparsed: `{}'", res->unparsed);
    }
    return std::move(res->val);
}

} // namespace datalake
