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

#include "base/format_to.h"
#include "base/seastarx.h"
#include "base/vassert.h"
#include "reflection/adl.h"
#include "serde/envelope.h"

#include <seastar/core/sstring.hh>

namespace cluster {

/// Relic of the removed data-policy/v8_engine feature. Kept only because it
/// is part of the serialization format of incremental_topic_custom_updates.
struct data_policy
  : public serde::
      envelope<data_policy, serde::version<0>, serde::compat_version<0>> {
    static constexpr int8_t version{1};

    data_policy() = default;
    data_policy(ss::sstring fn, ss::sstring sn) noexcept
      : fn_name(std::move(fn))
      , sct_name(std::move(sn)) {}

    const ss::sstring& function_name() const { return fn_name; }
    const ss::sstring& script_name() const { return sct_name; }

    friend bool operator==(const data_policy&, const data_policy&) = default;
    auto serde_fields() { return std::tie(fn_name, sct_name); }

    ss::sstring fn_name;
    ss::sstring sct_name;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "function_name: {} script_name: {}",
          function_name(),
          script_name());
    }
};

} // namespace cluster

namespace reflection {

template<>
struct adl<cluster::data_policy> {
    void to(iobuf& out, cluster::data_policy&& dp) {
        reflection::serialize(
          out, dp.version, dp.function_name(), dp.script_name());
    }
    cluster::data_policy from(iobuf_parser& in) {
        auto version = reflection::adl<int8_t>{}.from(in);
        vassert(
          version == cluster::data_policy::version,
          "Unexpected data_policy version");
        auto function_name = reflection::adl<ss::sstring>{}.from(in);
        auto script_name = reflection::adl<ss::sstring>{}.from(in);
        return cluster::data_policy(
          std::move(function_name), std::move(script_name));
    }
};

} // namespace reflection
