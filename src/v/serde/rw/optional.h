// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "serde/rw/rw.h"

#include <optional>

namespace serde {

template<typename O>
requires requires {
    []<typename T>(std::optional<T>) {}(std::declval<std::remove_cvref_t<O>>());
}
void tag_invoke(tag_t<write_tag>, iobuf& out, O&& t) {
    if (t) {
        write(out, true);
        write(out, std::forward_like<O>(t.value()));
    } else {
        write(out, false);
    }
}

template<typename T>
void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  std::optional<T>& t,
  const std::size_t bytes_left_limit) {
    t = read_nested<bool>(in, bytes_left_limit)
          ? std::optional{read_nested<T>(in, bytes_left_limit)}
          : std::nullopt;
}

} // namespace serde
