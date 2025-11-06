/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "cluster_link/errc.h"
#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>

#include <expected>
#include <utility>
namespace admin {

/**
 * @brief Will throw the appropriate RPC exception based on the error code
 *
 * @note Will assert if provided `cluster_link::errc::success`
 */
void handle_error(cluster_link::errc err, ss::sstring info);

/**
 * @brief Will extract the result and either return the successful value or
 * throw an exception on error
 */
template<typename T>
T handle_error(cluster_link::cl_result<T> result) {
    if (result.has_value()) {
        return std::move(result).assume_value();
    }

    auto info = result.assume_error();
    auto code = info.code();
    handle_error(code, std::move(info).message());

    // Handle error must throw
    std::unreachable();
}

/**
 * @brief Will extract the result and either return the successful value or
 * throw an exception on error
 */
template<typename T>
T handle_error(std::expected<T, cluster_link::errc> result) {
    if (result.has_value()) {
        return std::move(result).value();
    }

    handle_error(result.error(), ssx::sformat("{}", result.error()));

    // Handle error must throw
    std::unreachable();
}
} // namespace admin
