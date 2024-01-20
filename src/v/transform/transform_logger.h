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

#pragma once

#include "base/seastarx.h"
#include "wasm/api.h"

#include <seastar/util/log.hh>

namespace transform {
class logger final : public wasm::logger {
public:
    logger() = delete;
    explicit logger(model::transform_name name, ss::logger* log)
      : _name(std::move(name))
      , _log(log) {}
    ~logger() override = default;
    logger(const logger&) = delete;
    logger& operator=(const logger&) = delete;
    logger(logger&&) = delete;
    logger& operator=(logger&&) = delete;

    void log(ss::log_level lvl, std::string_view message) noexcept override {
        _log->log(lvl, "{} - {}", _name(), message);
    }

private:
    model::transform_name _name;
    ss::logger* _log;
};
} // namespace transform
