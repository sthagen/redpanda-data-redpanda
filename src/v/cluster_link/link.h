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

#include "cluster_link/model/types.h"

namespace cluster_link {
/**
 * @brief The link class represents a link between two clusters
 */
class link {
public:
    explicit link(model::metadata config);
    link(const link&) = delete;
    link(link&&) = delete;
    link& operator=(const link&) = delete;
    link& operator=(link&&) = delete;
    virtual ~link() = default;

    virtual ss::future<> start();
    virtual ss::future<> stop();

    void update_config(model::metadata);

    const model::metadata& config() const;

private:
    model::metadata _config;
};
} // namespace cluster_link
