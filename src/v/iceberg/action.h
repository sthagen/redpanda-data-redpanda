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

#include "base/format_to.h"
#include "base/outcome.h"
#include "base/seastarx.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"

namespace iceberg {

struct updates_and_reqs {
    chunked_vector<table_update::update> updates;
    chunked_vector<table_requirement::requirement> requirements;
};

class action {
public:
    enum class errc {
        // An invariant has been broken with some state, e.g. some ID was
        // missing that we expected to exist. May indicate an issue with
        // persisted metadata, or with uncommitted transaction state.
        unexpected_state,

        // IO failed while perfoming the action.
        // TODO: worth distinguishing from corruption?
        io_failed,

        // We're shutting down.
        shutting_down,
    };
    using action_outcome = checked<updates_and_reqs, errc>;
    // Constructs the updates and requirements needed to perform the given
    // action to the table metadata. Expected to be called once only.
    virtual ss::future<action_outcome> build_updates() && = 0;

    virtual ~action() = default;
};
inline fmt::iterator format_to(action::errc e, fmt::iterator out) {
    switch (e) {
    case action::errc::unexpected_state:
        return fmt::format_to(out, "action::errc::unexpected_state");
    case action::errc::io_failed:
        return fmt::format_to(out, "action::errc::io_failed");
    case action::errc::shutting_down:
        return fmt::format_to(out, "action::errc::shutting_down");
    }
}

} // namespace iceberg
