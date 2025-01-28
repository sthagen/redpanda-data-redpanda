/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/action.h"

namespace iceberg {

std::ostream& operator<<(std::ostream& o, action::errc e) {
    switch (e) {
        using enum action::errc;
    case unexpected_state:
        return o << "action::errc::unexpected_state";
    case io_failed:
        return o << "action::errc::io_failed";
    case shutting_down:
        return o << "action::errc::shutting_down";
    }
}

} // namespace iceberg
