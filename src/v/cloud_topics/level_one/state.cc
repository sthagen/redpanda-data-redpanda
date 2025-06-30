/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/state.h"

#include "model/namespace.h"

namespace experimental::cloud_topics::l1 {

std::optional<std::reference_wrapper<const partition_state>>
state::partition_state(const model::topic_id_partition& tidp) const {
    auto state_iter = topic_to_state.find(tidp.topic_id);
    if (state_iter == topic_to_state.end()) {
        return std::nullopt;
    }
    const auto& topic_state = state_iter->second;
    auto prt_iter = topic_state.pid_to_state.find(tidp.partition);
    if (prt_iter == topic_state.pid_to_state.end()) {
        return std::nullopt;
    }
    return prt_iter->second;
}

topic_state topic_state::copy() const {
    topic_state res;
    for (const auto& [p, s] : pid_to_state) {
        res.pid_to_state[p] = s;
    }
    return res;
}

state state::copy() const {
    state res;
    for (const auto& [t, s] : topic_to_state) {
        res.topic_to_state[t] = s.copy();
    }
    for (const auto& [o, e] : objects) {
        res.objects[o] = e;
    }
    return res;
}

} // namespace experimental::cloud_topics::l1
