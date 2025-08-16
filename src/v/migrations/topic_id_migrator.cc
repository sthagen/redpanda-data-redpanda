// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "migrations/topic_id_migrator.h"

#include "base/vlog.h"
#include "cluster/controller.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "features/logger.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"

namespace features::migrators {

ss::future<> topic_id_migrator::do_mutate() {
    vlog(featureslog.info, "Checking for topics to update...");

    auto& tt = _controller.get_topics_state().local();
    auto& tf = _controller.get_topics_frontend().local();

    cluster::topic_properties_update_vector upd_vec;
    for (const auto& topic : tt.all_topics_metadata()) {
        if (!topic.second.get_configuration().tp_id.has_value()) {
            auto upd = cluster::topic_properties_update{topic.first};
            upd.properties.topic_id.op
              = cluster::incremental_update_operation::set;
            upd.properties.topic_id.value = model::create_topic_id();
            upd_vec.emplace_back(std::move(upd));
        }
    }

    vlog(featureslog.info, "Assigning UUIDs to {} topics", upd_vec.size());
    auto res = co_await tf.update_topic_properties(
      std::move(upd_vec), model::time_from_now(30s));

    for (const auto& topic_res : res) {
        if (topic_res.ec != cluster::errc::success) {
            throw std::runtime_error(
              fmt::format(
                "Failed to update topic {}: {}",
                topic_res.tp_ns,
                topic_res.ec));
        }
    }

    vlog(
      featureslog.info, "Successfully assigned a UUID to all existing topics");
}

} // namespace features::migrators
