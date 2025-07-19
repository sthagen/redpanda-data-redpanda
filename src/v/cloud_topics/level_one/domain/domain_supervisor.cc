/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/domain/domain_supervisor.h"

#include "cloud_topics/logger.h"
#include "cluster/controller.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"

#include <seastar/coroutine/as_future.hh>

using namespace std::chrono_literals;

namespace experimental::cloud_topics::l1 {

class domain_supervisor::impl {
    // Intentionally under the ss::sstring SSO size.
    static constexpr model::topic_view domain_topic_name = model::topic_view{
      std::string_view{"ct_l1_domain"}};

public:
    explicit impl(cluster::controller* controller)
      : _controller(controller) {}

    ss::future<> start() {
        if (ss::this_shard_id() == 0) {
            _as = {};
            _loop = do_topic_reconciliation_loop();
        }
        // TODO(cloud-topics): We should also create domain supervisors if this
        // shard owns a domain partition.
        co_return;
    }

    ss::future<> stop() {
        if (ss::this_shard_id() == 0 && _loop) {
            _as.request_abort();
            co_await *std::exchange(_loop, std::nullopt);
        }
    }

private:
    ss::future<> do_topic_reconciliation_loop() {
        while (!_as.abort_requested()) {
            bool aborted = co_await loop_sleep();
            if (aborted) {
                // If we were aborted, we exit the loop.
                co_return;
            }
            if (_controller->get_topics_state().local().contains(
                  model::topic_namespace{
                    model::kafka_internal_namespace, domain_topic_name})) {
                co_await ensure_domains_replication_factor();
            } else {
                co_await create_domains_topic();
            }
        }
    }

    ss::future<bool> loop_sleep() {
        constexpr auto interval = 10min;
        simple_time_jitter<ss::lowres_clock> jitter(interval);
        try {
            co_await ss::sleep_abortable<ss::lowres_clock>(
              jitter.next_duration(), _as);
            co_return true;
        } catch (const ss::sleep_aborted& ex) {
            // do nothing, the caller will handle exiting properly.
            std::ignore = ex;
            co_return false;
        }
    }

    ss::future<> ensure_domains_replication_factor() {
        auto tp_ns = model::topic_namespace{
          model::kafka_internal_namespace, domain_topic_name};
        auto rf = _controller->get_topics_state()
                    .local()
                    .get_topic_replication_factor(tp_ns);
        if (!rf) {
            vlog(cd_log.warn, "unable to find {} replication factor", tp_ns);
            co_return;
        }
        auto target_rf = cluster::replication_factor(
          _controller->internal_topic_replication());
        if (*rf != target_rf) {
            vlog(
              cd_log.info,
              "updating {} replication factor to {}",
              tp_ns,
              target_rf);
            cluster::topic_properties_update update{tp_ns};
            update.custom_properties.replication_factor.op
              = cluster::incremental_update_operation::set;
            update.custom_properties.replication_factor.value = target_rf;
            co_await update_topic(std::move(update));
        } else {
            vlog(
              cd_log.trace, "replication factor for {} is already set", tp_ns);
        }
    }

    ss::future<> create_domains_topic() {
        auto tp_ns = model::topic_namespace{
          model::kafka_internal_namespace, domain_topic_name};
        cluster::topic_properties topic_props;
        // Mark all these as disabled
        topic_props.retention_bytes = tristate<size_t>();
        topic_props.retention_local_target_bytes = tristate<size_t>();
        topic_props.retention_duration = tristate<std::chrono::milliseconds>();
        topic_props.retention_local_target_ms
          = tristate<std::chrono::milliseconds>();
        topic_props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::none;
        // NOTE: For now we just have a single domain for the entire cluster.
        co_await create_topic(tp_ns, 1, topic_props);
    }

    ss::future<> update_topic(cluster::topic_properties_update update) {
        cluster::errc ec{};
        try {
            auto res
              = co_await _controller->get_topics_frontend()
                  .local()
                  .update_topic_properties(
                    {update},
                    ss::lowres_clock::now()
                      + config::shard_local_cfg().alter_topic_cfg_timeout_ms());
            vassert(res.size() == 1, "expected a single result");
            ec = res[0].ec;
        } catch (const std::exception& ex) {
            vlog(
              cd_log.warn, "unable to update topic {}: {}", update.tp_ns, ex);
            co_return;
        }
        if (ec != cluster::errc::success) {
            vlog(
              cd_log.warn, "failed to update topic {}: {}", update.tp_ns, ec);
        }
    }

    ss::future<> create_topic(
      model::topic_namespace_view tp_ns,
      int32_t partition_count,
      cluster::topic_properties properties) {
        cluster::topic_configuration topic_cfg(
          tp_ns.ns,
          tp_ns.tp,
          partition_count,
          _controller->internal_topic_replication());
        topic_cfg.properties = properties;

        cluster::errc ec{};
        try {
            auto res = co_await _controller->get_topics_frontend()
                         .local()
                         .autocreate_topics(
                           {std::move(topic_cfg)},
                           config::shard_local_cfg().create_topic_timeout_ms());
            vassert(res.size() == 1, "expected a single result");
            ec = res[0].ec;
        } catch (const std::exception& ex) {
            vlog(cd_log.warn, "unable to create topic {}: {}", tp_ns, ex);
            ec = cluster::errc::topic_operation_error;
        }
        if (ec != cluster::errc::success) {
            vlog(cd_log.warn, "failed to create topic {}: {}", tp_ns, ec);
        } else if (ec != cluster::errc::topic_already_exists) {
            vlog(cd_log.debug, "created topic {}", tp_ns);
        }
    }

    cluster::controller* _controller;
    std::optional<ss::future<>> _loop;
    ss::abort_source _as;
};

domain_supervisor::domain_supervisor(cluster::controller* controller)
  : _impl(std::make_unique<impl>(controller)) {}

domain_supervisor::~domain_supervisor() = default;

ss::future<> domain_supervisor::start() { return _impl->start(); }

ss::future<> domain_supervisor::stop() { return _impl->stop(); }

} // namespace experimental::cloud_topics::l1
