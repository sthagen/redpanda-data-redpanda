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

#include "cluster_link/manager.h"

#include "cluster_link/logger.h"

using namespace std::chrono_literals;

namespace cluster_link {
manager::manager(
  ::model::node_id self,
  std::unique_ptr<link_registry> registry,
  std::unique_ptr<link_factory> link_factory)
  : _self(self)
  , _registry(std::move(registry))
  , _link_factory(std::move(link_factory))
  , _queue(
      [](const std::exception_ptr& ex) {
          vlog(cllog.warn, "unexpected panda link manager error: {}", ex);
      },
      ssx::work_queue::is_paused_t::yes) {}

ss::future<> manager::start() {
    vlog(cllog.info, "Starting panda link manager");
    auto ids = _registry->get_all_link_ids();
    for (auto id : ids) {
        co_await handle_on_link_change(id);
    }
    _queue.resume();
}

ss::future<> manager::stop() {
    vlog(cllog.info, "Stopping panda link manager");
    co_await _queue.shutdown();
    for (auto& [_, link] : _links) {
        co_await link->stop();
    }
}

void manager::on_link_change(model::id_t id) {
    vlog(cllog.trace, "Panda link with id={} has changed", id);
    _queue.submit([this, id] { return handle_on_link_change(id); });
}

void manager::on_leadership_change(::model::ntp ntp, ntp_leader is_ntp_leader) {
    vlog(cllog.trace, "NTP={} leadership changed to {}", ntp, is_ntp_leader);
}

ss::future<> manager::handle_on_link_change(model::id_t id) {
    static constexpr auto retry_delay = 10s;

    vlog(cllog.trace, "Handling panda link change for id={}", id);
    auto link_opt = _registry->find_link_by_id(id);
    if (!link_opt) {
        vlog(cllog.debug, "Detected panda link id={} has been removed", id);
        auto it = _links.find(id);
        if (it != _links.end()) {
            // Stop and remove the link
            try {
                vlog(cllog.debug, "Stopping panda link with id={}", id);
                co_await it->second->stop();
                _links.erase(it);
            } catch (const std::exception& e) {
                vlog(
                  cllog.warn,
                  "Failed to stop link {}: \"{}\".  Re-attempting link stop "
                  "within {} seconds",
                  id,
                  e,
                  retry_delay.count());
                _queue.submit_delayed(retry_delay, [this, id] {
                    return handle_on_link_change(id);
                });
            }
        } else {
            vlog(cllog.trace, "No link found for id={}", id);
        }
        co_return;
    }

    const auto& link_metadata = link_opt->get();
    auto it = _links.find(id);
    if (it != _links.end()) {
        // Link already exists, update its configur
        vlog(
          cllog.debug,
          "Updating panda link id={} with new config: {}",
          id,
          link_metadata);
        it->second->update_config(link_metadata);
    } else {
        // Create a new link
        vlog(
          cllog.debug,
          "Creating new link with id={}, config: {}",
          id,
          link_metadata);
        try {
            auto new_link = _link_factory->create_link(link_metadata);
            vassert(
              new_link, "Link factory returned a null link for id={}", id);
            co_await new_link->start();
            _links.emplace(id, std::move(new_link));
        } catch (const std::exception& e) {
            vlog(
              cllog.warn,
              "Failed to create link {}: \"{}\".  Re-attempting link creation "
              "in {} seconds",
              id,
              e,
              retry_delay.count());
            _queue.submit_delayed(
              retry_delay, [this, id] { return handle_on_link_change(id); });
        }
    }
}
} // namespace cluster_link
