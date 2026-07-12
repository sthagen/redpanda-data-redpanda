/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/sr_preflight_checker.h"

#include "cluster_link/logger.h"
#include "cluster_link/model/sr_context_mapping.h"
#include "cluster_link/model/types.h"
#include "cluster_link/schema_registry_sync/http_source_reader.h"
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/timer.hh>
#include <seastar/coroutine/as_future.hh>

#include <absl/container/flat_hash_set.h>

#include <ranges>

namespace cluster_link {

namespace {
namespace ppsr = pandaproxy::schema_registry;

using shadow_schema_registry_api
  = model::schema_registry_sync_config::shadow_schema_registry_api;

// Deadline for the reachability probe. The source reader's own retry budget is
// far longer than the admin request deadline, so without a shorter bound an
// unreachable source would exhaust the RPC (surfacing as a transient
// 'unavailable') instead of a prompt link_sr_unreachable.
constexpr auto sr_probe_timeout = std::chrono::seconds{5};

class source_sr_prober_impl : public source_sr_prober {
public:
    ss::future<cl_result<void>> check_source_reachable(
      const shadow_schema_registry_api& cfg, ss::abort_source& as) final {
        // The factory never throws: a null or unparseable config yields an
        // unavailable reader whose list_contexts reports source_unavailable,
        // which we map to link_sr_unreachable below.
        auto reader = schema_registry_sync::http_source_reader_factory{}.create(
          &cfg);

        // Bound the probe: abort after sr_probe_timeout (or when the caller
        // aborts) so an unreachable source fails fast instead of running the
        // reader's full internal retry budget and timing out the admin request.
        auto probe_as = ss::abort_source{};
        // Abort with timed_out_error so a timeout surfaces as such in the error
        // message, rather than a generic abort_requested_exception.
        auto deadline = ss::timer<ss::lowres_clock>{[&probe_as]() noexcept {
            probe_as.request_abort_ex(ss::timed_out_error{});
        }};
        deadline.arm(sr_probe_timeout);
        const auto parent_sub = as.subscribe(
          [&probe_as]() noexcept { probe_as.request_abort(); });
        if (!parent_sub) {
            // Caller already aborted (e.g. shard shutdown); mirror it.
            probe_as.request_abort();
        }

        auto result = co_await ss::coroutine::as_future(
          reader->list_contexts(probe_as));

        // Best-effort cleanup of the throwaway probe reader; stop() closes the
        // underlying rest_client, which can throw. A failure to close cleanly
        // says nothing about source reachability, which we already have in
        // `result`, so log and ignore it rather than let it mask the outcome.
        auto stop_fut = co_await ss::coroutine::as_future(reader->stop());
        if (stop_fut.failed()) {
            const auto stop_ex = stop_fut.get_exception();
            vlog(
              cllog.debug,
              "error stopping source schema registry reader for '{}': {}",
              cfg.source_url,
              stop_ex);
        }

        if (result.failed()) {
            const auto probe_ex = result.get_exception();
            co_return err_info(
              errc::link_sr_unreachable,
              fmt::format(
                "failed to connect to source schema registry at '{}': {}",
                cfg.source_url,
                probe_ex));
        }
        if (const auto contexts = result.get(); !contexts.has_value()) {
            const auto& err = contexts.error();
            // Only source_unavailable means we could not reach the source; a
            // reachable-but-failed query (e.g. a malformed response) is a
            // verification failure, not unreachability.
            const auto mapped_errc = err.kind
                                         == schema_registry_sync::
                                           source_error_kind::source_unavailable
                                       ? errc::link_sr_unreachable
                                       : errc::link_sr_verification_failed;
            co_return err_info(
              mapped_errc,
              fmt::format(
                "failed to query source schema registry at '{}': {}",
                cfg.source_url,
                err.message));
        }
        co_return outcome::success();
    }
};

class sr_preflight_checker_impl : public sr_preflight_checker {
public:
    sr_preflight_checker_impl(
      schema::registry& destination, std::unique_ptr<source_sr_prober> prober)
      : _destination(destination)
      , _prober(std::move(prober)) {}

    ss::future<err_info>
    check(const model::metadata& md, ss::abort_source& as) final {
        const auto* api = md.configuration.schema_registry_sync_cfg.api_mode();
        if (api == nullptr) {
            // Not a Schema Registry API-sync link (topic mode or SR sync
            // disabled).
            co_return err_info{errc::success};
        }

        // A disabled destination registry can be neither verified nor imported
        // into; fail fast with a clear reason instead of letting its methods
        // throw a logic_error below and surfacing it as an opaque failure.
        if (!_destination.is_enabled()) {
            co_return err_info(
              errc::link_sr_verification_failed,
              fmt::format(
                "cannot verify schema registry for link '{}': the target "
                "cluster has no schema registry enabled",
                md.name));
        }

        try {
            // Check #1: connectivity and authentication against the source
            // Schema Registry.
            const auto reachable = co_await _prober->check_source_reachable(
              *api, as);
            if (reachable.has_error()) {
                // A shutdown aborts the probe, which the prober reports as
                // unreachable; distinguish it here so it maps to `unavailable`
                // rather than a precondition failure.
                if (as.abort_requested()) {
                    co_return err_info(
                      errc::service_shutting_down,
                      fmt::format(
                        "schema registry preflight for link '{}' aborted by "
                        "shutdown",
                        md.name));
                }

                const auto& err = reachable.assume_error();
                vlog(
                  cllog.warn,
                  "Cluster link '{}' schema registry preflight check failed - "
                  "{}",
                  md.name,
                  err.message());
                co_return err;
            }

            // Check #2: every destination context this link would import into
            // must be empty, matching Confluent IMPORT-mode semantics. Both the
            // set of imported-into contexts and their emptiness are derived
            // from the link config and the destination registry, not the
            // source's contents.
            //
            // Force the local store to catch up before treating a context as
            // empty: the reads below hit unsynced local shard state, so on a
            // lagging replica a populated context could otherwise read empty
            // and let a colliding link through.
            co_await _destination.sync();
            const auto offending = co_await collect_offending_target_contexts(
              *api);
            if (!offending.empty()) {
                vlog(
                  cllog.warn,
                  "Cluster link '{}' schema registry preflight check failed - "
                  "target context(s) not empty: [{}]",
                  md.name,
                  fmt::join(offending, ", "));
                co_return err_info(
                  errc::link_sr_target_not_empty,
                  fmt::format(
                    "target schema registry context(s) not empty: [{}]",
                    fmt::join(offending, ", ")));
            }
        } catch (...) {
            const auto ex = std::current_exception();
            // A shard shutdown aborts the in-flight reads; report it as such
            // rather than a verification failure the operator might retry.
            if (as.abort_requested()) {
                co_return err_info(
                  errc::service_shutting_down,
                  fmt::format(
                    "schema registry preflight for link '{}' aborted by "
                    "shutdown",
                    md.name));
            }
            vlog(
              cllog.warn,
              "Cluster link '{}' schema registry preflight check failed - {}",
              md.name,
              ex);
            co_return err_info(
              errc::link_sr_verification_failed,
              fmt::format(
                "failed to verify schema registry for link '{}' - {}",
                md.name,
                ex));
        }
        co_return err_info{errc::success};
    }

private:
    // Destination contexts the link would import into that are not empty.
    // include_deleted::yes because a soft-deleted subject still occupies the
    // context's namespace and would collide on import.
    ss::future<chunked_vector<ss::sstring>>
    collect_offending_target_contexts(const shadow_schema_registry_api& api) {
        // An unset destination mapping defaults to identity mapping.
        if (!api.destination.has_value()) {
            return identity_offending_contexts(api);
        }
        return ss::visit(
          api.destination.value(),
          [&](
            const model::schema_registry_sync_config::exact_context_mapping&
              m) { return exact_offending_contexts(api, m.mappings); },
          [&](
            const model::schema_registry_sync_config::
              identity_context_mapping&) {
              return identity_offending_contexts(api);
          });
    }

    // Exact mapping: the imported-into contexts are exactly the mapping
    // destinations whose source the filter selects -- a bounded set known from
    // config, so probe each with has_subjects (short-circuits). A mapping whose
    // source the filter excludes is inert, so requiring its destination to be
    // empty would over-block.
    ss::future<chunked_vector<ss::sstring>> exact_offending_contexts(
      const shadow_schema_registry_api& api,
      const chunked_hash_map<ss::sstring, ss::sstring>& mappings) {
        auto targets = mappings | std::views::filter([&](const auto& kv) {
                           return model::filter_selects_source_context(
                             api.filter, ppsr::context{kv.first});
                       })
                       | std::views::values
                       | std::ranges::to<absl::flat_hash_set<ss::sstring>>();

        auto offending = chunked_vector<ss::sstring>{};
        for (const auto& ctx : targets) {
            if (
              co_await _destination.has_subjects(
                ppsr::context{ctx}, ppsr::include_deleted::yes)) {
                offending.push_back(ctx);
            }
        }
        co_return offending;
    }

    // Identity/unset mapping: identity keeps the context name, so a destination
    // context is imported into iff the link filter selects it. Any in-scope
    // destination subject therefore makes its context offending. This mirrors
    // the runtime client-write blocker in frontend.cc
    // (api_mode_shadows_context), which likewise keys identity ownership off
    // the destination context and the filter.
    ss::future<chunked_vector<ss::sstring>>
    identity_offending_contexts(const shadow_schema_registry_api& api) {
        auto subjects = co_await _destination.get_subjects(
          ppsr::include_deleted::yes);

        co_return subjects
          | std::views::transform([](const auto& cs) { return cs.ctx(); })
          | std::ranges::to<chunked_hash_set<ss::sstring>>()
          | std::views::filter([&api](const ss::sstring& ctx) {
                return model::filter_selects_source_context(
                  api.filter, ppsr::context{ctx});
            })
          | std::ranges::to<chunked_vector<ss::sstring>>();
    }

    schema::registry& _destination;
    std::unique_ptr<source_sr_prober> _prober;
};

} // namespace

std::unique_ptr<source_sr_prober> source_sr_prober::make_default() {
    return std::make_unique<source_sr_prober_impl>();
}

std::unique_ptr<sr_preflight_checker> sr_preflight_checker::make_default(
  schema::registry& destination, std::unique_ptr<source_sr_prober> prober) {
    return std::make_unique<sr_preflight_checker_impl>(
      destination, std::move(prober));
}

} // namespace cluster_link
