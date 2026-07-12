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

#include "cluster_link/schema_registry_sync/scope.h"

#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <variant>

namespace cluster_link::schema_registry_sync {

ss::noncopyable_function<bool(const ppsr::context_subject&)> make_in_scope(
  chunked_hash_set<ppsr::context> contexts,
  chunked_hash_set<ppsr::context_subject> subjects) {
    const bool unfiltered = contexts.empty() && subjects.empty();
    return [unfiltered,
            contexts = std::move(contexts),
            subjects = std::move(subjects)](const ppsr::context_subject& sub) {
        return unfiltered || contexts.contains(sub.ctx)
               || subjects.contains(sub);
    };
}

context_mapper
context_mapper::make(const model::schema_registry_sync_config& config) {
    context_mapper m;
    const auto* api = config.api_mode();
    if (api == nullptr || !api->destination.has_value()) {
        return m;
    }
    using exact_context_mapping
      = model::schema_registry_sync_config::exact_context_mapping;
    if (
      const auto* exact = std::get_if<exact_context_mapping>(
        &*api->destination)) {
        // An exact mapping is not identity even when its table is empty: it
        // then covers no source context and rejects every one.
        m._identity = false;
        for (const auto& [src, dest] : exact->mappings) {
            m._fwd.emplace(ppsr::context{src}, ppsr::context{dest});
            m._rev.emplace(ppsr::context{dest}, ppsr::context{src});
        }
    }
    // The identity_context_mapping variant leaves the mapper as identity,
    // matching the no-destination-configured case.
    return m;
}

std::optional<ppsr::context>
context_mapper::forward(const ppsr::context& src) const {
    if (_identity) {
        return src;
    }
    // The global is written to the destination global directly, never remapped.
    if (src == ppsr::global_context) {
        return src;
    }
    if (auto it = _fwd.find(src); it != _fwd.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<ppsr::context>
context_mapper::reverse(const ppsr::context& dest) const {
    if (_identity) {
        // Identity: every destination context maps back to itself.
        return dest;
    }
    if (auto it = _rev.find(dest); it != _rev.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<ss::sstring> check_preconditions(
  const model::schema_registry_sync_config& config,
  const chunked_hash_set<ppsr::context>& in_scope_contexts,
  bool qualified_subjects_enabled) {
    const auto* api = config.api_mode();
    if (api == nullptr) {
        return std::nullopt;
    }

    using exact_context_mapping
      = model::schema_registry_sync_config::exact_context_mapping;

    // Enumerates every source context the run intends to replicate: those in
    // scope this run, plus any the filter names eagerly (which the source may
    // not hold yet). Stops at the first fault `fn` reports. The registry-wide
    // global (.__GLOBAL) is skipped: the sync writes it to the destination
    // global directly (a typed store write, never remapped and independent of
    // qualified subjects), so the mapping-coverage and default-context rules
    // below do not apply to it.
    auto for_each_source_context =
      [&](auto&& fn) -> std::optional<ss::sstring> {
        auto visit = [&](const ppsr::context& ctx) {
            return ctx == ppsr::global_context ? std::nullopt : fn(ctx);
        };
        for (const auto& ctx : in_scope_contexts) {
            if (auto r = visit(ctx)) {
                return r;
            }
        }
        for (const auto& ctx : api->filter.contexts) {
            if (auto r = visit(ppsr::context{ctx})) {
                return r;
            }
        }
        // Parse as qualified so a non-default context expressed in subject
        // syntax is seen, rather than flattened to a default-context subject.
        for (const auto& sub : api->filter.subjects) {
            auto parsed = ppsr::context_subject::from_string(
              sub, ppsr::qualified_subjects_enabled::yes);
            if (auto r = visit(parsed.ctx)) {
                return r;
            }
        }
        return std::nullopt;
    };

    // Mapping-usability checks, independent of qualified_subjects: an exact
    // mapping must send every in-scope source context to a distinct
    // destination. A missing mapping would drop a context silently; a shared
    // destination would make the reverse map (used to scope the destination
    // scan) ambiguous.
    if (
      api->destination.has_value()
      && std::holds_alternative<exact_context_mapping>(*api->destination)) {
        const auto& mappings
          = std::get<exact_context_mapping>(*api->destination).mappings;
        chunked_hash_set<ppsr::context> destinations;
        for (const auto& [src, dest] : mappings) {
            if (!destinations.insert(ppsr::context{dest}).second) {
                return ssx::sformat(
                  "context mapping destination '{}' is used by more than one "
                  "source context",
                  dest);
            }
        }
        if (
          auto r = for_each_source_context(
            [&](const ppsr::context& ctx) -> std::optional<ss::sstring> {
                if (!mappings.contains(ctx())) {
                    return ssx::sformat(
                      "source context '{}' in scope has no destination "
                      "mapping",
                      ctx);
                }
                return std::nullopt;
            })) {
            return r;
        }
    }

    if (qualified_subjects_enabled) {
        return std::nullopt;
    }

    // With qualified subjects off the destination can only hold the default
    // context, so reject any non-default context that would be written there.
    auto rejected = [](const ppsr::context& ctx) -> std::optional<ss::sstring> {
        if (ctx == ppsr::default_context) {
            return std::nullopt;
        }
        return ssx::sformat(
          "non-default context '{}' requires "
          "schema_registry_enable_qualified_subjects to be enabled",
          ctx);
    };

    // Identity mapping (also the default when no remapping is configured):
    // every context the run touches becomes a destination context -- discovered
    // from the source, or named by a context or subject filter the source may
    // not yet hold -- so each must be default-context.
    auto reject_identity = [&]() -> std::optional<ss::sstring> {
        return for_each_source_context(rejected);
    };
    if (!api->destination.has_value()) {
        return reject_identity();
    }
    using identity_context_mapping
      = model::schema_registry_sync_config::identity_context_mapping;
    return ss::visit(
      *api->destination,
      [&](const identity_context_mapping&) { return reject_identity(); },
      [&](const exact_context_mapping& m) -> std::optional<ss::sstring> {
          // Remapping writes to the mapping targets; source contexts are
          // remapped away, so only the targets matter.
          for (const auto& [_, dest] : m.mappings) {
              if (auto r = rejected(ppsr::context{dest})) {
                  return r;
              }
          }
          return std::nullopt;
      });
}

} // namespace cluster_link::schema_registry_sync
