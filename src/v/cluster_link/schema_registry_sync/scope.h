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

#pragma once

#include "cluster_link/model/types.h"
#include "container/chunked_hash_map.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include <optional>

namespace cluster_link::schema_registry_sync {

namespace ppsr = pandaproxy::schema_registry;

/// Builds a pure predicate reporting whether a context-qualified subject
/// belongs to the link's scope, following the configured source filter's union
/// semantics:
///   * neither `contexts` nor `subjects` configured: no filter, every node is
///     in scope;
///   * otherwise a node is in scope if its context is one of `contexts`, OR it
///     is one of the individually listed `subjects`.
/// A subject filter therefore widens scope -- it adds individual subjects from
/// contexts the context filter does not select -- rather than narrowing it.
ss::noncopyable_function<bool(const ppsr::context_subject&)> make_in_scope(
  chunked_hash_set<ppsr::context> contexts,
  chunked_hash_set<ppsr::context_subject> subjects);

/// Maps context names between source and destination per the link's configured
/// destination mapping (identity, or an exact source->destination table).
///
/// The reconcile pipeline runs in the source-context namespace; the mapper is
/// applied only at the destination boundary -- forward() when writing (imports,
/// mode/config, hard-deletes), reverse() when reading destination state back
/// into the diff.
class context_mapper {
public:
    /// Identity when no destination mapping is configured or it is the identity
    /// variant.
    static context_mapper make(const model::schema_registry_sync_config&);

    /// Source -> destination context; nullopt when an exact mapping has no
    /// entry (identity and the global context pass through, and the caller
    /// errors).
    std::optional<ppsr::context> forward(const ppsr::context&) const;

    /// Destination context -> source context; nullopt for a destination no
    /// source maps to. A remap target is thereby claimed in its entirety, so
    /// any subject placed there independently of the link reads as
    /// source-absent and is hard-deleted -- map only into contexts the mirror
    /// owns.
    std::optional<ppsr::context> reverse(const ppsr::context&) const;

    /// True when forward and reverse are both the identity.
    bool is_identity() const { return _identity; }

private:
    // The mode, set from the config variant -- not inferred from _fwd
    // emptiness, because an exact mapping with an empty table is a distinct
    // state from identity: it covers no source context and so rejects every
    // one, whereas identity passes every one through.
    bool _identity{true};
    // Empty for identity. Otherwise source->dest and its inverse (well-defined
    // because destinations must be distinct).
    chunked_hash_map<ppsr::context, ppsr::context> _fwd;
    chunked_hash_map<ppsr::context, ppsr::context> _rev;
};

/// Returns a fault reason if the configuration cannot be replicated, else
/// nullopt. Two classes of check: mapping usability (always) -- under an exact
/// mapping every in-scope source context must map to a distinct destination;
/// and the qualified-subject rule (only when `qualified_subjects_enabled` is
/// off) -- no non-default context may reach the destination. The flag is
/// injected to keep the check pure and testable.
std::optional<ss::sstring> check_preconditions(
  const model::schema_registry_sync_config& config,
  const chunked_hash_set<ppsr::context>& in_scope_contexts,
  bool qualified_subjects_enabled);

} // namespace cluster_link::schema_registry_sync
