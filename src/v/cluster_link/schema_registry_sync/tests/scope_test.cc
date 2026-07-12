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
#include "pandaproxy/schema_registry/types.h"
#include "test_utils/test.h"

namespace cluster_link::tests {

namespace ppsr = pandaproxy::schema_registry;
namespace srs = cluster_link::schema_registry_sync;

namespace {
model::schema_registry_sync_config config_with(
  model::schema_registry_sync_config::shadow_schema_registry_api api = {}) {
    model::schema_registry_sync_config config;
    config.sync_mode = std::move(api);
    return config;
}
} // namespace

TEST(scope, in_scope_predicate_matches_configured_contexts) {
    chunked_hash_set<ppsr::context> contexts{
      ppsr::default_context, ppsr::context{".b"}};
    // Empty subject set: no per-subject restriction within the in-scope
    // contexts.
    auto in_scope = srs::make_in_scope(std::move(contexts), {});

    EXPECT_TRUE(in_scope(ppsr::context_subject::unqualified("orders")));
    EXPECT_TRUE(
      in_scope(ppsr::context_subject{ppsr::context{".b"}, ppsr::subject{"x"}}));
    EXPECT_FALSE(
      in_scope(ppsr::context_subject{ppsr::context{".c"}, ppsr::subject{"x"}}));
}

TEST(scope, in_scope_filter_union_semantics) {
    // The source filter's context and subject selectors union: contexts select
    // whole contexts, subjects add individual context-qualified subjects, and
    // an empty filter replicates everything. Every scenario checks the same
    // four subjects in the same order, so the cases read as a truth table:
    //   * default_a:     subject "a" in the default context
    //   * other_x:       subject "x" in the .other context
    //   * example:       subject "example" in the .onemore context
    //   * onemore_other: subject "other" in the .onemore context (the rest of
    //                    .onemore that an individual `example` selector leaves
    //                    out)
    const auto other = ppsr::context{".other"};
    const auto onemore = ppsr::context{".onemore"};
    const auto default_a = ppsr::context_subject::unqualified("a");
    const auto other_x = ppsr::context_subject{other, ppsr::subject{"x"}};
    const auto example = ppsr::context_subject{
      onemore, ppsr::subject{"example"}};
    const auto onemore_other = ppsr::context_subject{
      onemore, ppsr::subject{"other"}};

    auto ctxs = [](std::initializer_list<ppsr::context> cs) {
        chunked_hash_set<ppsr::context> out;
        for (const auto& c : cs) {
            out.insert(c);
        }
        return out;
    };
    auto subs = [](std::initializer_list<ppsr::context_subject> ss) {
        chunked_hash_set<ppsr::context_subject> out;
        for (const auto& s : ss) {
            out.insert(s);
        }
        return out;
    };

    // No filter: every subject is in scope.
    {
        auto in_scope = srs::make_in_scope(ctxs({}), subs({}));
        EXPECT_TRUE(in_scope(default_a));
        EXPECT_TRUE(in_scope(other_x));
        EXPECT_TRUE(in_scope(example));
        EXPECT_TRUE(in_scope(onemore_other));
    }
    // Context selector only: the whole .other context, nothing else.
    {
        auto in_scope = srs::make_in_scope(ctxs({other}), subs({}));
        EXPECT_FALSE(in_scope(default_a));
        EXPECT_TRUE(in_scope(other_x));
        EXPECT_FALSE(in_scope(example));
        EXPECT_FALSE(in_scope(onemore_other));
    }
    // Context UNION subject: all of .other, plus the single .onemore:example;
    // the rest of .onemore stays out.
    {
        auto in_scope = srs::make_in_scope(ctxs({other}), subs({example}));
        EXPECT_FALSE(in_scope(default_a));
        EXPECT_TRUE(in_scope(other_x));
        EXPECT_TRUE(in_scope(example));
        EXPECT_FALSE(in_scope(onemore_other));
    }
    // Subject selector only: just the single .onemore:example subject.
    {
        auto in_scope = srs::make_in_scope(ctxs({}), subs({example}));
        EXPECT_FALSE(in_scope(default_a));
        EXPECT_FALSE(in_scope(other_x));
        EXPECT_TRUE(in_scope(example));
        EXPECT_FALSE(in_scope(onemore_other));
    }
}

TEST(scope, context_mapper_identity_passes_through) {
    // No destination configured: forward and reverse are the identity, so the
    // whole sync stays in the source namespace unchanged.
    auto mapper = srs::context_mapper::make(config_with());
    EXPECT_TRUE(mapper.is_identity());
    EXPECT_EQ(mapper.forward(ppsr::context{".prod"}), ppsr::context{".prod"});
    EXPECT_EQ(mapper.forward(ppsr::default_context), ppsr::default_context);
    EXPECT_EQ(mapper.reverse(ppsr::context{".prod"}), ppsr::context{".prod"});

    // An explicit identity mapping is equivalent to no destination.
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    api.destination
      = model::schema_registry_sync_config::identity_context_mapping{};
    auto explicit_identity = srs::context_mapper::make(
      config_with(std::move(api)));
    EXPECT_TRUE(explicit_identity.is_identity());
    EXPECT_EQ(
      explicit_identity.forward(ppsr::context{".x"}), ppsr::context{".x"});
}

TEST(scope, context_mapper_exact_maps_both_ways) {
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".prod", ".dest");
    mapping.mappings.emplace(".stage", ".dest-stage");
    api.destination = std::move(mapping);
    auto mapper = srs::context_mapper::make(config_with(std::move(api)));

    EXPECT_FALSE(mapper.is_identity());
    // Forward rewrites a configured source context to its destination.
    EXPECT_EQ(mapper.forward(ppsr::context{".prod"}), ppsr::context{".dest"});
    EXPECT_EQ(
      mapper.forward(ppsr::context{".stage"}), ppsr::context{".dest-stage"});
    // Reverse recovers the source context from its destination.
    EXPECT_EQ(mapper.reverse(ppsr::context{".dest"}), ppsr::context{".prod"});
    EXPECT_EQ(
      mapper.reverse(ppsr::context{".dest-stage"}), ppsr::context{".stage"});
    // A destination context no source maps to is out of scope.
    EXPECT_EQ(mapper.reverse(ppsr::context{".prod"}), std::nullopt);
    EXPECT_EQ(mapper.reverse(ppsr::default_context), std::nullopt);
    // An unmapped source context has no destination -> nullopt, so callers drop
    // the unit rather than leak an un-remapped context (check_preconditions
    // covers every in-scope context; this guards one appearing afterwards).
    EXPECT_EQ(mapper.forward(ppsr::context{".other"}), std::nullopt);
    // The registry-wide global is never remapped, so it passes through even
    // under an exact mapping (its mode/config is written to the dest global).
    EXPECT_EQ(mapper.forward(ppsr::global_context), ppsr::global_context);
}

TEST(scope, context_mapper_empty_exact_rejects_all_not_identity) {
    // An exact mapping with an empty table covers no source context, so it is
    // NOT identity: forward/reverse must reject every non-global context rather
    // than pass it through. (This is distinct from an identity or
    // no-destination config, which pass everything through -- see
    // context_mapper_identity_passes_through.) check_preconditions rejects such
    // a config before the mapper is built whenever any context is in scope, but
    // the mapper must still encode reject-all so the two modes never alias.
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    api.destination
      = model::schema_registry_sync_config::exact_context_mapping{};
    auto mapper = srs::context_mapper::make(config_with(std::move(api)));

    EXPECT_FALSE(mapper.is_identity());
    EXPECT_EQ(mapper.forward(ppsr::context{".prod"}), std::nullopt);
    EXPECT_EQ(mapper.forward(ppsr::default_context), std::nullopt);
    EXPECT_EQ(mapper.reverse(ppsr::context{".prod"}), std::nullopt);
    EXPECT_EQ(mapper.reverse(ppsr::default_context), std::nullopt);
    // The global is still written to the destination global directly, so it
    // passes through even under an (empty) exact mapping.
    EXPECT_EQ(mapper.forward(ppsr::global_context), ppsr::global_context);
}

TEST(scope, preconditions_exact_mapping_requires_full_coverage) {
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".prod", ".dest");
    api.destination = std::move(mapping);
    auto config = config_with(std::move(api));

    // The default context is in scope but unmapped: a source context with no
    // destination would be dropped silently, so it faults regardless of the
    // qualified-subjects flag (a usability check, not the qualified rule).
    chunked_hash_set<ppsr::context> uncovered;
    uncovered.insert(ppsr::default_context);
    uncovered.insert(ppsr::context{".prod"});
    EXPECT_TRUE(srs::check_preconditions(config, uncovered, true).has_value());
    EXPECT_TRUE(srs::check_preconditions(config, uncovered, false).has_value());

    // With only the mapped context in scope, the mapping fully covers it (the
    // non-default target still needs qualified subjects, so check with the flag
    // on).
    chunked_hash_set<ppsr::context> covered;
    covered.insert(ppsr::context{".prod"});
    EXPECT_FALSE(srs::check_preconditions(config, covered, true).has_value());
}

TEST(scope, preconditions_exact_mapping_requires_coverage_of_filter_contexts) {
    // A schema is imported only if it is in scope, and a context-qualified
    // reference to another context is imported only if that context is in scope
    // too (else the referrer is failed at reconcile time). "In scope" is
    // defined by the source filter, so a context the filter names -- as a
    // referenced context would be -- but the exact mapping omits must fault at
    // config validation, before forward() would silently pass the unmapped
    // context through to the destination. Checked with qualified subjects ON so
    // only the mapping-coverage rule can fault (the qualified rule
    // short-circuits), and with an empty discovered-context set so the fault
    // comes purely from the filter, not from list_contexts.
    chunked_hash_set<ppsr::context> no_discovered;

    // A context filter names an unmapped context.
    {
        model::schema_registry_sync_config::shadow_schema_registry_api api;
        api.filter.contexts.push_back(".a");
        api.filter.contexts.push_back(".b"); // in scope, no destination mapping
        model::schema_registry_sync_config::exact_context_mapping mapping;
        mapping.mappings.emplace(".a", ".x");
        api.destination = std::move(mapping);
        auto config = config_with(std::move(api));
        EXPECT_TRUE(
          srs::check_preconditions(config, no_discovered, true).has_value());
    }
    // A subject filter in qualified syntax names a subject in an unmapped
    // context -- the individual-subject shape a reference takes.
    {
        model::schema_registry_sync_config::shadow_schema_registry_api api;
        api.filter.subjects.push_back(":.b:orders"); // .b in scope, unmapped
        model::schema_registry_sync_config::exact_context_mapping mapping;
        mapping.mappings.emplace(".a", ".x");
        api.destination = std::move(mapping);
        auto config = config_with(std::move(api));
        EXPECT_TRUE(
          srs::check_preconditions(config, no_discovered, true).has_value());
    }
    // Mapping every filtered context clears the fault.
    {
        model::schema_registry_sync_config::shadow_schema_registry_api api;
        api.filter.contexts.push_back(".a");
        api.filter.contexts.push_back(".b");
        model::schema_registry_sync_config::exact_context_mapping mapping;
        mapping.mappings.emplace(".a", ".x");
        mapping.mappings.emplace(".b", ".y");
        api.destination = std::move(mapping);
        auto config = config_with(std::move(api));
        EXPECT_FALSE(
          srs::check_preconditions(config, no_discovered, true).has_value());
    }
}

TEST(scope, preconditions_exact_mapping_rejects_duplicate_destination) {
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".a", ".x");
    mapping.mappings.emplace(".b", ".x"); // collision: two sources -> .x
    api.destination = std::move(mapping);
    auto config = config_with(std::move(api));

    chunked_hash_set<ppsr::context> in_scope;
    in_scope.insert(ppsr::context{".a"});
    in_scope.insert(ppsr::context{".b"});
    // A shared destination makes the reverse map ambiguous, so it is rejected
    // regardless of the qualified-subjects flag.
    EXPECT_TRUE(srs::check_preconditions(config, in_scope, true).has_value());
    EXPECT_TRUE(srs::check_preconditions(config, in_scope, false).has_value());
}

TEST(scope, preconditions_remapping_target_requires_qualified) {
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".", ".prod"); // default -> non-default target
    api.destination = std::move(mapping);
    auto config = config_with(std::move(api));

    chunked_hash_set<ppsr::context> in_scope;
    in_scope.insert(ppsr::default_context);
    // Flag off rejects the non-default target; flag on allows any context.
    EXPECT_TRUE(srs::check_preconditions(config, in_scope, false).has_value());
    EXPECT_FALSE(srs::check_preconditions(config, in_scope, true).has_value());
}

TEST(scope, preconditions_remapping_to_default_allowed) {
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".src", "."); // non-default source -> default
    api.destination = std::move(mapping);
    auto config = config_with(std::move(api));

    // The non-default source context (in_scope) is irrelevant under remapping:
    // only the default target is written.
    chunked_hash_set<ppsr::context> in_scope;
    in_scope.insert(ppsr::context{".src"});
    EXPECT_FALSE(srs::check_preconditions(config, in_scope, false).has_value());
}

TEST(scope, preconditions_nondefault_filter_requires_qualified) {
    chunked_hash_set<ppsr::context> empty;

    // A non-default context filter faults eagerly, before the source is known
    // to hold the context.
    {
        model::schema_registry_sync_config::shadow_schema_registry_api api;
        api.filter.contexts.push_back(".prod");
        auto config = config_with(std::move(api));
        EXPECT_TRUE(srs::check_preconditions(config, empty, false).has_value());
        EXPECT_FALSE(srs::check_preconditions(config, empty, true).has_value());
    }
    // A subject filter in qualified syntax names a non-default context; it is
    // parsed as qualified so the off flag cannot flatten it to a default
    // subject and hide the fault.
    {
        model::schema_registry_sync_config::shadow_schema_registry_api api;
        api.filter.subjects.push_back(":.prod:orders");
        auto config = config_with(std::move(api));
        EXPECT_TRUE(srs::check_preconditions(config, empty, false).has_value());
        EXPECT_FALSE(srs::check_preconditions(config, empty, true).has_value());
    }
}

TEST(scope, preconditions_global_context_exempt) {
    // The registry-wide global (.__GLOBAL) is written to the destination global
    // directly -- never remapped and independent of qualified subjects -- so
    // naming it in a filter must not fault, unlike a real non-default context.
    const auto global = ss::sstring{ppsr::global_context()};
    chunked_hash_set<ppsr::context> empty;

    // Under an exact mapping that covers the real source context but not
    // .__GLOBAL, the global is exempt from the mapping-coverage check.
    {
        model::schema_registry_sync_config::shadow_schema_registry_api api;
        api.filter.contexts.push_back(".prod");
        api.filter.contexts.push_back(global);
        model::schema_registry_sync_config::exact_context_mapping mapping;
        mapping.mappings.emplace(".prod", ".dest");
        api.destination = std::move(mapping);
        auto config = config_with(std::move(api));
        EXPECT_FALSE(srs::check_preconditions(config, empty, true).has_value());
    }
    // With qualified subjects off, a non-default context filter faults, but
    // .__GLOBAL alone does not.
    {
        model::schema_registry_sync_config::shadow_schema_registry_api api;
        api.filter.contexts.push_back(global);
        auto config = config_with(std::move(api));
        EXPECT_FALSE(
          srs::check_preconditions(config, empty, false).has_value());
    }
}

TEST(scope, preconditions_explicit_identity_matches_no_destination) {
    // An explicit identity mapping is the default (no-destination) case, so a
    // non-default filter context must fault identically -- the two config
    // spellings cannot diverge.
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    api.filter.contexts.push_back(".prod");
    api.destination
      = model::schema_registry_sync_config::identity_context_mapping{};
    auto config = config_with(std::move(api));

    chunked_hash_set<ppsr::context> empty;
    EXPECT_TRUE(srs::check_preconditions(config, empty, false).has_value());
    EXPECT_FALSE(srs::check_preconditions(config, empty, true).has_value());
}

TEST(scope, preconditions_remapping_ignores_source_filter) {
    // Mapping a non-default source context onto the default destination is
    // allowed even with the flag off: the source-filter name is remapped away,
    // so only the (default) target is written.
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    api.filter.contexts.push_back(".prod");
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".prod", "."); // non-default source -> default
    api.destination = std::move(mapping);
    auto config = config_with(std::move(api));

    chunked_hash_set<ppsr::context> in_scope;
    in_scope.insert(ppsr::context{".prod"});
    EXPECT_FALSE(srs::check_preconditions(config, in_scope, false).has_value());
}

TEST(scope, preconditions_require_qualified_subjects_for_nondefault) {
    auto config = config_with();

    chunked_hash_set<ppsr::context> nondefault;
    nondefault.insert(ppsr::context{".b"});
    EXPECT_TRUE(
      srs::check_preconditions(config, nondefault, false).has_value());
    EXPECT_FALSE(
      srs::check_preconditions(config, nondefault, true).has_value());

    chunked_hash_set<ppsr::context> default_only;
    default_only.insert(ppsr::default_context);
    EXPECT_FALSE(
      srs::check_preconditions(config, default_only, false).has_value());
}

} // namespace cluster_link::tests
