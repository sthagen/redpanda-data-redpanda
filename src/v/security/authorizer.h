/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "config/property.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"
#include "security/acl.h"
#include "security/acl_store.h"
#include "security/logger.h"
#include "vlog.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/flat_hash_set.h>
#include <fmt/core.h>

namespace security {

/**
 * Holds authZ check metadata for audit processing
 */
struct auth_result {
    // Flag indicating if user is authorized
    bool authorized{false};
    // Indicates if the authorization system is disabled
    bool authorization_disabled{false};
    // Indicates if the user is a superuser
    bool is_superuser{false};
    // Indicates if no ACL matches were found
    bool empty_matches{false};

    // If found, the resource pattern that was matched to provide authZ decision
    std::optional<std::reference_wrapper<const resource_pattern>>
      resource_pattern;
    // If found, the ACL that was matched that provided the authZ decision
    std::optional<acl_entry_set::const_reference> acl;
    // The principal that was checked
    security::acl_principal principal;
    // The host
    security::acl_host host;
    // The type of resource
    security::resource_type resource_type;
    // The name of the resource
    ss::sstring resource_name;

    explicit operator bool() const noexcept { return is_authorized(); }

    bool is_authorized() const noexcept { return authorized; }

    template<typename T>
    static auth_result authz_disabled(
      const security::acl_principal& principal,
      security::acl_host host,
      const T& resource) {
        return {
          .authorized = true,
          .authorization_disabled = true,
          .principal = principal,
          .host = host,
          .resource_type = get_resource_type<T>(),
          .resource_name = resource(),
        };
    }

    template<typename T>
    static auth_result superuser_authorized(
      const security::acl_principal& principal,
      security::acl_host host,
      const T& resource) {
        return {
          .authorized = true,
          .is_superuser = true,
          .principal = principal,
          .host = host,
          .resource_type = get_resource_type<T>(),
          .resource_name = resource(),
        };
    }

    template<typename T>
    static auth_result empty_match_result(
      const security::acl_principal& principal,
      security::acl_host host,
      const T& resource,
      bool authorized) {
        return {
          .authorized = authorized,
          .empty_matches = true,
          .principal = principal,
          .host = host,
          .resource_type = get_resource_type<T>(),
          .resource_name = resource(),
        };
    }

    template<typename T>
    static auth_result acl_match(
      const security::acl_principal& principal,
      security::acl_host host,
      const T& resource,
      bool authorized,
      const acl_matches::acl_match& match) {
        return {
          .authorized = authorized,
          .resource_pattern = match.resource,
          .acl = match.acl,
          .principal = principal,
          .host = host,
          .resource_type = get_resource_type<T>(),
          .resource_name = resource()};
    }

    template<typename T>
    static auth_result opt_acl_match(
      const security::acl_principal& principal,
      security::acl_host host,
      const T& resource,
      const std::optional<acl_matches::acl_match>& match) {
        return {
          .authorized = match.has_value(),
          .empty_matches = !match.has_value(),
          .resource_pattern = match.has_value()
                                ? std::make_optional(match->resource)
                                : std::nullopt,
          .acl = match.has_value() ? std::make_optional(match->acl)
                                   : std::nullopt,
          .principal = principal,
          .host = host,
          .resource_type = get_resource_type<T>(),
          .resource_name = resource()};
    }
};

/*
 * Primary interface for request authorization and management of ACLs.
 *
 * superusers
 * ==========
 *
 * A set of principals may be registered with the authorizer that are allowed to
 * perform any operation. When authorization occurs if the assocaited principal
 * is found in the set of superusers then its request will be permitted. If the
 * principal is not a superuser then normal ACL authorization applies.
 */
class authorizer final {
public:
    // allow operation when no ACL match is found
    using allow_empty_matches = ss::bool_class<struct allow_empty_matches_type>;

    explicit authorizer(
      std::function<config::binding<std::vector<ss::sstring>>()> superusers_cb)
      : authorizer(allow_empty_matches::no, superusers_cb) {}

    authorizer(
      allow_empty_matches allow,
      std::function<config::binding<std::vector<ss::sstring>>()> superusers_cb)
      : _superusers_conf(superusers_cb())
      , _allow_empty_matches(allow) {
        update_superusers();
        _superusers_conf.watch([this]() { update_superusers(); });
    }

    /*
     * Add ACL bindings to the authorizer.
     */
    void add_bindings(const std::vector<acl_binding>& bindings) {
        if (unlikely(
              seclog.is_shard_zero()
              && seclog.is_enabled(ss::log_level::debug))) {
            for (const auto& binding : bindings) {
                vlog(seclog.debug, "Adding ACL binding: {}", binding);
            }
        }
        _store.add_bindings(bindings);
    }

    /*
     * Remove ACL bindings that match the filter(s).
     */
    std::vector<std::vector<acl_binding>> remove_bindings(
      const std::vector<acl_binding_filter>& filters, bool dry_run = false) {
        return _store.remove_bindings(filters, dry_run);
    }

    /*
     * Retrieve ACL bindings that match the filter.
     */
    std::vector<acl_binding> acls(const acl_binding_filter& filter) const {
        return _store.acls(filter);
    }

    /*
     * Authorize an operation on a resource. The type of resource is deduced by
     * the type `T` of the name of the resouce (e.g. `model::topic`).
     */
    template<typename T>
    auth_result authorized(
      const T& resource_name,
      acl_operation operation,
      const acl_principal& principal,
      const acl_host& host) const {
        auto type = get_resource_type<T>();
        auto acls = _store.find(type, resource_name());

        if (_superusers.contains(principal)) {
            return auth_result::superuser_authorized(
              principal, host, resource_name);
        }

        if (acls.empty()) {
            return auth_result::empty_match_result(
              principal, host, resource_name, bool(_allow_empty_matches));
        }

        // check for deny
        if (auto entry = acls.find(
              operation, principal, host, acl_permission::deny);
            entry.has_value()) {
            return auth_result::acl_match(
              principal, host, resource_name, false, *entry);
        }

        // check for allow
        return auth_result::opt_acl_match(
          principal,
          host,
          resource_name,
          acl_any_implied_ops_allowed(acls, principal, host, operation));
    }

    ss::future<fragmented_vector<acl_binding>> all_bindings() const {
        return _store.all_bindings();
    }

    ss::future<>
    reset_bindings(const fragmented_vector<acl_binding>& bindings) {
        return _store.reset_bindings(bindings);
    }

    acl_store& store() { return _store; }

private:
    /*
     * Compute whether the specified operation is allowed based on the implied
     * operations.
     */
    std::optional<acl_matches::acl_match> acl_any_implied_ops_allowed(
      const acl_matches& acls,
      const acl_principal& principal,
      const acl_host& host,
      const acl_operation operation) const {
        auto check_op = [&acls, &principal, &host](
                          auto begin,
                          auto end) -> std::optional<acl_matches::acl_match> {
            for (; begin != end; ++begin) {
                if (auto entry = acls.find(
                      *begin, principal, host, acl_permission::allow);
                    entry.has_value()) {
                    return entry;
                }
            }

            return {};
        };

        switch (operation) {
        case acl_operation::describe: {
            static constexpr std::array ops = {
              acl_operation::describe,
              acl_operation::read,
              acl_operation::write,
              acl_operation::remove,
              acl_operation::alter,
            };
            return check_op(ops.begin(), ops.end());
        }
        case acl_operation::describe_configs: {
            static constexpr std::array ops = {
              acl_operation::describe_configs,
              acl_operation::alter_configs,
            };
            return check_op(ops.begin(), ops.end());
        }
        default:
            return acls.find(operation, principal, host, acl_permission::allow);
        }
    }
    acl_store _store;

    // The list of superusers is stored twice: once as a vector in the
    // configuration subsystem, then again has a set here for fast lookups.
    // The set is updated on changes via the config::binding.
    absl::flat_hash_set<acl_principal> _superusers;
    config::binding<std::vector<ss::sstring>> _superusers_conf;
    void update_superusers() {
        // Rebuild the whole set, because an incremental change would
        // in any case involve constructing a set to do a comparison
        // between old and new.
        _superusers.clear();
        for (const auto& username : _superusers_conf()) {
            auto principal = acl_principal(principal_type::user, username);
            vlog(seclog.info, "Registered superuser account: {}", principal);
            _superusers.emplace(std::move(principal));
        }
    }

    allow_empty_matches _allow_empty_matches;
};

} // namespace security
