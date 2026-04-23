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

#include "base/format_to.h"
#include "cluster/errc.h"
#include "model/timeout_clock.h"
#include "security/acl.h"
#include "security/role.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "serde/rw/envelope.h"
#include "serde/rw/vector.h"

#include <fmt/format.h>

#include <vector>

namespace cluster {

struct create_acls_cmd_data
  : serde::envelope<
      create_acls_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    std::vector<security::acl_binding> bindings;

    friend bool operator==(
      const create_acls_cmd_data&, const create_acls_cmd_data&) = default;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{ bindings: {} }}", bindings);
    }

    auto serde_fields() { return std::tie(bindings); }
};

struct create_acls_request
  : serde::envelope<
      create_acls_request,
      serde::version<0>,
      serde::compat_version<0>> {
    create_acls_cmd_data data;
    model::timeout_clock::duration timeout;

    create_acls_request() noexcept = default;
    create_acls_request(
      create_acls_cmd_data data, model::timeout_clock::duration timeout)
      : data(std::move(data))
      , timeout(timeout) {}

    friend bool operator==(
      const create_acls_request&, const create_acls_request&) = default;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it, "{{ data: {}, timeout: {} }}", data, timeout.count());
    }

    auto serde_fields() { return std::tie(data, timeout); }
};

struct create_acls_reply
  : serde::
      envelope<create_acls_reply, serde::version<0>, serde::compat_version<0>> {
    std::vector<errc> results;

    friend bool
    operator==(const create_acls_reply&, const create_acls_reply&) = default;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{ results: {} }}", results);
    }

    auto serde_fields() { return std::tie(results); }
};

struct delete_acls_cmd_data
  : serde::envelope<
      delete_acls_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    std::vector<security::acl_binding_filter> filters;

    friend bool operator==(
      const delete_acls_cmd_data&, const delete_acls_cmd_data&) = default;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{ filters: {} }}", filters);
    }

    auto serde_fields() { return std::tie(filters); }
};

// result for a single filter
struct delete_acls_result
  : serde::envelope<
      delete_acls_result,
      serde::version<0>,
      serde::compat_version<0>> {
    errc error;
    std::vector<security::acl_binding> bindings;

    friend bool
    operator==(const delete_acls_result&, const delete_acls_result&) = default;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it, "{{ error: {} bindings: {} }}", error, bindings);
    }

    auto serde_fields() { return std::tie(error, bindings); }
};

struct delete_acls_request
  : serde::envelope<
      delete_acls_request,
      serde::version<0>,
      serde::compat_version<0>> {
    delete_acls_cmd_data data;
    model::timeout_clock::duration timeout;

    delete_acls_request() noexcept = default;
    delete_acls_request(
      delete_acls_cmd_data data, model::timeout_clock::duration timeout)
      : data(std::move(data))
      , timeout(timeout) {}

    friend bool operator==(
      const delete_acls_request&, const delete_acls_request&) = default;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{ data: {} timeout: {} }}", data, timeout);
    }

    auto serde_fields() { return std::tie(data, timeout); }
};

struct delete_acls_reply
  : serde::
      envelope<delete_acls_reply, serde::version<0>, serde::compat_version<0>> {
    std::vector<delete_acls_result> results;

    friend bool
    operator==(const delete_acls_reply&, const delete_acls_reply&) = default;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{ results: {} }}", results);
    }

    auto serde_fields() { return std::tie(results); }
};

struct user_and_credential
  : serde::envelope<
      user_and_credential,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    user_and_credential() = default;
    user_and_credential(
      security::credential_user&& username_,
      security::scram_credential&& credential_)
      : username(std::move(username_))
      , credential(std::move(credential_)) {}
    friend bool operator==(
      const user_and_credential&, const user_and_credential&) = default;
    auto serde_fields() { return std::tie(username, credential); }

    security::credential_user username;
    security::scram_credential credential;
};

struct upsert_role_cmd_data
  : serde::
      envelope<upsert_role_cmd_data, serde::version<0>, serde::version<0>> {
    security::role_name name;
    security::role role;

    auto serde_fields() { return std::tie(name, role); }

    friend bool operator==(
      const upsert_role_cmd_data&, const upsert_role_cmd_data&) = default;
};

struct delete_role_cmd_data
  : serde::
      envelope<delete_role_cmd_data, serde::version<0>, serde::version<0>> {
    security::role_name name;

    auto serde_fields() { return std::tie(name); }

    friend bool operator==(
      const delete_role_cmd_data&, const delete_role_cmd_data&) = default;
};

} // namespace cluster
