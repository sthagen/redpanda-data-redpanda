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

namespace kafka {

/// A compile-time list of types, used to enumerate Kafka request and handler
/// types (e.g. for building dispatch/flex tables).
template<typename... Ts>
struct type_list {};

/// Concatenate two type_lists into a single type_list.
template<typename A, typename B>
struct type_list_cat;

template<typename... Ts, typename... Us>
struct type_list_cat<type_list<Ts...>, type_list<Us...>> {
    using type = type_list<Ts..., Us...>;
};

template<typename A, typename B>
using type_list_cat_t = typename type_list_cat<A, B>::type;

} // namespace kafka
