/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/vlog.h"
#include "serde/rw/rw.h"

#include <array>
#include <type_traits>
#include <utility>
#include <variant>

namespace serde {

// A small wrapper around std::variant that is marked to be serializable.
//
// Special precation needs to be taken to mark a variant type as serializable
// with respect to compatibility. `serde::variant` should be a drop in
// replacement for `std::variant`, but allows for serde operations.
//
// # Variant Wire Compatibility:
//
// The wire format is: [variant size][active index][active alternative].
// Compatibility is positional: the active index must map to the same type in
// every definition that may read or write the value.
//
// Compatible changes:
//   - Append alternatives to the end. Existing indexes keep their meaning, so
//     readers can read values whose active index they know, even if the
//     serialized variant size differs.
//     Note: writing a newly appended alternative still requires rollout gating;
//     older readers cannot read an active index that is not in their
//     definition.
//
// Incompatible changes:
//   - Reordering alternatives.
//   - Inserting alternatives before existing alternatives.
//   - Removing alternatives that may appear on the wire.
//   - Changing the type at an existing index.
template<typename... Types>
struct variant : public std::variant<Types...> {
    using type = std::variant<Types...>;

    constexpr variant() noexcept(std::is_nothrow_default_constructible_v<
                                 std::variant_alternative_t<0, type>>)
      = default;
    constexpr variant(const variant&) noexcept(
      std::is_nothrow_copy_constructible_v<type>) = default;
    constexpr variant(variant&&) noexcept(
      std::is_nothrow_move_constructible_v<type>) = default;

    // Ensure that this is not implicitly convertable from std::variant
    // but allow assignment from each individual type. For example:
    //
    // ```cpp
    // using my_variant = serde::variant<int, bool>
    //
    // my_variant v = false; // should compile
    //
    // my_variant v = std::variant<int, bool>(false); // should NOT compile
    // ```
    template<class T>
    constexpr variant(T&& t) // NOLINT(*-explicit-*)
      noexcept(std::is_nothrow_constructible_v<type, decltype(t)>)
    requires(
      !std::is_same_v<std::decay_t<T>, type>
      && std::is_constructible_v<type, T>)
      : type(std::forward<T>(t)) {};
    // Allow explicit conversion from std::variant
    explicit constexpr variant(type v) noexcept(
      std::is_nothrow_move_constructible_v<type>)
      : type(std::move(v)) {};

    template<class T, class... Args>
    constexpr explicit variant(
      std::in_place_type_t<T> in_place,
      Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>)
      : type(in_place, std::forward<Args...>(args)...) {}
    template<std::size_t I, class... Args>
    constexpr explicit variant(
      std::in_place_index_t<I> in_place,
      Args&&... args) noexcept(std::
                                 is_nothrow_constructible_v<
                                   std::variant_alternative_t<I, type>,
                                   Args...>)
      : type(in_place, std::forward<Args...>(args)...) {}

    variant& operator=(const variant&) noexcept(
      std::is_nothrow_copy_assignable_v<type>) = default;
    variant& operator=(variant&&) noexcept(
      std::is_nothrow_move_assignable_v<type>) = default;

    constexpr ~variant() noexcept = default;

    using type::emplace;
    using type::index;
    using type::swap;
    using type::valueless_by_exception;
};

template<typename V>
requires requires {
    []<typename... T>(variant<T...>) {}(std::declval<std::remove_cvref_t<V>>());
}
void tag_invoke(tag_t<write_tag>, iobuf& out, V&& v) {
    write<size_t>(
      out, std::variant_size_v<typename std::remove_cvref_t<V>::type>);
    write<size_t>(out, v.index());
    std::visit(
      [&out]<typename T>(T&& t) { write(out, std::forward<T>(t)); },
      std::forward<V>(v));
}

namespace detail {

template<typename Variant>
struct variant_factory {
    using constructor = Variant (*)(iobuf_parser&, std::size_t);
    using constructor_table
      = std::array<constructor, std::variant_size_v<Variant>>;

    consteval variant_factory()
      : constructors([]<std::size_t... Index>(std::index_sequence<Index...>) {
          return std::to_array<constructor>({
            [](iobuf_parser& in, std::size_t bytes_left_limit) {
                return Variant{
                  std::in_place_index<Index>,
                  read_nested<std::variant_alternative_t<Index, Variant>>(
                    in, bytes_left_limit)};
            }...,
          });
      }(std::make_index_sequence<std::variant_size_v<Variant>>())) {}

    constructor_table constructors;
};

} // namespace detail

template<typename... T>
void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  variant<T...>& t,
  const std::size_t bytes_left_limit) {
    using Type = std::decay_t<decltype(t)>;
    using UnderlyingType = Type::type;

    // The serialized size is intentionally *not* required to match the current
    // variant size: appending alternatives keeps the index -> type mapping
    // stable, so a differing size is expected when reading across versions. We
    // only validate that the active index is known to the current definition.
    auto serialized_size = read_nested<size_t>(in, bytes_left_limit);
    auto index = read_nested<size_t>(in, bytes_left_limit);

    if (index >= std::variant_size_v<UnderlyingType>) [[unlikely]] {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "reading type {} of size {}: {} bytes left - variant index {} is out "
          "of range for the current definition (serialized variant size: {}, "
          "current variant size: {}); the active alternative is not known to "
          "this binary, likely a compatibility issue.",
          type_str<Type>(),
          sizeof(Type),
          in.bytes_left(),
          index,
          serialized_size,
          std::variant_size_v<UnderlyingType>));
    }
    constexpr detail::variant_factory<UnderlyingType> factory{};
    t = Type(factory.constructors[index](in, bytes_left_limit));
}

} // namespace serde

/// Formatter for serde::variant - visits the variant and formats the active
/// alternative.
template<typename... Types>
struct fmt::formatter<serde::variant<Types...>> {
    constexpr auto parse(fmt::format_parse_context& ctx) const {
        return ctx.begin();
    }
    template<typename Ctx>
    auto format(const serde::variant<Types...>& v, Ctx& ctx) const {
        return std::visit(
          [&ctx](const auto& val) {
              return fmt::format_to(ctx.out(), "{}", val);
          },
          static_cast<const std::variant<Types...>&>(v));
    }
};
