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

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <span>
#include <vector>

namespace serde::pb {

// The API contract expected from generated protobuf messages.
template<typename T>
concept MaskableProtobufMessage = requires {
    {
        // If this path is valid for this message.
        T::is_valid_field_path(std::declval<std::span<const ss::sstring>>())
    } -> std::same_as<bool>;
    {
        // Apply the given field path from the given message to this message.
        //
        // Allowed to move values out of the update as needed.
        std::declval<T>().apply_field_path_from(
          std::declval<std::span<const ss::sstring>>(), std::declval<T*>())
    };
};

/**
 * A field mask is a protobuf message that specifies which fields
 * should be considered set in an other protobuf message.
 *
 * It is often used for partial updates, where only a subset of fields
 * are modified.
 *
 * See: https://protobuf.dev/reference/protobuf/google.protobuf/#field-mask
 * for more details.
 */
struct field_mask {
    // The maximum number of paths in a field mask.
    constexpr static size_t max_paths = 256;
    // The maximum number of segments in a path.
    constexpr static size_t max_path_segments = 64;

    // A path is a sequence of field names.
    using path = std::vector<ss::sstring>;
    using path_view = std::span<const ss::sstring>;

    // The set of paths in the field mask.
    std::vector<path> paths;

    // Returns true if all paths in field mask is valid for this message.
    //
    // Additionally, it checks that there are no overlapping paths.
    template<MaskableProtobufMessage T>
    bool is_valid_for_message() const {
        auto copy = paths;
        std::ranges::sort(copy);
        auto it = copy.begin();
        while (it != copy.end()) {
            if (!T::is_valid_field_path(*it)) {
                return false;
            }
            auto prev = it;
            ++it;
            if (it != copy.end() && std::ranges::starts_with(*it, *prev)) {
                // If the previous path is a prefix of the next one (due to
                // being sorted), then there is an overlapping field mask.
                // This is invalid.
                return false;
            }
        }
        return true;
    }

    // Applies the field mask to the given resource, setting the fields
    // specified in the mask to the values from the update.
    //
    // NOTE: repeated fields are appended to, not replaced.
    // NOTE: map fields are merged.
    // NOTE: It's assumed that the field mask has already been validated
    // for the proto type.
    //
    //
    // Example usage:
    //
    // ```
    // void handle_update(proto_t update, field_mask mask) {
    //   proto_t resource = get_resource();
    //   if (!mask.is_valid_for_message<proto_t>()) {
    //     throw std::invalid_argument("Invalid field mask");
    //   }
    //   mask.merge_into(std::move(update), &resource);
    //   save_resource(std::move(resource));
    // }
    // ```
    template<MaskableProtobufMessage T>
    void merge_into(T update, T* resource) const {
        if (paths.empty()) {
            *resource = std::move(update);
        }
        for (const path& path : paths) {
            resource->apply_field_path_from(path, &update);
        }
    }

    bool operator==(const field_mask& other) const = default;
    fmt::iterator format_to(fmt::iterator it) const;
};

} // namespace serde::pb
