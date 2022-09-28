// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "bytes/details/out_of_range.h"
#include "reflection/adl.h"

#include <absl/hash/hash.h>
#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <vector>

// Wrapper around Boost's UUID type suitable for serialization with serde.
// Provides utilities to construct and convert to other types.
//
// Expected usage is to supply a UUID v4 (i.e. based on random numbers). As
// such, serialization of this type simply serializes 16 bytes.
struct uuid_t {
public:
    static constexpr auto length = 16;
    using underlying_t = boost::uuids::uuid;

    underlying_t uuid;

    explicit uuid_t(const underlying_t& uuid)
      : uuid(uuid) {}

    explicit uuid_t(const std::vector<uint8_t>& v)
      : uuid({}) {
        if (v.size() != length) {
            details::throw_out_of_range(
              "Expected size of {} for UUID, got {}", length, v.size());
        }
        std::copy(v.begin(), v.end(), uuid.begin());
    }

    uuid_t() noexcept = default;

    std::vector<uint8_t> to_vector() const {
        return {uuid.begin(), uuid.end()};
    }

    friend std::ostream& operator<<(std::ostream& os, const uuid_t& u) {
        return os << fmt::format("{}", u.uuid);
    }

    bool operator==(const uuid_t& u) const { return this->uuid == u.uuid; }

    template<typename H>
    friend H AbslHashValue(H h, const uuid_t& u) {
        for (const uint8_t byte : u.uuid) {
            H tmp = H::combine(std::move(h), byte);
            h = std::move(tmp);
        }
        return h;
    }
};

namespace std {
template<>
struct hash<uuid_t> {
    size_t operator()(const uuid_t& u) const {
        return boost::hash<uuid_t::underlying_t>()(u.uuid);
    }
};
} // namespace std
