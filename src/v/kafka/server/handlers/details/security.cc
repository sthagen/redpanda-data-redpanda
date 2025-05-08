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

#include "kafka/server/handlers/details/security.h"

#include "security/scram_algorithm.h"

namespace kafka::details {

std::optional<security::scram_algorithm_t>
kafka_to_security_mechanism(kafka::scram_mechanism mechanism) {
    switch (mechanism) {
    case kafka::scram_mechanism::unknown:
        return std::nullopt;
    case kafka::scram_mechanism::scram_sha_256:
        return security::scram_algorithm_t::sha256;
    case kafka::scram_mechanism::scram_sha_512:
        return security::scram_algorithm_t::sha512;
    }
    return std::nullopt;
}

scram_mechanism key_size_to_mechanism(size_t key_size) {
    switch (key_size) {
    case security::scram_sha256::key_size:
        return scram_mechanism::scram_sha_256;
    case security::scram_sha512::key_size:
        return scram_mechanism::scram_sha_512;
    default:
        return scram_mechanism::unknown;
    }
}
} // namespace kafka::details
