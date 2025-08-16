/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/protocol/types.h"

namespace kafka {

std::ostream& operator<<(std::ostream& os, describe_configs_type t) {
    switch (t) {
    case describe_configs_type::unknown:
        return os << "{unknown}";
    case describe_configs_type::boolean:
        return os << "{boolean}";
    case describe_configs_type::string:
        return os << "{string}";
    case describe_configs_type::int_type:
        return os << "{int}";
    case describe_configs_type::short_type:
        return os << "{short}";
    case describe_configs_type::long_type:
        return os << "{long}";
    case describe_configs_type::double_type:
        return os << "{double}";
    case describe_configs_type::list:
        return os << "{list}";
    case describe_configs_type::class_type:
        return os << "{class}";
    case describe_configs_type::password:
        return os << "{password}";
    }
    return os << "{unsupported type}";
}

std::ostream&
operator<<(std::ostream& os, describe_client_quotas_match_type t) {
    switch (t) {
    case describe_client_quotas_match_type::exact_name:
        return os << "{exact_name}";
    case describe_client_quotas_match_type::default_name:
        return os << "{default_name}";
    case describe_client_quotas_match_type::any_specified_name:
        return os << "{any_specified_name}";
    }
    return os << "{unsupported type}";
}

std::ostream& operator<<(std::ostream& os, coordinator_type t) {
    switch (t) {
    case coordinator_type::group:
        return os << "{group}";
    case coordinator_type::transaction:
        return os << "{transaction}";
    };
    return os << "{unknown type}";
}

std::ostream& operator<<(std::ostream& os, config_resource_type t) {
    switch (t) {
    case config_resource_type::topic:
        return os << "{topic}";
    case config_resource_type::broker:
        [[fallthrough]];
    case config_resource_type::broker_logger:
        break;
    }
    return os << "{unknown type}";
}

std::ostream& operator<<(std::ostream& os, describe_configs_source s) {
    switch (s) {
    case describe_configs_source::topic:
        return os << "{topic}";
    case describe_configs_source::static_broker_config:
        return os << "{static_broker_config}";
    case describe_configs_source::default_config:
        return os << "{default_config}";
    }
    return os << "{unknown type}";
}

std::ostream& operator<<(std::ostream& os, config_resource_operation t) {
    switch (t) {
    case config_resource_operation::set:
        return os << "set";
    case config_resource_operation::append:
        return os << "append";
    case config_resource_operation::remove:
        return os << "remove";
    case config_resource_operation::subtract:
        return os << "subtract";
    }
    return os << "unknown type";
}

std::ostream& operator<<(std::ostream& os, scram_mechanism m) {
    switch (m) {
    case scram_mechanism::scram_sha_256:
        return os << "SCRAM-SHA-256";
    case scram_mechanism::scram_sha_512:
        return os << "SCRAM-SHA-512";
    case scram_mechanism::unknown:
        return os << "unknown";
    }
    return os << "unsupported type";
}

} // namespace kafka
