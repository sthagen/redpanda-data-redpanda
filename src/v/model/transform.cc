/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "model/transform.h"

#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"
#include "utils/vint.h"

#include <seastar/core/print.hh>

#include <algorithm>
#include <optional>
#include <stdexcept>

namespace model {

namespace {

bool validate_kv(iobuf_const_parser* parser) {
    auto [key_length, kl] = parser->read_varlong();
    // -1 is valid size of a `null` record, and is used to distinguish between
    // `null` and empty bytes.
    if (key_length < -1) {
        return false;
    }
    if (key_length > 0) {
        parser->skip(key_length);
    }
    auto [value_length, vl] = parser->read_varlong();
    if (value_length < -1) {
        return false;
    }
    if (value_length > 0) {
        parser->skip(value_length);
    }
    return true;
}

bool validate_record_payload(const iobuf& buf) {
    iobuf_const_parser parser(buf);
    if (!validate_kv(&parser)) {
        return false;
    }
    auto [header_count, hc] = parser.read_varlong();
    if (header_count < 0) {
        return false;
    }
    for (int64_t i = 0; i < header_count; ++i) {
        if (!validate_kv(&parser)) {
            return false;
        }
    }
    return parser.bytes_left() == 0;
}

} // namespace

std::ostream& operator<<(std::ostream& os, const transform_metadata& meta) {
    fmt::print(
      os,
      "{{name: \"{}\", input: {}, outputs: {}, "
      "env: <redacted>, uuid: {}, source_ptr: {} }}",
      meta.name,
      meta.input_topic,
      meta.output_topics,
      // skip env becuase of pii
      meta.uuid,
      meta.source_ptr);
    return os;
}

std::ostream& operator<<(std::ostream& os, const transform_offsets_key& key) {
    fmt::print(
      os, "{{ transform id: {}, partition: {} }}", key.id, key.partition);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const transform_offsets_value& value) {
    fmt::print(os, "{{ offset: {} }}", value.offset);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const transform_report::processor& p) {
    fmt::print(os, "{{id: {}, status: {}, node: {}}}", p.id, p.status, p.node);
    return os;
}

transform_report::transform_report(transform_metadata meta)
  : metadata(std::move(meta))
  , processors() {}

transform_report::transform_report(
  transform_metadata meta, absl::btree_map<model::partition_id, processor> map)
  : metadata(std::move(meta))
  , processors(std::move(map)){};

void transform_report::add(processor processor) {
    processors.insert_or_assign(processor.id, processor);
}

void cluster_transform_report::add(
  transform_id id,
  const transform_metadata& meta,
  transform_report::processor processor) {
    auto [it, _] = transforms.try_emplace(id, meta);
    it->second.add(processor);
}

void cluster_transform_report::merge(const cluster_transform_report& other) {
    for (const auto& [tid, treport] : other.transforms) {
        for (const auto& [pid, preport] : treport.processors) {
            add(tid, treport.metadata, preport);
        }
    }
}

std::ostream&
operator<<(std::ostream& os, transform_report::processor::state s) {
    return os << processor_state_to_string(s);
}

std::string_view
processor_state_to_string(transform_report::processor::state state) {
    switch (state) {
    case transform_report::processor::state::inactive:
        return "inactive";
    case transform_report::processor::state::running:
        return "running";
    case transform_report::processor::state::errored:
        return "errored";
    case transform_report::processor::state::unknown:
        break;
    }
    return "unknown";
}

transformed_data::transformed_data(iobuf d)
  : _data(std::move(d)) {}

std::optional<transformed_data> transformed_data::create_validated(iobuf buf) {
    try {
        if (!validate_record_payload(buf)) {
            return std::nullopt;
        }
    } catch (const std::out_of_range&) {
        return std::nullopt;
    }
    return transformed_data(std::move(buf));
}

iobuf transformed_data::to_serialized_record(
  record_attributes attrs, int64_t timestamp_delta, int32_t offset_delta) && {
    iobuf out;
    out.reserve_memory(sizeof(attrs) + vint::max_length * 3);
    // placeholder for the final length.
    auto placeholder = out.reserve(vint::max_length);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    const auto attr = ss::cpu_to_be(attrs.value());
    out.append(reinterpret_cast<const char*>(&attr), sizeof(attr));

    bytes td = vint::to_bytes(timestamp_delta);
    out.append(td.data(), td.size());

    bytes od = vint::to_bytes(offset_delta);
    out.append(od.data(), od.size());

    out.append_fragments(std::move(_data));

    bytes encoded_size = vint::to_bytes(
      int64_t(out.size_bytes() - vint::max_length));

    // Write out the size at the end of the reserved space we took at the
    // beginning.
    placeholder.write_end(encoded_size.data(), encoded_size.size());
    // drop the bytes we reserved, but didn't use.
    out.trim_front(vint::max_length - encoded_size.size());

    return out;
}

} // namespace model