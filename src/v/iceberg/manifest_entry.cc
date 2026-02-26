/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/manifest_entry.h"

namespace iceberg {
namespace {

constexpr auto copy_map = [](const auto& m) {
    std::decay_t<decltype(m)> ret;
    ret.reserve(m.size());
    for (auto& [k, v] : m) {
        if constexpr (requires { v.copy(); }) {
            ret.emplace(k, v.copy());
        } else {
            ret.emplace(k, v);
        }
    }
    return ret;
};

} // namespace
data_file data_file::copy() const {
    return data_file{
      .content_type = content_type,
      .file_path = file_path,
      .file_format = file_format,
      .partition = partition.copy(),
      .record_count = record_count,
      .file_size_bytes = file_size_bytes,
      .column_sizes = column_sizes.transform(copy_map),
      .value_counts = value_counts.transform(copy_map),
      .null_value_counts = null_value_counts.transform(copy_map),
      .nan_value_counts = nan_value_counts.transform(copy_map),
      .lower_bounds = lower_bounds.transform(copy_map),
      .upper_bounds = upper_bounds.transform(copy_map),
      .key_metadata = key_metadata.transform(&iobuf::copy),
      .split_offsets = split_offsets.transform(&chunked_vector<int64_t>::copy),
      .equality_ids = equality_ids.transform(
        &chunked_vector<nested_field::id_t>::copy),
      .sort_order_id = sort_order_id,
      .referenced_data_file = referenced_data_file,
    };
}

manifest_entry manifest_entry::copy() const {
    return manifest_entry{
      .status = status,
      .snapshot_id = snapshot_id,
      .sequence_number = sequence_number,
      .file_sequence_number = file_sequence_number,
      .data_file = data_file.copy(),
    };
}

} // namespace iceberg
