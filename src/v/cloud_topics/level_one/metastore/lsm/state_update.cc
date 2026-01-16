/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/state_update.h"

#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/state_update.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l1 {

namespace {

// Checks the input new objects and ensures that they don't already exist in
// the state. Collects the input extents and objects into the input maps.
ss::future<std::expected<void, db_update_error>> validate_new_objects_missing(
  const chunked_vector<new_object>& new_objects,
  state_reader& state,
  sorted_extents_by_tidp_t& out_extents,
  chunked_hash_map<object_id, object_entry>& out_objects) {
    for (const auto& o : new_objects) {
        auto object_res = co_await state.get_object(o.oid);
        if (!object_res.has_value()) {
            co_return std::unexpected(
              db_update_error{fmt::format("Error getting object {}", o.oid)});
        }
        if (object_res.value().has_value()) {
            co_return std::unexpected(
              db_update_error{fmt::format("Object {} already exists", o.oid)});
        }
        auto data_size = o.collect_extents_by_tidp(&out_extents);
        out_objects.emplace(
          o.oid,
          object_entry{
            .total_data_size = data_size,
            .removed_data_size = 0,
            .footer_pos = o.footer_pos,
            .object_size = o.object_size,
          });
    }
    co_return std::expected<void, db_update_error>{};
}

// Returns the extents of the given partition that align exactly with the input
// intervals. If there any of the intervals don't align with exact intervals,
// returns an error.
ss::future<std::expected<void, db_update_error>> collect_exact_intervals(
  const model::topic_id_partition& tidp,
  const chunked_vector<offset_interval_set::interval>& intervals,
  state_reader& state,
  chunked_vector<ss::sstring>& out_extent_keys,
  chunked_hash_map<object_id, size_t>& out_sizes) {
    for (const auto& interval : intervals) {
        auto range_res = co_await state.get_extent_range(
          tidp, interval.base_offset, interval.last_offset);

        if (!range_res.has_value()) {
            co_return std::unexpected(
              db_update_error{fmt::format(
                "Error getting extent range for {} [{}, {}]",
                tidp,
                interval.base_offset,
                interval.last_offset)});
        }
        if (!range_res.value().has_value()) {
            co_return std::unexpected(
              db_update_error{fmt::format(
                "Partition {} doesn't contain extents that span exactly "
                "[{}, {}]",
                tidp,
                interval.base_offset,
                interval.last_offset)});
        }

        auto extent_gen = range_res.value()->get_rows();
        while (auto extent_res = co_await extent_gen()) {
            if (!extent_res->get().has_value()) {
                co_return std::unexpected(
                  db_update_error{fmt::format(
                    "Error iterating through {} extents in range [{}, {}]",
                    tidp,
                    interval.base_offset,
                    interval.last_offset)});
            }
            auto& row = extent_res->get().value();
            out_sizes[row.val.oid] += row.val.len;
            out_extent_keys.push_back(std::move(row.key));
        }
    }
    co_return std::expected<void, db_update_error>{};
}

ss::future<std::expected<void, db_update_error>> merge_compaction_state(
  const model::topic_id_partition& tidp,
  const compaction_state_update& comp_update,
  state_reader& state,
  metadata_row_value& out_meta_val,
  compaction_state& out_state) {
    // Get current metadata to validate and update compaction_epoch.
    auto meta_res = co_await state.get_metadata(tidp);
    if (!meta_res.has_value()) {
        co_return std::unexpected(
          db_update_error{fmt::format(
            "Error getting metadata for {} during compaction", tidp)});
    }
    if (!meta_res.value().has_value()) {
        co_return std::unexpected(
          db_update_error{
            fmt::format("Partition {} not tracked during compaction", tidp)});
    }
    auto& meta = meta_res.value().value();
    if (meta.compaction_epoch != comp_update.expected_compaction_epoch) {
        co_return std::unexpected(
          db_update_error{fmt::format(
            "Compaction epoch mismatch for {}: expected {}, got {}",
            tidp,
            comp_update.expected_compaction_epoch,
            meta.compaction_epoch)});
    }
    meta.compaction_epoch = partition_state::compaction_epoch_t{
      meta.compaction_epoch() + 1};
    out_meta_val = meta;

    auto comp_res = co_await state.get_compaction_metadata(tidp);
    if (!comp_res.has_value()) {
        co_return std::unexpected(
          db_update_error{
            fmt::format("Error getting compaction metadata for {}", tidp)});
    }

    // Validate and apply new cleaned ranges.
    auto merged_state = comp_res.value().value_or(compaction_state{});
    if (!comp_update.new_cleaned_ranges.empty()) {
        const auto& req_cleaned_ranges = comp_update.new_cleaned_ranges;

        // Check that ranges with tombstones don't overlap.
        for (const auto& req_cleaned_range : req_cleaned_ranges) {
            if (
              req_cleaned_range.has_tombstones
              && !merged_state.may_add(
                compaction_state::cleaned_range_with_tombstones{
                  .base_offset = req_cleaned_range.base_offset,
                  .last_offset = req_cleaned_range.last_offset,
                  .cleaned_with_tombstones_at = comp_update.cleaned_at,
                })) {
                co_return std::unexpected(
                  db_update_error{fmt::format(
                    "Cleaned range for {} has tombstones and overlaps "
                    "with an existing cleaned range with tombstones: "
                    "[{}, {}]",
                    tidp,
                    req_cleaned_range.base_offset,
                    req_cleaned_range.last_offset)});
            }
        }

        // Apply new cleaned ranges.
        for (const auto& req_cleaned_range : req_cleaned_ranges) {
            auto inserted = merged_state.cleaned_ranges.insert(
              req_cleaned_range.base_offset, req_cleaned_range.last_offset);
            vassert(
              inserted,
              "Invalid interval [{}, {}]",
              req_cleaned_range.base_offset,
              req_cleaned_range.last_offset);

            if (req_cleaned_range.has_tombstones) {
                auto inserted_tomb = merged_state.add(
                  compaction_state::cleaned_range_with_tombstones{
                    .base_offset = req_cleaned_range.base_offset,
                    .last_offset = req_cleaned_range.last_offset,
                    .cleaned_with_tombstones_at = comp_update.cleaned_at,
                  });
                vassert(
                  inserted_tomb,
                  "Failed to insert cleaned range with tombstones: "
                  "[{}, {}]",
                  req_cleaned_range.base_offset,
                  req_cleaned_range.last_offset);
            }
        }
    }
    // Validate and remove tombstone ranges.
    auto req_range_removed_tombstones
      = comp_update.removed_tombstones_ranges.make_stream();
    while (req_range_removed_tombstones.has_next()) {
        auto req_range = req_range_removed_tombstones.next();
        if (!merged_state.has_contiguous_range_with_tombstones(
              req_range.base_offset, req_range.last_offset)) {
            co_return std::unexpected(
              db_update_error{fmt::format(
                "Tombstone-removed range [{}, {}] for {} is not "
                "tracked as having tombstones",
                req_range.base_offset,
                req_range.last_offset,
                tidp)});
        }
        auto erased = merged_state.erase_contiguous_range_with_tombstones(
          req_range.base_offset, req_range.last_offset);
        vassert(
          erased,
          "Failed to remove range: [{}, {}]",
          req_range.base_offset,
          req_range.last_offset);
    }

    out_state = std::move(merged_state);
    co_return std::expected<void, db_update_error>{};
}

} // namespace

ss::future<std::expected<void, db_update_error>>
add_objects_db_update::build_rows(
  state_reader& state,
  chunked_vector<write_batch_row>& out,
  chunked_hash_map<model::topic_id_partition, kafka::offset>* corrections)
  const {
    auto validate_res = validate_inputs();
    if (!validate_res.has_value()) {
        co_return std::unexpected(validate_res.error());
    }
    sorted_extents_by_tidp_t new_extents_by_tp;
    chunked_hash_map<object_id, object_entry> new_objects_by_oid;
    auto new_extents_res = co_await validate_new_objects_missing(
      new_objects, state, new_extents_by_tp, new_objects_by_oid);
    if (!new_extents_res.has_value()) {
        co_return std::unexpected(new_extents_res.error());
    }

    chunked_hash_map<model::topic_id_partition, kafka::offset>
      corrected_next_offsets;
    chunked_hash_map<model::topic_id_partition, chunked_vector<extent>>
      verified_extents;
    chunked_hash_map<model::topic_id_partition, metadata_row_value>
      verified_meta_vals;
    for (const auto& [tidp, extents] : new_extents_by_tp) {
        // TODO: maybe we need some mount operation that adopts a partition log
        // and allows it to start a specific offset.
        auto meta_res = co_await state.get_metadata(tidp);
        if (!meta_res.has_value()) {
            co_return std::unexpected(
              db_update_error{
                fmt::format("Error getting metadata for {}", tidp)});
        }
        auto opt = meta_res.value();
        auto expected_next = opt ? opt->next_offset : kafka::offset{0};

        if (extents.begin()->base_offset != expected_next) {
            // If the start of the new extents for this partition aren't
            // aligned, allow the operation to succeed, but the expectation is
            // when applying, we'll "drop" these extents. Account for them as
            // removed data.
            for (const auto& extent : extents) {
                new_objects_by_oid[extent.oid].removed_data_size += extent.len;
            }
            corrected_next_offsets[tidp] = expected_next;
            continue;
        }
        for (const auto& extent : extents) {
            verified_extents[tidp].push_back(extent);
        }
        verified_meta_vals[tidp] = metadata_row_value{
          .start_offset = opt ? opt->start_offset : kafka::offset{0},
          .next_offset = kafka::next_offset(extents.rbegin()->last_offset),
          .compaction_epoch = opt ? opt->compaction_epoch
                                  : partition_state::compaction_epoch_t{0},
        };
    }
    // Now that we've validated the offsets of our extents, validate the terms
    // for the accepted extents.
    chunked_hash_map<model::topic_id_partition, absl::btree_set<term_start>>
      verified_terms;
    for (const auto& [tp, req_entries] : new_terms) {
        if (corrected_next_offsets.contains(tp)) {
            continue;
        }
        // First do a basic check that the incoming term entries can be
        // appended to our state without violating ordering requirements.
        auto term_res = co_await state.get_max_term(tp);
        if (!term_res.has_value()) {
            co_return std::unexpected(
              db_update_error{
                fmt::format("Error getting max term for {}", tp)});
        }
        auto term_opt = term_res.value();
        if (term_opt.has_value()) {
            auto req_first_entry = req_entries.begin();
            // NOTE: it's valid for the first requested term to be equal to the
            // last term (e.g. if leadership has not changed). The same cannot
            // be said about offsets, hence the difference in comparator.
            if (req_first_entry->term_id < term_opt->term_id) {
                co_return std::unexpected(
                  db_update_error{fmt::format(
                    "New term for {} must be >= last term: {} < {}",
                    tp,
                    req_first_entry->term_id,
                    term_opt->term_id)});
            }
            if (req_first_entry->start_offset <= term_opt->start_offset) {
                co_return std::unexpected(
                  db_update_error{fmt::format(
                    "New term for {} must start after last term: {} <= {}",
                    tp,
                    req_first_entry->start_offset,
                    term_opt->start_offset)});
            }
        }
        auto new_term_it = req_entries.begin();
        if (term_opt.has_value() && new_term_it->term_id == term_opt->term_id) {
            // If the first added term matches the back of the latest
            // tracked term, it isn't a new term.
            ++new_term_it;
        }
        verified_terms[tp].insert(new_term_it, req_entries.end());
    }
    if (corrections) {
        *corrections = std::move(corrected_next_offsets);
    }
    // Generate the rows.
    for (const auto& [oid, entry] : new_objects_by_oid) {
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid),
            .value = serde::to_iobuf(
              object_row_value{
                .object = entry,
              }),
          });
    }
    for (const auto& [tidp, entries] : verified_terms) {
        for (const auto& entry : entries) {
            out.emplace_back(
              write_batch_row{
                .key = term_row_key::encode(tidp, entry.term_id),
                .value = serde::to_iobuf(
                  term_row_value{
                    .term_start_offset = entry.start_offset,
                  }),
              });
        }
    }
    for (const auto& [tidp, entries] : verified_extents) {
        for (const auto& entry : entries) {
            out.emplace_back(
              write_batch_row{
                .key = extent_row_key::encode(tidp, entry.base_offset),
                .value = serde::to_iobuf(
                  extent_row_value{
                    .last_offset = entry.last_offset,
                    .max_timestamp = entry.max_timestamp,
                    .filepos = entry.filepos,
                    .len = entry.len,
                    .oid = entry.oid,
                  }),
              });
        }
    }
    for (const auto& [tidp, entry] : verified_meta_vals) {
        out.emplace_back(
          write_batch_row{
            .key = metadata_row_key::encode(tidp),
            .value = serde::to_iobuf(entry),
          });
    }
    co_return std::expected<void, db_update_error>{};
}

std::expected<void, db_update_error>
add_objects_db_update::validate_inputs() const {
    if (new_objects.empty()) {
        return std::unexpected(db_update_error{"No objects requested"});
    }
    if (new_terms.empty()) {
        return std::unexpected(db_update_error{"Missing term info in request"});
    }
    sorted_extents_by_tidp_t new_extents;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&new_extents);
    }
    for (const auto& [tidp, extents] : new_extents) {
        if (!new_terms.contains(tidp)) {
            return std::unexpected(
              db_update_error{fmt::format("Missing term info for {}", tidp)});
        }
        auto expected_next = extents.begin()->base_offset;
        for (const auto& extent : extents) {
            if (extent.base_offset != expected_next) {
                return std::unexpected(db_update_error(
                  fmt::format(
                    "Input object breaks partition {} offset ordering: "
                    "expected next: {}, actual: {}",
                    tidp,
                    expected_next,
                    extent.base_offset())));
            }
            expected_next = kafka::next_offset(extent.last_offset);
        }
    }
    for (const auto& [tidp, terms] : new_terms) {
        if (terms.empty()) {
            return std::unexpected(
              db_update_error{
                fmt::format("Empty terms requested for {}", tidp)});
        }
        auto extents_it = new_extents.find(tidp);
        if (extents_it == new_extents.end() || extents_it->second.empty()) {
            return std::unexpected(
              db_update_error{fmt::format(
                "Terms provided for a partition that has no extents", tidp)});
        }
        auto new_extents_start_offset
          = extents_it->second.begin()->base_offset();
        auto new_terms_start_offset = terms.begin()->start_offset;
        if (new_extents_start_offset != new_terms_start_offset) {
            return std::unexpected(
              db_update_error{fmt::format(
                "Extent start and term start do not match for {}: {} != {}",
                tidp,
                new_extents_start_offset,
                new_terms_start_offset)});
        }
        auto new_extents_last_offset = extents_it->second.rbegin()->last_offset;
        auto new_terms_last_start_offset = terms.back().start_offset;
        if (new_extents_last_offset < new_terms_last_start_offset) {
            return std::unexpected(
              db_update_error{fmt::format(
                "Extents end below a requested new term for {}: {} < {}",
                tidp,
                new_extents_last_offset,
                new_terms_last_start_offset)});
        }
        // Now check that the the term entries themselves (both terms and
        // offsets) are in increasing order.
        auto max_term_so_far = model::term_id{-1};
        auto max_offset_so_far = kafka::offset{-1};
        for (const auto& entry : terms) {
            if (
              entry.term_id <= max_term_so_far
              || entry.start_offset <= max_offset_so_far) {
                return std::unexpected(
                  db_update_error{fmt::format(
                    "Invalid term for {}: term={}, offset={}, "
                    "max_term_so_far={}, max_offset_so_far={}",
                    tidp,
                    entry.term_id,
                    entry.start_offset,
                    max_term_so_far,
                    max_offset_so_far)});
            }
            max_term_so_far = entry.term_id;
            max_offset_so_far = entry.start_offset;
        }
    }
    return std::expected<void, db_update_error>{};
}

ss::future<std::expected<void, db_update_error>>
replace_objects_db_update::build_rows(
  state_reader& state, chunked_vector<write_batch_row>& out) const {
    auto validate_res = validate_inputs();
    if (!validate_res.has_value()) {
        co_return std::unexpected(validate_res.error());
    }

    // Verify new objects don't exist.
    sorted_extents_by_tidp_t new_extents_by_tp;
    chunked_hash_map<object_id, object_entry> new_objects_map;
    auto new_extents_res = co_await validate_new_objects_missing(
      new_objects, state, new_extents_by_tp, new_objects_map);
    if (!new_extents_res.has_value()) {
        co_return std::unexpected(new_extents_res.error());
    }

    // Calculate contiguous intervals and validate that they align with
    // appropriate extents.
    auto contiguous_intervals_res = contiguous_intervals_for_extents(
      new_extents_by_tp);
    if (!contiguous_intervals_res.has_value()) {
        co_return std::unexpected(contiguous_intervals_res.error());
    }
    const auto& contiguous_intervals_by_tp = contiguous_intervals_res.value();
    chunked_hash_map<object_id, size_t> old_extent_sizes_by_oid;
    chunked_hash_map<model::topic_id_partition, chunked_vector<ss::sstring>>
      extent_keys_to_delete;
    for (const auto& [tidp, intervals] : contiguous_intervals_by_tp) {
        auto meta_res = co_await state.get_metadata(tidp);
        if (!meta_res.has_value()) {
            co_return std::unexpected(
              db_update_error{
                fmt::format("Error getting metadata for {}", tidp)});
        }
        if (!meta_res.value().has_value()) {
            co_return std::unexpected(
              db_update_error{
                fmt::format("Partition {} not tracked by state", tidp)});
        }
        auto exact_intervals_res = co_await collect_exact_intervals(
          tidp,
          intervals,
          state,
          extent_keys_to_delete[tidp],
          old_extent_sizes_by_oid);
        if (!exact_intervals_res.has_value()) {
            co_return std::unexpected(exact_intervals_res.error());
        }
    }

    // Update existing object entries to indicate the removal of data from
    // replaced extents.
    chunked_hash_map<object_id, object_entry> updated_old_objects;
    for (const auto& [oid, removed_size] : old_extent_sizes_by_oid) {
        auto obj_res = co_await state.get_object(oid);
        if (!obj_res.has_value()) {
            co_return std::unexpected(
              db_update_error{
                fmt::format("Error getting object {} for update", oid)});
        }
        if (obj_res.value().has_value()) {
            auto obj_entry = obj_res.value().value();
            obj_entry.removed_data_size += removed_size;
            updated_old_objects[oid] = obj_entry;
        }
        // If object doesn't exist, skip it (benign).
    }

    chunked_hash_map<model::topic_id_partition, compaction_state>
      merged_compaction_states;
    chunked_hash_map<model::topic_id_partition, metadata_row_value>
      updated_metadata;
    for (const auto& [t, p_updates] : compaction_updates) {
        for (const auto& [p, comp_update] : p_updates) {
            model::topic_id_partition tidp{t, p};
            auto merge_res = co_await merge_compaction_state(
              tidp,
              comp_update,
              state,
              updated_metadata[tidp],
              merged_compaction_states[tidp]);
            if (!merge_res.has_value()) {
                co_return std::unexpected(merge_res.error());
            }

            // If there are new cleaned ranges, check that the extents replace
            // down to the start of the log. By definition, this is a
            // requirement of cleaning the log.
            if (!comp_update.new_cleaned_ranges.empty()) {
                auto new_extent_iter = new_extents_by_tp.find(tidp);
                if (new_extent_iter != new_extents_by_tp.end()) {
                    auto req_extents_base
                      = new_extent_iter->second.begin()->base_offset;
                    auto start_offset = updated_metadata[tidp].start_offset;
                    if (req_extents_base > start_offset) {
                        co_return std::unexpected(
                          db_update_error{fmt::format(
                            "Cleaned range for {} does not replace to the "
                            "beginning of the log: {} > {}",
                            tidp,
                            req_extents_base,
                            start_offset)});
                    }
                }
            }
        }
    }

    // Generate the rows.
    chunked_hash_set<ss::sstring> added_extent_keys;
    for (const auto& [tidp, extents] : new_extents_by_tp) {
        for (const auto& extent : extents) {
            auto key = extent_row_key::encode(tidp, extent.base_offset);
            added_extent_keys.emplace(key);
            out.emplace_back(
              write_batch_row{
                .key = extent_row_key::encode(tidp, extent.base_offset),
                .value = serde::to_iobuf(
                  extent_row_value{
                    .last_offset = extent.last_offset,
                    .max_timestamp = extent.max_timestamp,
                    .filepos = extent.filepos,
                    .len = extent.len,
                    .oid = extent.oid,
                  }),
              });
        }
    }
    for (const auto& [tidp, keys] : extent_keys_to_delete) {
        for (const auto& key : keys) {
            if (added_extent_keys.contains(key)) {
                // Don't delete keys that are also being added.
                continue;
            }
            out.emplace_back(
              write_batch_row{
                .key = key,
                .value = iobuf{},
              });
        }
    }
    for (const auto& [oid, entry] : updated_old_objects) {
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid),
            .value = serde::to_iobuf(object_row_value{.object = entry}),
          });
    }
    for (const auto& [oid, entry] : new_objects_map) {
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid),
            .value = serde::to_iobuf(object_row_value{.object = entry}),
          });
    }
    for (const auto& [tidp, comp_state] : merged_compaction_states) {
        out.emplace_back(
          write_batch_row{
            .key = compaction_row_key::encode(tidp),
            .value = serde::to_iobuf(compaction_row_value{.state = comp_state}),
          });
    }
    for (const auto& [tidp, meta] : updated_metadata) {
        out.emplace_back(
          write_batch_row{
            .key = metadata_row_key::encode(tidp),
            .value = serde::to_iobuf(meta),
          });
    }
    co_return std::expected<void, db_update_error>{};
}

std::expected<void, db_update_error>
replace_objects_db_update::validate_inputs() const {
    if (new_objects.empty()) {
        return std::unexpected(db_update_error{"No objects requested"});
    }

    sorted_extents_by_tidp_t new_extents_by_tp;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&new_extents_by_tp);
    }

    auto contiguous_intervals = contiguous_intervals_for_extents(
      new_extents_by_tp);
    if (!contiguous_intervals.has_value()) {
        return std::unexpected(db_update_error(contiguous_intervals.error()));
    }

    // Validate compaction updates align with extents
    if (compaction_updates.empty()) {
        return std::expected<void, db_update_error>{};
    }
    for (const auto& [t, t_req] : compaction_updates) {
        for (const auto& [p, compaction_update] : t_req) {
            model::topic_id_partition tidp{t, p};

            // Verify partition has new extents
            auto new_extent_iter = new_extents_by_tp.find(tidp);
            if (new_extent_iter == new_extents_by_tp.end()) {
                return std::unexpected(
                  db_update_error{fmt::format(
                    "New cleaned range does not refer to partition with "
                    "extent: {}",
                    tidp)});
            }

            if (!compaction_update.new_cleaned_ranges.empty()) {
                const auto& req_cleaned_ranges
                  = compaction_update.new_cleaned_ranges;
                const auto& new_extents = new_extent_iter->second;

                // Check that the new extents span the cleaned ranges
                auto req_last = new_extents.rbegin()->last_offset;
                auto clean_last = req_cleaned_ranges.back().last_offset;
                if (clean_last > req_last) {
                    return std::unexpected(
                      db_update_error{fmt::format(
                        "Cleaned range for {} does not match requested "
                        "new extents' last_offset {} > {}",
                        tidp,
                        clean_last,
                        req_last)});
                }

                auto req_extents_base = new_extents.begin()->base_offset;
                auto clean_base = req_cleaned_ranges.front().base_offset;
                if (req_extents_base > clean_base) {
                    return std::unexpected(
                      db_update_error{fmt::format(
                        "Cleaned range start_offset for {} is not "
                        "covered by extents: {} > {}",
                        tidp,
                        req_extents_base,
                        clean_base)});
                }
            }
        }
    }

    return std::expected<void, db_update_error>{};
}

} // namespace cloud_topics::l1
