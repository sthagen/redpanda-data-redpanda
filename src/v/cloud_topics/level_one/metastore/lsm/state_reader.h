/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

// Encapsulate queries that operate on state in a database.
class state_reader {
public:
    enum class errc {
        io_error,
        corruption,
        shutting_down,
    };

    struct extent_row {
        ss::sstring key;
        extent_row_value val;
    };

    enum class direction { forward, backward };

    class extent_key_range {
    public:
        extent_key_range(
          ss::sstring base, ss::sstring last, lsm::iterator it, direction dir)
          : _base_key(std::move(base))
          , _last_key(std::move(last))
          , _iter(std::move(it))
          , _direction(dir) {}

        // Returns extent_rows matching exactly between _base_key and
        // _last_key, or generates an error if it can't.
        //
        // Stops generating after the first error.
        ss::coroutine::experimental::generator<std::expected<extent_row, errc>>
        get_rows();

        ss::future<chunked_vector<std::expected<extent_row, errc>>>
        materialize_rows();

    private:
        ss::sstring to_string();

        ss::sstring _base_key;
        ss::sstring _last_key;

        // Snapshot of the database.
        lsm::iterator _iter;
        direction _direction;
    };

    explicit state_reader(lsm::snapshot snap)
      : snap_(std::move(snap)) {}

    ss::future<std::expected<std::optional<metadata_row_value>, errc>>
    get_metadata(const model::topic_id_partition&);

    ss::future<std::expected<std::optional<compaction_state>, errc>>
    get_compaction_metadata(const model::topic_id_partition&);

    ss::future<std::expected<std::optional<object_entry>, errc>>
      get_object(object_id);

    // Returns the highest term start for the given partition, or nullopt if
    // the partition does not exist.
    ss::future<std::expected<std::optional<term_start>, errc>>
    get_max_term(const model::topic_id_partition&);

    // Returns the first extent that contains an offset at or equal to the
    // given offset. If no such extent exists (e.g. all extents are below the
    // offset) returns nullopt.
    ss::future<std::expected<std::optional<extent>, errc>>
    get_extent_ge(const model::topic_id_partition&, kafka::offset);

    // Returns the key ranges whose first extent's base offset matches `base`
    // and whose last extent's last offset matches `last`. If no such range
    // exists, return nullopt.
    ss::future<std::expected<std::optional<extent_key_range>, errc>>
    get_extent_range(
      const model::topic_id_partition&, kafka::offset base, kafka::offset last);

    // Returns an iterator for all extents that overlap with the inclusive
    // range [min_offset, max_offset]. Returns nullopt if there are no extents
    // in the given bounds.
    ss::future<std::expected<std::optional<extent_key_range>, errc>>
    get_inclusive_extents(
      const model::topic_id_partition&,
      std::optional<kafka::offset> min_offset,
      std::optional<kafka::offset> max_offset);
    ss::future<std::expected<std::optional<extent_key_range>, errc>>
    get_inclusive_extents_backward(
      const model::topic_id_partition&,
      std::optional<kafka::offset> min_offset,
      std::optional<kafka::offset> max_offset);

    // Returns the term containing the given offset, i.e. the term with the
    // start_offset <= the given offset, or  nullopt if no such term exists.
    ss::future<std::expected<std::optional<term_start>, errc>>
    get_term_le(const model::topic_id_partition&, kafka::offset);

    // Returns the end offset (exclusive) for the given term:
    // - If a higher term exists, returns the start_offset of the next term
    // - If this is the highest term, the partition's next_offset
    // Returns nullopt if all terms are below the given term or the term
    // doesn't exist.
    ss::future<std::expected<std::optional<kafka::offset>, errc>>
    get_term_end(const model::topic_id_partition&, model::term_id);

private:
    ss::future<
      std::expected<std::optional<std::pair<ss::sstring, ss::sstring>>, errc>>
    find_inclusive_extent_keys(
      const model::topic_id_partition&,
      std::optional<kafka::offset> min_offset,
      std::optional<kafka::offset> max_offset);

    template<typename KeyT, typename ValT, typename... KeyEncodeArgs>
    ss::future<std::expected<std::optional<ValT>, errc>>
    get_val(KeyEncodeArgs...);

    lsm::snapshot snap_;
};

} // namespace cloud_topics::l1
