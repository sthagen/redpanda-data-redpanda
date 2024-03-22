// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <system_error>
#include <vector>

namespace redpanda {

/**
 * Owned variable sized byte array.
 */
using bytes = std::vector<uint8_t>;

/**
 * Unowned variable sized byte array.
 *
 * This is not `std::span` because `std::span` does not provide
 * an equality operator, and it's a mutable view, where we want an immutable
 * one.
 */
class bytes_view : public std::ranges::view_interface<bytes_view> {
public:
    /** A default empty view of bytes. */
    bytes_view() = default;
    /** A view over `size` bytes starting at `start`. */
    bytes_view(bytes::const_pointer start, size_t size);
    /** A view over `size` bytes starting at `start`. */
    bytes_view(bytes::const_iterator start, size_t size);
    /** A view from `start` to `end`. */
    bytes_view(bytes::const_iterator start, bytes::const_iterator end);
    /** A view from `start` to `end`. */
    bytes_view(bytes::const_pointer start, bytes::const_pointer end);
    /** A view over an entire bytes range. */
    // NOLINTNEXTLINE(*-explicit-*)
    bytes_view(const bytes&);

    /** The start of this range. */
    [[nodiscard]] bytes::const_pointer begin() const;
    /** The end of this range. */
    [[nodiscard]] bytes::const_pointer end() const;
    /**
     * Create a subview of this view. Similar to `std::span::subspan` or
     * `std::string_view::substr`.
     */
    [[nodiscard]] bytes_view
    subview(size_t offset, size_t count = std::dynamic_extent) const;

    /** Access the nth element of the view. */
    bytes::value_type operator[](size_t n) const;

    /** Returns true if the views have the same contents. */
    bool operator==(const bytes_view& other) const;

    /** Convert this bytes view into a string view. */
    explicit operator std::string_view() const;

private:
    bytes::const_pointer _start{}, _end{};
};

/**
 * A zero-copy `header`.
 */
struct header_view {
    std::string_view key;
    std::optional<bytes_view> value;

    bool operator==(const header_view&) const = default;
};

/**
 * A zero-copy representation of a record within Redpanda.
 *
 * `review_view` are handed to [`on_record_written`] event handlers as the
 * record that Redpanda wrote.
 */
struct record_view {
    std::optional<bytes_view> key;
    std::optional<bytes_view> value;
    std::vector<header_view> headers;

    bool operator==(const record_view&) const = default;
};

/**
 * Records may have a collection of headers attached to them.
 *
 * Headers are opaque to the broker and are purely a mechanism for the producer
 * and consumers to pass information.
 */
struct header {
    std::string key;
    std::optional<bytes> value;

    // NOLINTNEXTLINE(*-explicit-*)
    operator header_view() const;
    bool operator==(const header&) const = default;
};

/**
 * A record in Redpanda.
 *
 * Records are generated as the result of any transforms that act upon a
 * `record_view`.
 */
struct record {
    std::optional<bytes> key;
    std::optional<bytes> value;
    std::vector<header> headers;

    // NOLINTNEXTLINE(*-explicit-*)
    operator record_view() const;
    bool operator==(const record&) const = default;
};

/**
 * A persisted record written to a topic within Redpanda.
 *
 * It is similar to a `record_view` except that it also carries a timestamp of
 * when it was produced.
 */
struct written_record {
    std::optional<bytes_view> key;
    std::optional<bytes_view> value;
    std::vector<header_view> headers;
    std::chrono::system_clock::time_point timestamp;

    // NOLINTNEXTLINE(*-explicit-*)
    operator record_view() const;
    bool operator==(const written_record&) const = default;
};

/**
 * An event generated after a write event within the broker.
 */
struct write_event {
    /** The record that was written as part of this event. */
    written_record record;
};

/**
 * A writer for transformed records that are output to the destination topic.
 */
class record_writer {
public:
    record_writer() = default;
    record_writer(const record_writer&) = delete;
    record_writer(record_writer&&) = delete;
    record_writer& operator=(const record_writer&) = delete;
    record_writer& operator=(record_writer&&) = delete;
    virtual ~record_writer() = default;

    /**
     * Write a record to the output topic, returning any errors.
     */
    virtual std::error_code write(record_view) = 0;
};

/**
 * A callback to process write events and respond with a number of records to
 * write back to the output topic.
 */
using on_record_written_callback
  = std::function<std::error_code(write_event, record_writer*)>;

/**
 * Register a callback to be fired when a record is written to the input topic.
 *
 * This callback is triggered after the record has been written and fsynced to
 * disk and the producer has been acknowledged.
 *
 * This method blocks and runs forever, it should be called from `main` and any
 * setup that is needed can be done before calling this method.
 *
 * # Example
 *
 * The following example copies every record on the input topic indentically to
 * the output topic.
 *
 * ```cpp
 * int main() {
 *   redpanda::on_record_written(
 *       [](redpanda::write_event event, redpanda::record_writer* writer) {
 *          return writer->write(event.record);
 *       });
 * }
 * ```
 */
[[noreturn]] void on_record_written(const on_record_written_callback& callback);

} // namespace redpanda
