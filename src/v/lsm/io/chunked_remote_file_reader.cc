/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "lsm/io/chunked_remote_file_reader.h"

#include "base/vassert.h"
#include "cloud_io/io_result.h"
#include "container/chunked_vector.h"
#include "lsm/core/exceptions.h"
#include "lsm/io/file_io.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/when_all.hh>

#include <algorithm>
#include <exception>
#include <ranges>
#include <utility>

namespace lsm::io {

namespace {

// Rethrows an exception from a chunk operation as an lsm io error, mirroring
// disk_file_reader::read: pass an existing lsm io error through, preserve the
// error code of a system_error, and wrap anything else.
[[noreturn]] void rethrow_as_lsm_error(
  std::exception_ptr eptr, const chunked_remote_file_reader& self) {
    if (ssx::is_shutdown_exception(eptr)) {
        throw abort_requested_exception("io error reading {}: {}", self, eptr);
    }
    try {
        std::rethrow_exception(eptr);
    } catch (const std::system_error& err) {
        throw io_error_exception(
          err.code(), "io error reading {}: {}", self, err);
    } catch (const io_error_exception&) {
        throw;
    } catch (...) {
        throw io_error_exception("io error reading {}: {}", self, eptr);
    }
}

} // namespace

chunked_remote_file_reader::chunked_remote_file_reader(
  cloud_io::cache* cache,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key key,
  std::filesystem::path cache_key_prefix,
  uint64_t file_size,
  uint64_t chunk_size,
  ss::lowres_clock::duration retry_timeout,
  ss::lowres_clock::duration retry_backoff,
  ss::abort_source& as,
  cloud_io::group_id gid)
  : _cache(cache)
  , _remote(remote)
  , _bucket(std::move(bucket))
  , _key(std::move(key))
  , _cache_key_prefix(std::move(cache_key_prefix))
  , _file_size(file_size)
  , _chunk_size(chunk_size)
  , _retry_timeout(retry_timeout)
  , _retry_backoff(retry_backoff)
  , _as(as)
  , _gid(gid) {}

chunked_remote_file_reader::~chunked_remote_file_reader() {
    vassert(
      _gate.is_closed(),
      "chunked_remote_file_reader destroyed without close() ({})",
      _key);
    vassert(
      _in_flight.empty(), "in-flight map not empty: {}", _in_flight.size());
}

ss::future<std::optional<std::unique_ptr<chunked_remote_file_reader>>>
chunked_remote_file_reader::open(
  cloud_io::cache* cache,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key key,
  std::filesystem::path cache_key_prefix,
  uint64_t file_size,
  uint64_t chunk_size,
  ss::lowres_clock::duration retry_timeout,
  ss::lowres_clock::duration retry_backoff,
  ss::abort_source& as,
  cloud_io::group_id gid) {
    std::unique_ptr<chunked_remote_file_reader> reader(
      new chunked_remote_file_reader(
        cache,
        remote,
        std::move(bucket),
        std::move(key),
        std::move(cache_key_prefix),
        file_size,
        chunk_size,
        retry_timeout,
        retry_backoff,
        as,
        gid));

    // Prove existence by fetching the tail chunk. A common pattern is for SST
    // readers to read the back of the file (for footers, indexes, filters,
    // etc), so fetching the tail chunk serves two purposes: confirming object
    // existence, and warming cache with the back of the file.
    retry_chain_node rtc(as, retry_timeout, retry_backoff);
    auto tail_chunk = reader->chunk_for_offset(file_size - 1);
    auto probe = co_await ss::coroutine::as_future(
      reader->ensure_chunk_cached(tail_chunk, rtc));
    if (probe.failed()) {
        co_await reader->close();
        rethrow_as_lsm_error(probe.get_exception(), *reader);
    }
    auto handle = std::move(probe).get();
    if (!handle) {
        co_await reader->close();
        co_return std::nullopt;
    }
    co_await handle->close();
    co_return reader;
}

chunked_remote_file_reader::aligned_chunk
chunked_remote_file_reader::chunk_for_offset(uint64_t offset) const {
    if (offset >= _file_size) {
        throw io_error_exception(
          "trying to read offset {} of {} in {}", offset, _file_size, _key);
    }

    const uint64_t head_size = _file_size % _chunk_size;
    if (offset < head_size) {
        return {0, head_size};
    }
    // Everything past the head is a full chunk on a grid aligned starting at
    // head_size, bounded by the full file size.
    const uint64_t start = head_size
                           + (offset - head_size) / _chunk_size * _chunk_size;
    const uint64_t end = std::min(start + _chunk_size, _file_size);
    return {start, end};
}

std::filesystem::path
chunked_remote_file_reader::chunk_cache_key(aligned_chunk chunk) const {
    return _cache_key_prefix / fmt::format("c={}", _chunk_size)
           / fmt::format("{}", chunk.start);
}

ss::future<std::optional<ss::file>>
chunked_remote_file_reader::ensure_chunk_cached(
  aligned_chunk chunk, retry_chain_node& parent) {
    auto key = chunk_cache_key(chunk);

    while (true) {
        if (auto item = co_await _cache->get(key); item.has_value()) {
            // Already on disk (either from this loop, or a previous read()
            // call). Return the ss::file so even if the cache evicts it we can
            // still read.
            co_return std::move(item->body);
        }
        if (auto it = _in_flight.find(chunk.start); it != _in_flight.end()) {
            auto found_fut = it->second.get_future();
            // A download is already in flight. Wait for its outcome, then
            // re-open in the next iteration.
            switch (co_await std::move(found_fut)) {
            case chunk_fetch_outcome::present:
                break; // fall through and re-open from cache
            case chunk_fetch_outcome::absent:
                co_return std::nullopt;
            case chunk_fetch_outcome::failed:
                // The leader's fetch failed and rethrew the underlying error to
                // its own caller. Surface an equivalent failure here rather
                // than routing an exception through the shared future, which
                // could be dropped as an ignored exceptional future if a waiter
                // is torn down during shutdown.
                throw io_error_exception(
                  "in-flight fetch of chunk {} of {} failed",
                  chunk.start,
                  _key);
            }
        } else {
            // There isn't an in-flight download; kick one off and publish a
            // shared future.
            ss::shared_promise<chunk_fetch_outcome> sp;
            _in_flight.emplace(chunk.start, sp.get_shared_future());
            auto fetched = co_await ss::coroutine::as_future(
              download_chunk(chunk, parent));
            _in_flight.erase(chunk.start);
            if (fetched.failed()) {
                // Publish the failure to waiters and rethrow to
                // this caller.
                auto eptr = fetched.get_exception();
                sp.set_value(chunk_fetch_outcome::failed);
                std::rethrow_exception(eptr);
            }
            bool found = fetched.get();
            sp.set_value(
              found ? chunk_fetch_outcome::present
                    : chunk_fetch_outcome::absent);
            if (!found) {
                co_return std::nullopt;
            }
        }

        // We've determined the chunk exists in object storage, and should now
        // exist in the cache -- fallthrough to get() it. With enough cache
        // churn, we may repeatedly download and miss the cache lookup, so
        // bound the repetition with a deadline.
        if (ss::lowres_clock::now() >= parent.get_deadline()) {
            throw io_error_exception(
              "chunk {} of {} repeatedly evicted from cache after fetch",
              chunk.start,
              _key);
        }
    }
}

ss::future<bool> chunked_remote_file_reader::download_chunk(
  aligned_chunk chunk, retry_chain_node& parent) {
    auto key = chunk_cache_key(chunk);
    // http_byte_range is a [start, end] inclusive pair.
    cloud_storage_clients::http_byte_range byte_range{
      chunk.start, chunk.end - 1};

    auto result = co_await _remote->download_stream(
      {.bucket = _bucket, .key = _key, .parent_rtc = parent},
      [this,
       &key](uint64_t content_length, ss::input_stream<char> input_stream) {
          return save_chunk_to_cache(
            content_length, std::move(input_stream), key);
      },
      "chunked SST chunk download",
      /*acquire_hydration_units=*/true,
      byte_range,
      /*throttle_metric_ms_cb=*/{},
      _gid);

    switch (result) {
    case cloud_io::download_result::success:
        co_return true;
    case cloud_io::download_result::notfound:
        co_return false;
    case cloud_io::download_result::timedout:
        throw io_error_exception(
          "timeout fetching chunk {} of {}", chunk.start, _key);
    case cloud_io::download_result::failed:
        throw io_error_exception(
          "failed to fetch chunk {} of {}", chunk.start, _key);
    }
}

ss::future<uint64_t> chunked_remote_file_reader::save_chunk_to_cache(
  uint64_t content_length,
  ss::input_stream<char> input_stream,
  const std::filesystem::path& key) {
    std::exception_ptr ex;
    try {
        auto reservation = co_await _cache->reserve_space(content_length, 1);
        co_await _cache->put(key, input_stream, reservation);
    } catch (...) {
        ex = std::current_exception();
    }
    co_await input_stream.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    co_return content_length;
}

ss::future<ioarray> chunked_remote_file_reader::read(size_t offset, size_t n) {
    if (n == 0) {
        co_return ioarray{};
    }
    if (offset >= _file_size || n > _file_size - offset) {
        throw io_error_exception(
          "read of {} bytes at offset {} is out of range for {} (size {})",
          n,
          offset,
          _key,
          _file_size);
    }
    auto gate_hold = _gate.hold();
    retry_chain_node rtc(_as, _retry_timeout, _retry_backoff);

    chunked_vector<aligned_chunk> chunks;
    size_t pos = offset;
    size_t end_offset = offset + n;
    while (pos < end_offset) {
        auto chunk = chunk_for_offset(pos);
        chunks.push_back(chunk);
        pos = chunk.end;
    }

    // Kick off the cloud IO and subsequent reads in parallel (bounded by
    // client pool capacity) to lower read latencies.
    chunked_vector<ioarray> slices;
    slices.reserve(chunks.size());
    for (size_t i = 0; i < chunks.size(); ++i) {
        slices.emplace_back();
    }
    auto max_concurrent = std::min(_remote->concurrency(), size_t{10});
    auto indices = std::views::iota(size_t{0}, chunks.size());
    auto read_chunks = co_await ss::coroutine::as_future(
      ss::max_concurrent_for_each(
        indices,
        max_concurrent,
        [this, &chunks, &slices, &rtc, offset, n](
          this auto, size_t i) -> ss::future<> {
            const auto& chunk = chunks[i];
            auto handle = co_await ensure_chunk_cached(chunk, rtc);
            if (!handle) {
                throw io_error_exception(
                  "underlying object {} no longer present in cloud storage",
                  *this);
            }
            // Account for when the range of data being read isn't chunk
            // aligned; this may snip off part of the first and last chunks.
            const size_t slice_begin = std::max<size_t>(offset, chunk.start);
            const size_t slice_end = std::min<size_t>(offset + n, chunk.end);
            const size_t offset_in_chunk = slice_begin - chunk.start;
            const size_t length = slice_end - slice_begin;
            auto piece = co_await ss::coroutine::as_future(
              aligned_dma_read(*handle, offset_in_chunk, length));
            co_await handle->close().handle_exception(
              [](const std::exception_ptr&) {});
            slices[i] = std::move(piece).get();
        }));
    if (read_chunks.failed()) {
        rethrow_as_lsm_error(read_chunks.get_exception(), *this);
    }
    ioarray result;
    for (auto& slice : slices) {
        result = ioarray::concat(std::move(result), std::move(slice));
    }
    co_return result;
}

ss::future<> chunked_remote_file_reader::close() { return _gate.close(); }

fmt::iterator chunked_remote_file_reader::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{chunked_remote_file_reader key={} file_size={} chunk_size={}}}",
      _key,
      _file_size,
      _chunk_size);
}

} // namespace lsm::io
