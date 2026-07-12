/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_io/admission_control_types.h"
#include "cloud_io/cache_service.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "container/chunked_hash_map.h"
#include "lsm/io/persistence.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_future.hh>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>

namespace lsm::io {

/// A random_access_file_reader for an SST stored as a cloud object, read in
/// fixed-size chunks hydrated into the cloud_io disk cache on demand.
///
/// Chunks are aligned to the end of the file, so the SST's trailing
/// footer/index/filter land in the tail chunk and open() costs one range GET.
///
/// The cloud_io disk cache owns residency (which chunk bytes stay local). This
/// reader holds chunk handles only for the duration of a read() -- it opens
/// the covering chunks, reads them, and closes them before returning -- so
/// open fds are bounded by in-flight reads, with no persistent handle map.
///
/// Concurrent reads that miss the same uncached chunk coalesce their cloud
/// fetch behind a shared future, so a burst of readers for a cold SST issues
/// one range GET per chunk rather than one per reader.
class chunked_remote_file_reader final : public random_access_file_reader {
public:
    /// Open the SST. Probes existence by ensuring the tail chunk is cached
    /// (one range GET, which also warms the footer/index for the first reads);
    /// returns nullopt if the object does not exist (404). Throws on other I/O
    /// errors.
    static ss::future<
      std::optional<std::unique_ptr<chunked_remote_file_reader>>>
    open(
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
      cloud_io::group_id gid);

    chunked_remote_file_reader(const chunked_remote_file_reader&) = delete;
    chunked_remote_file_reader&
    operator=(const chunked_remote_file_reader&) = delete;
    chunked_remote_file_reader(chunked_remote_file_reader&&) = delete;
    chunked_remote_file_reader&
    operator=(chunked_remote_file_reader&&) = delete;
    ~chunked_remote_file_reader() override;

    ss::future<ioarray> read(size_t offset, size_t n) override;
    ss::future<> close() override;
    fmt::iterator format_to(fmt::iterator it) const override;

    uint64_t file_size() const { return _file_size; }
    uint64_t chunk_size() const { return _chunk_size; }

private:
    chunked_remote_file_reader(
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
      cloud_io::group_id gid);

    /// A half-open byte range [start, end) within the file. Fixed to
    /// _chunk_size in size (except potentially for the first chunk) and
    /// end-aligned to the end of the file (_file_size).
    struct aligned_chunk {
        uint64_t start;
        uint64_t end;
    };
    /// The chunk containing `offset`, end-aligned to the file end.
    aligned_chunk chunk_for_offset(uint64_t offset) const;

    /// Cache key for `chunk`. Encodes the chunk size so changing it between
    /// runs invalidates rather than collides.
    std::filesystem::path chunk_cache_key(aligned_chunk chunk) const;

    /// Ensure `chunk` is in the disk cache and return an open handle to it.
    /// The caller owns the handle and must close() it. Returns nullopt if the
    /// object does not exist (404). Concurrent calls for the same chunk
    /// coalesce the fetch.
    ss::future<std::optional<ss::file>>
    ensure_chunk_cached(aligned_chunk chunk, retry_chain_node& parent);

    /// Range-GET a single chunk into the cache. true on success, false on 404.
    ss::future<bool>
    download_chunk(aligned_chunk chunk, retry_chain_node& parent);

    /// Reserve space and stream the response body into the cache. Always
    /// closes `input_stream`; returns the content length.
    ss::future<uint64_t> save_chunk_to_cache(
      uint64_t content_length,
      ss::input_stream<char> input_stream,
      const std::filesystem::path& key);

    cloud_io::cache* _cache;
    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    cloud_storage_clients::object_key _key;
    std::filesystem::path _cache_key_prefix;
    uint64_t _file_size;
    uint64_t _chunk_size;
    ss::lowres_clock::duration _retry_timeout;
    ss::lowres_clock::duration _retry_backoff;
    ss::abort_source& _as;
    cloud_io::group_id _gid;

    /// Outcome of an in-flight chunk fetch, published to coalesced waiters.
    /// Failure is encoded as a value.
    enum class chunk_fetch_outcome : uint8_t {
        /// The object exists and the chunk was hydrated into the cache.
        present,
        /// The object does not exist (404).
        absent,
        /// The fetch failed transiently.
        failed,
    };

    // In-flight chunk fetches keyed by chunk start. An entry exists only while
    // a download is in progress. Coalesces concurrent fetches of the same
    // chunk.
    chunked_hash_map<uint64_t, ss::shared_future<chunk_fetch_outcome>>
      _in_flight;
    ss::gate _gate;
};

} // namespace lsm::io
