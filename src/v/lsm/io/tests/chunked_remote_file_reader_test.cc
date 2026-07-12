/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_io/tests/cache_test_fixture.h"
#include "cloud_io/tests/db_s3_imposter_fixture.h"
#include "cloud_io/tests/scoped_remote.h"
#include "container/chunked_vector.h"
#include "lsm/io/chunked_remote_file_reader.h"
#include "lsm/io/memory_persistence.h"
#include "random/generators.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace {

ss::abort_source g_never_abort;
constexpr uint64_t k_chunk_size = 16ULL * 1024ULL;
constexpr auto k_retry_timeout = std::chrono::seconds(10);
constexpr auto k_retry_backoff = std::chrono::milliseconds(10);

std::string ioarray_to_string(ioarray a) {
    std::string out;
    out.reserve(a.size());
    for (auto c : a.as_range()) {
        out.push_back(c);
    }
    return out;
}

// A random payload so that a read returning the wrong chunk's bytes is
// detected: over any range, two distinct positions almost never hold the same
// bytes, so a positional bug shows up as a content mismatch.
std::string random_payload(size_t size) {
    auto s = random_generators::gen_alphanum_string(size);
    return {s.begin(), s.end()};
}

} // namespace

class chunked_remote_file_reader_test
  : public cloud_io::cache_test_fixture
  , public ::db_s3_imposter_fixture
  , public seastar_test {
public:
    void SetUp() override {
        ::db_s3_imposter_fixture::start().get();
        _scoped = cloud_io::scoped_remote::create(1, conf);
    }

    void TearDown() override {
        _scoped->request_stop();
        _scoped.reset();
        ::db_s3_imposter_fixture::stop().get();
    }

    void upload(std::string_view key, std::string_view body) {
        retry_chain_node rtc(g_never_abort, 30s, 100ms);
        auto okey = cloud_storage_clients::object_key{ss::sstring{key}};
        auto r = remote()
                   .upload_object(
                     {.transfer_details
                      = {.bucket = bucket_name, .key = okey, .parent_rtc = rtc},
                      .display_str = "test-upload",
                      .payload = iobuf::from(body)})
                   .get();
        ASSERT_EQ(r, cloud_io::upload_result::success);
    }

    cloud_io::remote& remote() { return _scoped->remote.local(); }
    cloud_io::cache& cache() { return sharded_cache.local(); }

    // Opens a reader via the factory. The object must already exist, since
    // open() probes the tail chunk for existence.
    std::unique_ptr<lsm::io::chunked_remote_file_reader> open_reader(
      std::string_view object_key,
      uint64_t file_size,
      uint64_t chunk_size = k_chunk_size,
      std::string_view cache_prefix = "test") {
        auto r = lsm::io::chunked_remote_file_reader::open(
                   &cache(),
                   &remote(),
                   bucket_name,
                   cloud_storage_clients::object_key{ss::sstring{object_key}},
                   std::filesystem::path{std::string{cache_prefix}},
                   file_size,
                   chunk_size,
                   k_retry_timeout,
                   k_retry_backoff,
                   g_never_abort,
                   cloud_io::group_id::default_group)
                   .get();
        EXPECT_TRUE(r.has_value());
        return std::move(r.value());
    }

    // Mirrors chunked_remote_file_reader::chunk_cache_key with the test's
    // cache_key_prefix ("test") and chunk size.
    static std::filesystem::path expected_chunk_key(uint64_t chunk_start) {
        return std::filesystem::path("test") / fmt::format("c={}", k_chunk_size)
               / fmt::format("{}", chunk_start);
    }

private:
    std::unique_ptr<cloud_io::scoped_remote> _scoped;
};

TEST_F(chunked_remote_file_reader_test, OpenNotFound) {
    // No object uploaded: the tail probe 404s and open() yields nullopt.
    auto r = lsm::io::chunked_remote_file_reader::open(
               &cache(),
               &remote(),
               bucket_name,
               cloud_storage_clients::object_key{ss::sstring{"obj-missing"}},
               "test",
               /*file_size=*/1024,
               k_chunk_size,
               k_retry_timeout,
               k_retry_backoff,
               g_never_abort,
               cloud_io::group_id::default_group)
               .get();
    EXPECT_FALSE(r.has_value());
}

TEST_F(chunked_remote_file_reader_test, ReadEmpty) {
    upload("obj-empty", random_payload(1024));
    auto reader = open_reader("obj-empty", 1024);
    auto result = reader->read(0, 0).get();
    EXPECT_EQ(result.size(), 0u);
    reader->close().get();
}

TEST_F(chunked_remote_file_reader_test, ReadWithinSingleChunk) {
    constexpr size_t file_size = 1024;
    auto payload = random_payload(file_size);
    upload("obj-single", payload);

    auto reader = open_reader("obj-single", file_size);
    auto result = reader->read(50, 100).get();
    ASSERT_EQ(result.size(), 100u);
    EXPECT_EQ(ioarray_to_string(std::move(result)), payload.substr(50, 100));

    reader->close().get();
}

TEST_F(chunked_remote_file_reader_test, ReadAcrossChunkBoundary) {
    // 50 KiB file, 16 KiB chunks -> end-aligned chunks at
    // [0,2K), [2K,18K), [18K,34K), [34K,50K). A read from offset 16 KiB of
    // length 4 KiB straddles chunks 1 and 2.
    constexpr size_t file_size = 50 * 1024;
    auto payload = random_payload(file_size);
    upload("obj-cross", payload);

    auto reader = open_reader("obj-cross", file_size);
    auto result = reader->read(16 * 1024, 4 * 1024).get();
    ASSERT_EQ(result.size(), 4u * 1024);
    EXPECT_EQ(
      ioarray_to_string(std::move(result)),
      payload.substr(16 * 1024, 4 * 1024));

    reader->close().get();
}

TEST_F(chunked_remote_file_reader_test, ReadWholeFileSpanningChunks) {
    constexpr size_t file_size = 50 * 1024;
    auto payload = random_payload(file_size);
    upload("obj-whole", payload);

    auto reader = open_reader("obj-whole", file_size);
    auto result = reader->read(0, file_size).get();
    ASSERT_EQ(result.size(), file_size);
    EXPECT_EQ(ioarray_to_string(std::move(result)), payload);

    reader->close().get();
}

TEST_F(chunked_remote_file_reader_test, OpenWarmsOnlyTailChunk) {
    // open() probes the tail, so only the tail chunk [34 KiB, 50 KiB) should
    // be cached afterwards -- not the head.
    constexpr size_t file_size = 50 * 1024;
    upload("obj-tail", random_payload(file_size));

    auto reader = open_reader("obj-tail", file_size);
    EXPECT_EQ(
      cache().is_cached(expected_chunk_key(34 * 1024)).get(),
      cloud_io::cache_element_status::available);
    EXPECT_EQ(
      cache().is_cached(expected_chunk_key(0)).get(),
      cloud_io::cache_element_status::not_available);

    reader->close().get();
}

TEST_F(chunked_remote_file_reader_test, ReadAfterCacheWarmReusesCachedChunks) {
    constexpr size_t file_size = 50 * 1024;
    auto payload = random_payload(file_size);
    upload("obj-warm", payload);

    auto reader = open_reader("obj-warm", file_size);
    auto first = reader->read(0, file_size).get();
    EXPECT_EQ(first.size(), file_size);

    auto second = reader->read(16 * 1024, 4 * 1024).get();
    EXPECT_EQ(
      ioarray_to_string(std::move(second)),
      payload.substr(16 * 1024, 4 * 1024));

    reader->close().get();
}

TEST_F(chunked_remote_file_reader_test, ConcurrentColdReadsAreCorrect) {
    // Two concurrent reads of the same cold object exercise the fetch
    // coalescing path; both must return correct data.
    constexpr size_t file_size = 50 * 1024;
    auto payload = random_payload(file_size);
    upload("obj-concurrent", payload);

    auto reader = open_reader("obj-concurrent", file_size);
    chunked_vector<ss::future<ioarray>> reads;
    reads.push_back(reader->read(0, file_size));
    reads.push_back(reader->read(0, file_size));
    auto results = ss::when_all_succeed(reads.begin(), reads.end()).get();
    for (auto& r : results) {
        EXPECT_EQ(ioarray_to_string(std::move(r)), payload);
    }

    reader->close().get();
}

TEST_F(chunked_remote_file_reader_test, ReadsMatchNonChunked) {
    struct shape {
        uint64_t file_size;
        uint64_t chunk_size;
    };
    // Shapes hit every structural edge of the end-aligned partition: file
    // smaller than / equal to a chunk, a 1-byte head, head_size == 0, and a
    // partial head. Sizes are tiny so we can exhaustively read every
    // (start, end) pair.
    const std::vector<shape> shapes = {
      {1, 16},  // single 1-byte chunk
      {7, 16},  // file < chunk
      {16, 16}, // file == chunk: a single full chunk
      {17, 16}, // 1-byte head + 1 full
      {31, 16}, // 15-byte head + 1 full
      {32, 16}, // head_size == 0: two full chunks
      {33, 16}, // 1-byte head + two full
      {48, 16}, // head_size == 0: three full chunks
    };

    int idx = 0;
    for (const auto& s : shapes) {
        auto key = fmt::format("obj-shape-{}", idx++);
        auto payload = random_payload(s.file_size);
        upload(key, payload);

        // Each shape gets its own cache prefix (mirroring the per-SST prefix in
        // production) so chunk keys don't collide across shapes here.
        auto chunked = open_reader(key, s.file_size, s.chunk_size, key);
        auto chunked_closer = ss::defer([&] { chunked->close().get(); });

        // The oracle is a non-chunked, in-memory reader over the same bytes,
        // backed by a memory persistence. It shares neither the chunking nor
        // the DMA path, so any divergence is a real bug in the chunked reader.
        auto mem = lsm::io::make_memory_data_persistence();
        auto mem_closer = ss::defer([&] { mem->close().get(); });
        {
            auto w = mem->open_sequential_writer({}).get();
            w->append(iobuf::from(payload)).get();
            w->close().get();
        }
        auto oracle_opt = mem->open_random_access_reader({}, s.file_size).get();
        ASSERT_TRUE(bool(oracle_opt));
        auto oracle = std::move(*oracle_opt);
        auto oracle_closer = ss::defer([&] { oracle->close().get(); });

        // Every (start, end) pair, including zero-length and reads to EOF.
        for (uint64_t start = 0; start <= s.file_size; ++start) {
            for (uint64_t end = start; end <= s.file_size; ++end) {
                auto n = end - start;
                std::string got;
                std::string want;
                try {
                    got = ioarray_to_string(chunked->read(start, n).get());
                    want = ioarray_to_string(oracle->read(start, n).get());
                } catch (const std::exception& e) {
                    FAIL() << "read threw: " << e.what()
                           << " file=" << s.file_size
                           << " chunk=" << s.chunk_size << " start=" << start
                           << " end=" << end;
                }
                ASSERT_EQ(got, want)
                  << "file=" << s.file_size << " chunk=" << s.chunk_size
                  << " start=" << start << " end=" << end;
            }
        }
    }
}

TEST_F(chunked_remote_file_reader_test, OutOfRangeReadsThrow) {
    constexpr size_t file_size = 250;
    upload("obj-oob", random_payload(file_size));
    auto reader = open_reader("obj-oob", file_size, /*chunk_size=*/64);

    EXPECT_ANY_THROW(reader->read(file_size, 1).get());     // at EOF
    EXPECT_ANY_THROW(reader->read(file_size + 5, 1).get()); // past EOF
    EXPECT_ANY_THROW(reader->read(0, file_size + 1).get()); // length past EOF
    EXPECT_ANY_THROW(
      reader->read(file_size - 1, 2).get()); // last byte + 1 over

    // The exact-to-EOF read at the last byte is in range.
    EXPECT_NO_THROW(reader->read(file_size - 1, 1).get());

    reader->close().get();
}
