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
#include "bytes/iobuf_parser.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_io/tests/db_s3_imposter_fixture.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage_clients/types.h"
#include "test_utils/test.h"

#include <memory>

using namespace std::chrono_literals;
using namespace cloud_io;

static ss::abort_source never_abort;

static iobuf iobuf_from(std::string_view s) {
    iobuf b;
    b.append(s.data(), s.size());
    return b;
}

static ss::sstring iobuf_to_string(iobuf buf) {
    iobuf_parser p(std::move(buf));
    return p.read_string(p.bytes_left());
}

class db_s3_imposter_test
  : public db_s3_imposter_fixture
  , public seastar_test {
public:
    void SetUp() override {
        start().get();
        _scoped = scoped_remote::create(1, conf);
    }

    void TearDown() override {
        _scoped->request_stop();
        _scoped.reset();
        stop().get();
    }

    cloud_io::remote& remote() { return _scoped->remote.local(); }

    cloud_storage_clients::object_key object_key(std::string_view key) {
        return cloud_storage_clients::object_key{ss::sstring{key}};
    }

    upload_result upload(std::string_view key, std::string_view body) {
        retry_chain_node rtc(never_abort, 5s, 100ms);
        auto okey = object_key(key);
        return remote()
          .upload_object(
            {.transfer_details
             = {.bucket = bucket_name, .key = okey, .parent_rtc = rtc},
             .display_str = "test-upload",
             .payload = iobuf_from(body)})
          .get();
    }

    std::pair<download_result, ss::sstring> download(std::string_view key) {
        retry_chain_node rtc(never_abort, 5s, 100ms);
        auto okey = object_key(key);
        iobuf payload;
        auto res
          = remote()
              .download_object(
                {.transfer_details
                 = {.bucket = bucket_name, .key = okey, .parent_rtc = rtc},
                 .display_str = "test-download",
                 .payload = payload})
              .get();
        return {res, iobuf_to_string(std::move(payload))};
    }

    download_result head(std::string_view key) {
        retry_chain_node rtc(never_abort, 5s, 100ms);
        return remote()
          .object_exists(bucket_name, object_key(key), rtc, "test-head")
          .get();
    }

    upload_result del(std::string_view key) {
        retry_chain_node rtc(never_abort, 5s, 100ms);
        auto okey = object_key(key);
        return remote()
          .delete_object(
            {.bucket = bucket_name, .key = okey, .parent_rtc = rtc})
          .get();
    }

    list_result list(
      std::optional<ss::sstring> prefix = std::nullopt,
      std::optional<char> delimiter = std::nullopt,
      std::optional<size_t> max_keys = std::nullopt,
      std::optional<ss::sstring> continuation = std::nullopt) {
        retry_chain_node rtc(never_abort, 5s, 100ms);
        auto pfx
          = prefix
              ? std::optional<cloud_storage_clients::
                                object_key>{cloud_storage_clients::object_key{
                  *prefix}}
              : std::nullopt;
        return remote()
          .list_objects(
            bucket_name,
            rtc,
            pfx,
            delimiter,
            std::nullopt,
            max_keys,
            continuation)
          .get();
    }

private:
    std::unique_ptr<scoped_remote> _scoped;
};

TEST_F(db_s3_imposter_test, put_get_roundtrip) {
    auto body = "the quick brown fox jumps over the lazy dog";
    ASSERT_EQ(upload("my/key", body), upload_result::success);

    auto [res, content] = download("my/key");
    ASSERT_EQ(res, download_result::success);
    EXPECT_EQ(content, body);
}

TEST_F(db_s3_imposter_test, get_nonexistent) {
    auto [res, content] = download("does/not/exist");
    EXPECT_EQ(res, download_result::notfound);
    EXPECT_TRUE(content.empty());
}

TEST_F(db_s3_imposter_test, put_overwrites_existing) {
    ASSERT_EQ(upload("key", "first"), upload_result::success);
    ASSERT_EQ(upload("key", "second"), upload_result::success);

    auto [res, content] = download("key");
    ASSERT_EQ(res, download_result::success);
    EXPECT_EQ(content, "second");
}

TEST_F(db_s3_imposter_test, delete_removes_object) {
    ASSERT_EQ(upload("ephemeral", "data"), upload_result::success);
    ASSERT_EQ(del("ephemeral"), upload_result::success);

    EXPECT_EQ(head("ephemeral"), download_result::notfound);
}

TEST_F(db_s3_imposter_test, head_exists_and_missing) {
    ASSERT_EQ(upload("present", "x"), upload_result::success);

    EXPECT_EQ(head("present"), download_result::success);
    EXPECT_EQ(head("absent"), download_result::notfound);
}

TEST_F(db_s3_imposter_test, list_objects_with_prefix) {
    ASSERT_EQ(upload("a/1", ""), upload_result::success);
    ASSERT_EQ(upload("a/2", ""), upload_result::success);
    ASSERT_EQ(upload("b/1", ""), upload_result::success);

    auto result = list("a/");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().contents.size(), 2);

    for (const auto& item : result.value().contents) {
        EXPECT_TRUE(item.key.starts_with("a/"));
    }
}

TEST_F(db_s3_imposter_test, list_objects_with_delimiter) {
    ASSERT_EQ(upload("dir1/file1", ""), upload_result::success);
    ASSERT_EQ(upload("dir1/file2", ""), upload_result::success);
    ASSERT_EQ(upload("dir2/file1", ""), upload_result::success);
    ASSERT_EQ(upload("root-file", ""), upload_result::success);

    auto result = list(std::nullopt, '/');
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().contents.size(), 1);
    EXPECT_EQ(result.value().contents.front().key, "root-file");
    EXPECT_EQ(result.value().common_prefixes.size(), 2);
}

TEST_F(db_s3_imposter_test, list_objects_pagination) {
    for (int i = 0; i < 5; i++) {
        auto key = fmt::format("obj/{:04d}", i);
        ASSERT_EQ(upload(key, "x"), upload_result::success);
    }

    auto page1 = list(std::nullopt, std::nullopt, 3);
    ASSERT_TRUE(page1.has_value());
    EXPECT_EQ(page1.value().contents.size(), 3);
    EXPECT_TRUE(page1.value().is_truncated);
    EXPECT_FALSE(page1.value().next_continuation_token.empty());

    auto page2 = list(
      std::nullopt, std::nullopt, 3, page1.value().next_continuation_token);
    ASSERT_TRUE(page2.has_value());
    EXPECT_EQ(page2.value().contents.size(), 2);
    EXPECT_FALSE(page2.value().is_truncated);
}

TEST_F(db_s3_imposter_test, batch_delete) {
    std::vector<cloud_storage_clients::object_key> keys;
    for (int i = 0; i < 5; i++) {
        auto key = fmt::format("batch/{}", i);
        ASSERT_EQ(upload(key, "x"), upload_result::success);
        keys.emplace_back(key);
    }

    retry_chain_node rtc(never_abort, 5s, 100ms);
    auto res = remote()
                 .delete_objects(
                   bucket_name, std::move(keys), rtc, [](size_t) {})
                 .get();
    ASSERT_EQ(res, upload_result::success);

    auto remaining = list("batch/");
    ASSERT_TRUE(remaining.has_value());
    EXPECT_EQ(remaining.value().contents.size(), 0);
}

TEST_F(db_s3_imposter_test, multipart_upload) {
    cloud_storage_clients::object_key key{"multi/assembled"};

    // S3 requires parts >= 5 MiB (enforced by the client).
    constexpr size_t part_size = 5 * 1024 * 1024;
    auto mp_result = remote()
                       .initiate_multipart_upload(
                         bucket_name, key, part_size, 30s)
                       .get();
    ASSERT_FALSE(mp_result.has_error());
    auto mp = std::move(mp_result.value());

    auto make_part = [](char fill) {
        iobuf buf;
        ss::sstring data(part_size, fill);
        buf.append(data.data(), data.size());
        return buf;
    };

    mp->put(make_part('A')).get();
    mp->put(make_part('B')).get();
    mp->complete().get();
    // The multipart handle holds a client pool lease. The pool has one
    // client, so we must release it before download() can acquire one.
    mp = {};

    auto [res, content] = download("multi/assembled");
    ASSERT_EQ(res, download_result::success);
    EXPECT_EQ(content.size(), part_size * 2);
    EXPECT_EQ(content[0], 'A');
    EXPECT_EQ(content[part_size], 'B');
}

TEST_F(db_s3_imposter_test, multipart_abort) {
    cloud_storage_clients::object_key key{"aborted/obj"};

    constexpr size_t part_size = 5 * 1024 * 1024;
    auto mp_result = remote()
                       .initiate_multipart_upload(
                         bucket_name, key, part_size, 30s)
                       .get();
    ASSERT_FALSE(mp_result.has_error());
    auto mp = std::move(mp_result.value());

    iobuf part;
    ss::sstring data(part_size, 'X');
    part.append(data.data(), data.size());
    mp->put(std::move(part)).get();
    mp->abort().get();
    mp = {};

    EXPECT_EQ(head("aborted/obj"), download_result::notfound);
}
