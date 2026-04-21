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

#include "cloud_io/tests/db_s3_imposter_fixture.h"

#include "absl/container/btree_map.h"
#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "cloud_storage_clients/configuration.h"
#include "http/utils.h"
#include "lsm/io/disk_persistence.h"
#include "lsm/lsm.h"
#include "utils/unresolved_address.h"

#include <seastar/net/socket_defs.hh>
#include <seastar/util/log.hh>

#include <charconv>
#include <filesystem>
#include <set>

static ss::logger dbfixt_log("db_s3_imposter"); // NOLINT

namespace {

constexpr uint16_t httpd_port = 4442;
constexpr const char* httpd_host = "localhost";
constexpr const char* httpd_ip = "127.0.0.1";
constexpr size_t default_max_keys = 1000;

constexpr auto error_payload = R"xml(<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>NoSuchKey</Code>
    <Message>Object not found</Message>
    <Resource>resource</Resource>
    <RequestId>requestid</RequestId>
</Error>)xml";

uint64_t string_view_to_ul(std::string_view sv) {
    uint64_t result{};
    auto conv_res = std::from_chars(sv.data(), sv.data() + sv.size(), result);
    vassert(
      conv_res.ec != std::errc::invalid_argument,
      "failed to convert {} to an unsigned long",
      sv);
    return result;
}

ss::future<> write_iobuf_to_stream(iobuf buf, ss::output_stream<char> out) {
    for (const auto& frag : buf) {
        co_await out.write(frag.get(), frag.size());
    }
    co_await out.flush();
    co_await out.close();
}

} // namespace

// Async HTTP handler backed by the LSM database. Holds references to
// fixture members — the fixture must call stop() (which drains
// in-flight requests) before destruction.
struct db_s3_imposter_fixture::handler : ss::httpd::handler_base {
    lsm::database& db;
    lsm::sequence_number& seqno;
    const cloud_storage_clients::bucket_name& bucket;

    handler(
      lsm::database& db,
      lsm::sequence_number& seqno,
      const cloud_storage_clients::bucket_name& bucket)
      : db(db)
      , seqno(seqno)
      , bucket(bucket) {}

    ss::future<std::unique_ptr<ss::http::reply>> handle(
      const ss::sstring&,
      std::unique_ptr<ss::http::request> req,
      std::unique_ptr<ss::http::reply> rep) override {
        // Normalize _url to a bare object key: strip query params
        // and validate/remove the bucket prefix. Query params are
        // still accessible via get_query_param().
        auto qpos = req->_url.find('?');
        if (qpos != ss::sstring::npos) {
            req->_url = req->_url.substr(0, qpos);
        }
        auto expected_prefix = fmt::format("/{}/", bucket());
        vassert(
          req->_url.starts_with(expected_prefix),
          "request URL {} does not start with bucket prefix {}",
          req->_url,
          expected_prefix);
        req->_url = req->_url.substr(expected_prefix.size());

        vlog(
          dbfixt_log.trace,
          "request {} {} content_length={}",
          req->_method,
          req->_url,
          req->content_length);

        if (req->_method == "GET") {
            // S3 ListObjectsV2 is a GET with ?list-type=2.
            if (req->get_query_param("list-type") == "2") {
                co_await handle_list(*req, *rep);
            } else {
                co_await handle_get(*req, *rep);
            }
        } else if (req->_method == "PUT") {
            co_await handle_put(*req, *rep);
        } else if (req->_method == "DELETE") {
            co_await handle_delete(*req, *rep);
        } else if (req->_method == "HEAD") {
            co_await handle_head(*req, *rep);
        } else if (req->_method == "POST") {
            co_await handle_post(*req, *rep);
        } else {
            rep->set_status(ss::http::reply::status_type::bad_request);
        }

        rep->done("xml");
        co_return std::move(rep);
    }

    ss::future<>
    handle_get(const ss::http::request& req, ss::http::reply& repl) {
        auto result = co_await db.get(req._url);
        if (!result.has_value()) {
            repl.set_status(ss::http::reply::status_type::not_found);
            repl._content = error_payload;
            co_return;
        }

        // Range: bytes=START-END (inclusive on both ends)
        auto range_header = req.get_header("Range");
        if (!range_header.empty()) {
            auto eq_pos = range_header.find('=');
            auto dash_pos = range_header.find('-', eq_pos + 1);
            if (
              eq_pos == ss::sstring::npos || dash_pos == ss::sstring::npos
              || dash_pos + 1 >= range_header.size()) {
                repl.set_status(ss::http::reply::status_type::bad_request);
                co_return;
            }
            auto start = string_view_to_ul(
              std::string_view(range_header)
                .substr(eq_pos + 1, dash_pos - eq_pos - 1));
            auto end = string_view_to_ul(
              std::string_view(range_header).substr(dash_pos + 1));
            end = std::min(end, result->size_bytes() - 1);
            result->trim_front(start);
            result->trim_back(result->size_bytes() - (end - start + 1));
        }

        // Stream the body from the iobuf fragments rather than
        // linearizing to sstring (which has a 128KiB size limit).
        repl.write_body(
          "xml",
          ss::http::body_writer_type([result = std::move(*result)](
                                       ss::output_stream<char>&& out) mutable {
              return write_iobuf_to_stream(std::move(result), std::move(out));
          }));
    }

    ss::future<>
    handle_put(const ss::http::request& req, ss::http::reply& repl) {
        vlog(dbfixt_log.trace, "PUT {}", req._url);
        auto batch = db.create_write_batch();

        auto upload_id = req.get_query_param("uploadId");
        auto part_num = req.get_query_param("partNumber");
        if (!upload_id.empty() && !part_num.empty()) {
            // S3 UploadPart: store under a temporary key that sorts by
            // part number so CompleteMultipartUpload can iterate in order.
            auto part_key = fmt::format(
              "__multipart/{}/{}/{:0>6}", req._url, upload_id, part_num);
            batch.put(part_key, iobuf::from(req.content), ++seqno);
            co_await db.apply(std::move(batch));
            repl.add_header("ETag", fmt::format("\"etag-part-{}\"", part_num));
            co_return;
        }

        batch.put(req._url, iobuf::from(req.content), ++seqno);
        co_await db.apply(std::move(batch));
    }

    ss::future<>
    handle_delete(const ss::http::request& req, ss::http::reply& repl) {
        vlog(dbfixt_log.trace, "DELETE {}", req._url);
        auto batch = db.create_write_batch();

        // S3 AbortMultipartUpload: DELETE with ?uploadId removes
        // the temporary part keys.
        auto upload_id = req.get_query_param("uploadId");
        if (!upload_id.empty()) {
            auto part_prefix = fmt::format(
              "__multipart/{}/{}/", req._url, upload_id);
            auto iter = co_await db.create_iterator();
            co_await iter.seek(part_prefix);
            while (iter.valid()) {
                auto key = ss::sstring(iter.key());
                if (!key.starts_with(part_prefix)) {
                    break;
                }
                batch.remove(key, ++seqno);
                co_await iter.next();
            }
        } else {
            batch.remove(req._url, ++seqno);
        }

        co_await db.apply(std::move(batch));
        repl.set_status(ss::http::reply::status_type::no_content);
    }

    ss::future<>
    handle_head(const ss::http::request& req, ss::http::reply& repl) {
        auto result = co_await db.get(req._url);
        if (!result.has_value()) {
            repl.add_header("x-amz-request-id", "placeholder-id");
            repl.set_status(ss::http::reply::status_type::not_found);
            co_return;
        }

        repl.add_header("ETag", "placeholder-etag");
        repl.set_status(ss::http::reply::status_type::ok);
    }

    ss::future<>
    handle_post(const ss::http::request& req, ss::http::reply& repl) {
        // S3 DeleteObjects: POST /<bucket>/?delete with XML body
        // listing <Key> elements to remove.
        if (req.has_query_param("delete")) {
            vlog(dbfixt_log.trace, "batch DELETE via POST {}", req._url);
            auto body = std::string_view(req.content);
            auto batch = db.create_write_batch();

            auto pos = body.find("<Key>");
            while (pos != std::string_view::npos) {
                pos += 5; // strlen("<Key>")
                auto end_pos = body.find("</Key>", pos);
                if (end_pos == std::string_view::npos) {
                    break;
                }
                auto key = ss::sstring(body.substr(pos, end_pos - pos));
                vlog(dbfixt_log.trace, "batch DELETE key {}", key);
                batch.remove(key, ++seqno);
                pos = body.find("<Key>", end_pos);
            }

            co_await db.apply(std::move(batch));
            repl._content
              = R"xml(<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>)xml";
            co_return;
        }

        if (req.has_query_param("uploads")) {
            // CreateMultipartUpload
            auto upload_id = fmt::format("upload-{}", ++seqno);
            vlog(
              dbfixt_log.trace,
              "CreateMultipartUpload {} -> {}",
              req._url,
              upload_id);
            repl.set_status(ss::http::reply::status_type::ok);
            repl._content = fmt::format(
              R"xml(<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <UploadId>{}</UploadId>
</InitiateMultipartUploadResult>)xml",
              bucket(),
              req._url,
              upload_id);
            co_return;
        }

        if (req.has_query_param("uploadId")) {
            // CompleteMultipartUpload: read parts, concatenate, store
            auto upload_id = req.get_query_param("uploadId");
            vlog(
              dbfixt_log.trace,
              "CompleteMultipartUpload {} upload_id={}",
              req._url,
              upload_id);

            auto part_prefix = fmt::format(
              "__multipart/{}/{}/", req._url, upload_id);

            auto iter = co_await db.create_iterator();
            co_await iter.seek(part_prefix);

            iobuf combined;
            auto batch = db.create_write_batch();
            while (iter.valid()) {
                auto key = ss::sstring(iter.key());
                if (!key.starts_with(part_prefix)) {
                    break;
                }
                combined.append(iter.value());
                batch.remove(key, ++seqno);
                co_await iter.next();
            }

            batch.put(req._url, std::move(combined), ++seqno);
            co_await db.apply(std::move(batch));

            repl.set_status(ss::http::reply::status_type::ok);
            repl._content = fmt::format(
              R"xml(<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult>
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <ETag>"combined-etag"</ETag>
</CompleteMultipartUploadResult>)xml",
              bucket(),
              req._url);
            co_return;
        }

        repl.set_status(ss::http::reply::status_type::bad_request);
    }

    ss::future<>
    handle_list(const ss::http::request& req, ss::http::reply& repl) {
        auto prefix = http::uri_decode(req.get_query_param("prefix"));
        auto delimiter = http::uri_decode(req.get_query_param("delimiter"));
        auto max_keys_str = req.get_query_param("max-keys");
        auto continuation_token = http::uri_decode(
          req.get_query_param("continuation-token"));

        size_t max_keys = default_max_keys;
        if (!max_keys_str.empty()) {
            max_keys = std::min(
              static_cast<size_t>(std::stoi(max_keys_str)), default_max_keys);
        }

        // Seek directly to the prefix range to avoid scanning
        // unrelated keys. Continuation tokens are already bare
        // keys from a prior response, so they work the same way.
        auto seek_target = continuation_token.empty()
                             ? ss::sstring(prefix)
                             : ss::sstring(continuation_token);

        auto iter = co_await db.create_iterator();
        co_await iter.seek(seek_target);

        absl::btree_map<ss::sstring, size_t> content_key_to_size;
        std::set<ss::sstring> common_prefixes;
        ss::sstring next_continuation_token;

        auto matches_prefix = [&prefix](const ss::sstring& key) {
            return key.size() >= prefix.size()
                   && key.compare(0, prefix.size(), prefix) == 0;
        };

        // If a delimiter is set, classify a key as either a leaf object
        // or a common prefix ("directory"). E.g. with delimiter='/' and
        // prefix="dir/":
        //   "dir/sub/file" → common prefix "dir/sub/"
        //   "dir/file"     → nullopt (leaf)
        auto common_prefix_for =
          [&prefix,
           &delimiter](const ss::sstring& key) -> std::optional<ss::sstring> {
            if (delimiter.empty()) {
                return std::nullopt;
            }
            // Find the first instance of the delimiter, if any, after the
            // prefix. Common prefixes are only those that go up to that next
            // delimiter.
            auto pos = key.find(delimiter, prefix.size());
            if (pos == ss::sstring::npos) {
                return std::nullopt;
            }
            return ss::sstring(
              prefix + key.substr(prefix.size(), pos - prefix.size() + 1));
        };

        while (iter.valid()) {
            auto key = ss::sstring(iter.key());

            // Skip internal keys used for in-flight multipart uploads.
            if (key.starts_with("__multipart/")) {
                co_await iter.next();
                continue;
            }

            if (
              content_key_to_size.size() + common_prefixes.size() >= max_keys) {
                next_continuation_token = key;
                break;
            }

            if (!matches_prefix(key)) {
                co_await iter.next();
                continue;
            }

            if (auto cp = common_prefix_for(key)) {
                common_prefixes.emplace(std::move(*cp));
            } else {
                content_key_to_size.emplace(key, iter.value().size_bytes());
            }

            co_await iter.next();
        }

        bool is_truncated = !next_continuation_token.empty();

        ss::sstring ret;
        ret += fmt::format(
          R"xml(
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix>{}</Prefix>
  <KeyCount>{}</KeyCount>
  <MaxKeys>{}</MaxKeys>
  <Delimiter>{}</Delimiter>
  <IsTruncated>{}</IsTruncated>
  <NextContinuationToken>{}</NextContinuationToken>
)xml",
          prefix,
          content_key_to_size.size(),
          max_keys,
          delimiter,
          is_truncated,
          next_continuation_token);

        for (const auto& [key, size] : content_key_to_size) {
            ret += fmt::format(
              R"xml(
  <Contents>
    <Key>{}</Key>
    <LastModified>2021-01-10T01:00:00.000Z</LastModified>
    <ETag>"test-etag-1"</ETag>
    <Size>{}</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
)xml",
              key,
              size);
        }

        ret += "<CommonPrefixes>\n";
        for (const auto& pfx : common_prefixes) {
            ret += fmt::format("<Prefix>{}</Prefix>\n", pfx);
        }
        ret += "</CommonPrefixes>\n";
        ret += "</ListBucketResult>\n";
        repl._content = std::move(ret);
    }
};

// Forwards requests to shard 0 where the LSM database lives.
struct db_s3_imposter_fixture::forwarding_handler : ss::httpd::handler_base {
    handler& shard0_handler;

    explicit forwarding_handler(handler& h)
      : shard0_handler(h) {}

    ss::future<std::unique_ptr<ss::http::reply>> handle(
      const ss::sstring& path,
      std::unique_ptr<ss::http::request> req,
      std::unique_ptr<ss::http::reply> rep) override {
        return ss::smp::submit_to(
          0,
          [this, path, req = std::move(req), rep = std::move(rep)]() mutable {
              return shard0_handler.handle(
                path, std::move(req), std::move(rep));
          });
    }
};

db_s3_imposter_fixture::db_s3_imposter_fixture() {
    _data_dir = std::filesystem::temp_directory_path()
                / fmt::format("db_s3_imposter_{}", getpid());
    // Remove stale directory from a previous crashed run
    std::filesystem::remove_all(_data_dir);
    std::filesystem::create_directories(_data_dir);

    net::unresolved_address server_addr(httpd_host, httpd_port);
    conf.uri = cloud_storage_clients::access_point_uri(httpd_host);
    conf.access_key = cloud_roles::public_key_str("access-key");
    conf.secret_key = cloud_roles::private_key_str("secret-key");
    conf.region = cloud_roles::aws_region_name("us-east-1");
    conf.service = cloud_roles::aws_service_name("s3");
    conf.url_style = cloud_storage_clients::s3_url_style::path;
    conf.server_addr = server_addr;
}

db_s3_imposter_fixture::~db_s3_imposter_fixture() noexcept {
    try {
        stop().get();
    } catch (...) {
    }
    std::error_code ec;
    std::filesystem::remove_all(_data_dir, ec);
}

ss::future<> db_s3_imposter_fixture::start() {
    auto data_path = _data_dir / "data";
    auto meta_path = _data_dir / "meta";
    std::filesystem::create_directories(data_path);
    std::filesystem::create_directories(meta_path);

    auto data_p = co_await lsm::io::open_disk_data_persistence(data_path);
    auto meta_p = co_await lsm::io::open_disk_metadata_persistence(meta_path);
    lsm::io::persistence persistence{
      .data = std::move(data_p),
      .metadata = std::move(meta_p),
    };
    _db = co_await lsm::database::open(lsm::options{}, std::move(persistence));

    _server = ss::make_shared<ss::httpd::http_server_control>();
    co_await _server->start();

    // The real handler runs on shard 0; other shards forward to it.
    auto real = std::make_unique<handler>(*_db, _seqno, bucket_name);
    auto* real_ptr = real.get();
    _handlers.push_back(std::move(real));

    std::vector<forwarding_handler*> fwd_ptrs;
    for (unsigned i = 1; i < ss::smp::count; ++i) {
        auto fwd = std::make_unique<forwarding_handler>(*real_ptr);
        fwd_ptrs.push_back(fwd.get());
        _handlers.push_back(std::move(fwd));
    }

    co_await _server->set_routes([real_ptr, &fwd_ptrs](ss::httpd::routes& r) {
        auto shard = ss::this_shard_id();
        if (shard == 0) {
            r.add_default_handler(real_ptr);
        } else {
            r.add_default_handler(fwd_ptrs[shard - 1]);
        }
    });

    ss::ipv4_addr ip_addr = {httpd_ip, httpd_port};
    co_await _server->listen(ss::socket_address(ip_addr));
}

ss::future<> db_s3_imposter_fixture::stop() {
    if (_server) {
        co_await _server->stop();
        _server = {};
    }
    if (_db) {
        co_await _db->close();
        _db.reset();
    }
}
