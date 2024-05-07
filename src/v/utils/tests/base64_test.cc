// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/random.h"
#include "random/generators.h"
#include "utils/base64.h"

#include <boost/test/unit_test.hpp>
#include <cryptopp/base64.h>

BOOST_AUTO_TEST_CASE(bytes_type) {
    auto encdec = [](const bytes& input, const auto expected) {
        auto encoded = bytes_to_base64(input);
        BOOST_REQUIRE_EQUAL(encoded, expected);
        auto decoded = base64_to_bytes(encoded);
        BOOST_REQUIRE_EQUAL(decoded, input);
    };

    encdec("", "");
    encdec("this is a string", "dGhpcyBpcyBhIHN0cmluZw==");
    encdec("a", "YQ==");
}

BOOST_AUTO_TEST_CASE(iobuf_type) {
    auto encdec = [](const iobuf& input, const auto expected) {
        auto encoded = iobuf_to_base64(input);
        BOOST_REQUIRE_EQUAL(encoded, expected);
        auto decoded = base64_to_bytes(encoded);
        BOOST_REQUIRE_EQUAL(decoded, iobuf_to_bytes(input));
    };

    encdec(bytes_to_iobuf(""), "");
    encdec(bytes_to_iobuf("this is a string"), "dGhpcyBpcyBhIHN0cmluZw==");
    encdec(bytes_to_iobuf("a"), "YQ==");

    // test with multiple iobuf fragments
    iobuf buf;
    while (std::distance(buf.begin(), buf.end()) < 3) {
        auto data = random_generators::get_bytes(128);
        buf.append(data.data(), data.size());
    }

    auto encoded = iobuf_to_base64(buf);
    auto decoded = base64_to_bytes(encoded);
    BOOST_REQUIRE_EQUAL(decoded, iobuf_to_bytes(buf));
}

BOOST_AUTO_TEST_CASE(base64_url_decode_test_basic) {
    auto dec = [](std::string_view input, const bytes& expected) {
        auto decoded = base64url_to_bytes(input);
        BOOST_REQUIRE_EQUAL(decoded, expected);
    };

    dec("UmVkcGFuZGEgUm9ja3M", "Redpanda Rocks");
    // ChatGPT was asked to describe the Redpanda product
    dec(
      "UmVkcGFuZGEgaXMgYSBjdXR0aW5nLWVkZ2UgZGF0YSBzdHJlYW1pbmcgcGxhdGZvcm0gZGVz"
      "aWduZWQgdG8gb2ZmZXIgYSBoaWdoLXBlcmZvcm1hbmNlIGFsdGVybmF0aXZlIHRvIEFwYWNo"
      "ZSBLYWZrYS4gSXQncyBjcmFmdGVkIHRvIGhhbmRsZSB2YXN0IGFtb3VudHMgb2YgcmVhbC10"
      "aW1lIGRhdGEgZWZmaWNpZW50bHksIG1ha2luZyBpdCBhbiBleGNlbGxlbnQgY2hvaWNlIGZv"
      "ciBtb2Rlcm4gZGF0YS1kcml2ZW4gYXBwbGljYXRpb25zLiAgT3ZlcmFsbCwgUmVkcGFuZGEg"
      "cmVwcmVzZW50cyBhIGNvbXBlbGxpbmcgb3B0aW9uIGZvciBvcmdhbml6YXRpb25zIHNlZWtp"
      "bmcgYSBoaWdoLXBlcmZvcm1hbmNlLCBzY2FsYWJsZSwgYW5kIHJlbGlhYmxlIGRhdGEgc3Ry"
      "ZWFtaW5nIHNvbHV0aW9uLiBXaGV0aGVyIHlvdSdyZSBidWlsZGluZyByZWFsLXRpbWUgYW5h"
      "bHl0aWNzIGFwcGxpY2F0aW9ucywgcHJvY2Vzc2luZyBJb1QgZGF0YSBzdHJlYW1zLCBvciBt"
      "YW5hZ2luZyBldmVudC1kcml2ZW4gbWljcm9zZXJ2aWNlcywgUmVkcGFuZGEgaGFzIHlvdSBj"
      "b3ZlcmVkLg",
      "Redpanda is a cutting-edge data streaming platform designed to offer a "
      "high-performance alternative to Apache Kafka. It's crafted to handle "
      "vast amounts of real-time data efficiently, making it an excellent "
      "choice for modern data-driven applications.  Overall, Redpanda "
      "represents a compelling option for organizations seeking a "
      "high-performance, scalable, and reliable data streaming solution. "
      "Whether you're building real-time analytics applications, processing "
      "IoT data streams, or managing event-driven microservices, Redpanda has "
      "you covered.");

    dec("", "");
    dec("YQ", "a");
    dec("YWI", "ab");
    dec("YWJj", "abc");
    dec("A", "");
}

BOOST_AUTO_TEST_CASE(base64_url_decode_test_random) {
    const std::array<size_t, 5> test_sizes = {1, 10, 128, 256, 512};
    auto dec = [](std::string_view input, const bytes& expected) {
        auto decoded = base64url_to_bytes(input);
        BOOST_REQUIRE_EQUAL(decoded, expected);
    };

    auto enc = [](const bytes& msg) {
        CryptoPP::Base64URLEncoder encoder;
        encoder.Put(msg.data(), msg.size());
        encoder.MessageEnd();
        auto size = encoder.MaxRetrievable();
        BOOST_REQUIRE_NE(size, 0);
        ss::sstring encoded(ss::sstring::initialized_later{}, size);
        encoder.Get(
          reinterpret_cast<CryptoPP::byte*>(encoded.data()), encoded.size());
        return encoded;
    };

    for (auto s : test_sizes) {
        auto val = random_generators::get_bytes(s);
        auto encoded = enc(val);
        dec(encoded, val);
    }
}

BOOST_AUTO_TEST_CASE(base64_url_decode_invalid_character) {
    const std::string invalid_encode = "abc+/";
    BOOST_REQUIRE_THROW(
      base64url_to_bytes(invalid_encode), base64_url_decoder_exception);
}
