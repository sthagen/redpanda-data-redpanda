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

#include "strings/utf8.h"

#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

#include <string>
#include <string_view>

// Build an iobuf with two distinct fragments to exercise state-machine
// boundary handling in is_valid_utf8.
static iobuf make_split_iobuf(std::string_view a, std::string_view b) {
    iobuf buf;
    buf.append(ss::temporary_buffer<char>(a.data(), a.size()));
    buf.append(ss::temporary_buffer<char>(b.data(), b.size()));
    return buf;
}

static std::string iobuf_to_string(iobuf buf) {
    std::string s;
    s.reserve(buf.size_bytes());
    for (const auto& frag : buf) {
        s.append(frag.get(), frag.size());
    }
    return s;
}

// ---------------------------------------------------------------------------
// Valid inputs — fast path, zero copy
// ---------------------------------------------------------------------------

TEST(Utf8Sanitize, ValidEmpty) {
    auto r = utf8_sanitize(iobuf::from(""));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(iobuf_to_string(std::move(*r)), "");
}

TEST(Utf8Sanitize, ValidAscii) {
    auto r = utf8_sanitize(iobuf::from("hello world"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(iobuf_to_string(std::move(*r)), "hello world");
}

TEST(Utf8Sanitize, Valid2Byte) {
    // U+00E9 LATIN SMALL LETTER E WITH ACUTE: C3 A9
    auto r = utf8_sanitize(iobuf::from("\xC3\xA9"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(iobuf_to_string(std::move(*r)), "\xC3\xA9");
}

TEST(Utf8Sanitize, ValidFragmentSplitMultiByte) {
    // C3 | A9 — pending=1 carries into second fragment
    auto r = utf8_sanitize(make_split_iobuf("\xC3", "\xA9"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(iobuf_to_string(std::move(*r)), "\xC3\xA9");
}

// ---------------------------------------------------------------------------
// Invalid inputs — slow path replacement (advance-1-on-error)
// ---------------------------------------------------------------------------

TEST(Utf8Sanitize, InvalidBareContinuation) {
    // 0x80 alone — one U+FFFD
    auto r = utf8_sanitize(iobuf::from("\x80"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(iobuf_to_string(std::move(*r)), "\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidTruncated3Byte) {
    // E4 B8 at end of string — two U+FFFD (one per ill-formed byte)
    auto r = utf8_sanitize(iobuf::from("\xE4\xB8"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(iobuf_to_string(std::move(*r)), "\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidTruncated4Byte) {
    // F0 9F 98 at end of string — three U+FFFD (one per ill-formed byte)
    auto r = utf8_sanitize(iobuf::from("\xF0\x9F\x98"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(
      iobuf_to_string(std::move(*r)), "\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidSurrogate) {
    // U+D800 encoded as UTF-8: ED A0 80 — three U+FFFD.
    // utf8proc rejects the lead byte ED A0 as a surrogate; each subsequent
    // continuation byte is then a bare continuation.
    auto r = utf8_sanitize(iobuf::from("\xED\xA0\x80"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(
      iobuf_to_string(std::move(*r)), "\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidMixed) {
    // Valid prefix + bare continuation + valid suffix
    auto r = utf8_sanitize(iobuf::from("hello\x80world"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(iobuf_to_string(std::move(*r)), "hello\xEF\xBF\xBDworld");
}

TEST(Utf8Sanitize, InvalidContinuationMidSequence) {
    // C3 expects one continuation, but gets another lead byte — two U+FFFD.
    auto r = utf8_sanitize(iobuf::from("\xC3\xC3"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(iobuf_to_string(std::move(*r)), "\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidOverlong2Byte) {
    // C0 80 — overlong encoding of U+0000, two U+FFFD.
    auto r = utf8_sanitize(iobuf::from("\xC0\x80"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(iobuf_to_string(std::move(*r)), "\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidOverlong3Byte) {
    // E0 9F 80 — overlong (first continuation 0x9F < 0xA0), three U+FFFD.
    auto r = utf8_sanitize(iobuf::from("\xE0\x9F\x80"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(
      iobuf_to_string(std::move(*r)), "\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidOutOfRangeLead) {
    // 0xF5 and above are not valid UTF-8 lead bytes (would exceed U+10FFFF).
    auto r = utf8_sanitize(iobuf::from("\xF5"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(iobuf_to_string(std::move(*r)), "\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidSurrogateFragmentSplit) {
    // ED | A0 80 — surrogate split across fragment boundary; verifies that
    // max_cont=0x9F carries over from the first fragment.
    auto r = utf8_sanitize(make_split_iobuf("\xED", "\xA0\x80"));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(
      iobuf_to_string(std::move(*r)), "\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD");
}

// ---------------------------------------------------------------------------
// Size limit checks
// ---------------------------------------------------------------------------

TEST(Utf8Sanitize, OversizedValid) {
    // A valid string larger than max_bytes passes through unchanged (fast
    // path).
    std::string big(iobuf::max_linearize_size + 1, 'a');
    auto r = utf8_sanitize(iobuf::from(big));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->size_bytes(), big.size());
}

TEST(Utf8Sanitize, OversizedInvalid) {
    // An invalid string larger than max_bytes → input_too_large.
    std::string big(iobuf::max_linearize_size + 1, '\x80');
    auto r = utf8_sanitize(iobuf::from(big));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), utf8_sanitize_error::input_too_large);
}

TEST(Utf8Sanitize, ExactlyAtLimitInvalid) {
    // size_bytes() == max_linearize_size: the slow path should proceed (the
    // check is strictly >, not >=).
    std::string at_limit(iobuf::max_linearize_size, '\x80');
    auto r = utf8_sanitize(iobuf::from(at_limit));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(
      r->size_bytes(), at_limit.size() * 3); // each 0x80 → 3-byte U+FFFD
}
