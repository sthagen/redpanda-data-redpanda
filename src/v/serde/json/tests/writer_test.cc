/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "absl/cleanup/cleanup.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "random/generators.h"
#include "serde/json/writer.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/reader.h>
#include <rapidjson/writer.h>

#include <cstdlib>
#include <numbers>
#include <source_location>

namespace {

std::string to_string(serde::json::writer&& w) {
    return std::move(w).finish().linearize_to_string();
}

class JsonWriterTest : public ::testing::Test {
protected:
    std::string
    serialize_value(const std::function<void(serde::json::writer&)>& fn) {
        serde::json::writer w;
        fn(w);
        return to_string(std::move(w));
    }
    std::string
    serialize_object(const std::function<void(serde::json::writer&)>& fn) {
        return serialize_value([fn](serde::json::writer& w) {
            w.begin_object();
            fn(w);
            w.end_object();
        });
    }
};

TEST_F(JsonWriterTest, SingleValue) {
    auto result = serialize_value(
      [](serde::json::writer& w) { w.string("testing"); });
    EXPECT_EQ(result, R"("testing")");
}

TEST_F(JsonWriterTest, EscapedBackslashAndQuotes) {
    auto result = serialize_object([](serde::json::writer& w) {
        w.key("key");
        w.string(R"(value with "escaped quote" and backslash\)");
    });
    EXPECT_EQ(
      result, R"({"key":"value with \"escaped quote\" and backslash\\"})");
}

TEST_F(JsonWriterTest, UnicodeEscapeSequences) {
    auto result = serialize_object([](serde::json::writer& w) {
        w.key("key");
        w.string("\u2028\u2029\xF0\x9D\x84\x9E"); // UTF-8 for U+1D11E
    });
    EXPECT_EQ(result, R"({"key":"\u2028\u2029\ud834\udd1e"})");
    result = serialize_object([](serde::json::writer& w) {
        w.key("emoji");
        w.string("\u2028\u2029\xF0\x9D\x84\x9E"); // UTF-8 for U+1D11E
    });
}

TEST_F(JsonWriterTest, Utf8CharacterWidths) {
    auto result = serialize_object([](serde::json::writer& w) {
        w.key("utf8");
        // 1-byte: A, 2-byte: ©, 3-byte: €, 4-byte: 😀
        w.string("A\xC2\xA9\xE2\x82\xAC\xF0\x9F\x98\x80");
    });
    EXPECT_EQ(result, R"({"utf8":"A\u00a9\u20ac\ud83d\ude00"})");
}

TEST_F(JsonWriterTest, InvalidUtf8) {
    auto result = serialize_object([](serde::json::writer& w) {
        w.key("utf8");
        // Should be an invalid utf8 character, so we get the replacement char
        w.string("My face was: \xF0\x9F\x98 - can't you see the smile?");
    });
    EXPECT_EQ(
      result, R"({"utf8":"My face was: \ufffd- can't you see the smile?"})");
}

TEST_F(JsonWriterTest, EmptyKey) {
    auto result = serialize_object([](serde::json::writer& w) {
        w.key("");
        w.string("empty key");
    });
    EXPECT_EQ(result, "{\"\":\"empty key\"}");
}

TEST_F(JsonWriterTest, KeyWithEscapedChars) {
    auto result = serialize_object([](serde::json::writer& w) {
        w.key("ke\ny");
        w.string(std::string_view{"val\0ue", 6});
    });
    EXPECT_EQ(result, R"({"ke\ny":"val\u0000ue"})");
}

TEST_F(JsonWriterTest, DeeplyNestedObject) {
    std::string expected = "{";
    for (char c = 'a'; c <= 'g'; ++c) {
        expected += std::string("\"") + c + "\":{";
    }
    expected.pop_back();
    expected += R"("too deep?")";
    for (int i = 0; i <= ('g' - 'a'); ++i) {
        expected += "}";
    }

    auto result = serialize_object([](serde::json::writer& w) {
        w.key("a");
        w.begin_object();
        auto o1 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("b");
        w.begin_object();
        auto o2 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("c");
        w.begin_object();
        auto o3 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("d");
        w.begin_object();
        auto o4 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("e");
        w.begin_object();
        auto o5 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("f");
        w.begin_object();
        auto o6 = absl::MakeCleanup([&w] { w.end_object(); });
        w.key("g");
        w.string("too deep?");
    });
    EXPECT_EQ(result, expected);
}

TEST_F(JsonWriterTest, Primatives) {
    auto result = serialize_object([](serde::json::writer& w) {
        w.key("pi");
        w.number(std::numbers::pi);
        w.key("null");
        w.null();
        w.key("boolean");
        w.boolean(true);
        w.key("integer");
        w.number(42);
        w.key("array");
        w.begin_array();
        w.number(std::numbers::e);
        w.boolean(false);
        w.null();
        w.number(-42);
        w.end_array();
    });
    EXPECT_EQ(
      result,
      R"({"pi":3.14159,"null":null,"boolean":true,"integer":42,"array":[2.71828,false,null,-42]})");
}

TEST_F(JsonWriterTest, Base64) {
    auto result = serialize_value(
      [](serde::json::writer& w) { w.base64_string(iobuf::from("testing")); });
    EXPECT_EQ(result, R"("dGVzdGluZw==")");
}

TEST_F(JsonWriterTest, EmptyArrayAndObject) {
    auto result = serialize_object([](serde::json::writer& w) {
        w.key("emptyArr");
        w.begin_array();
        w.end_array();
        w.key("emptyObj");
        w.begin_object();
        w.end_object();
    });
    EXPECT_EQ(result, R"({"emptyArr":[],"emptyObj":{}})");
}

std::string generate_random_utf8(size_t approx_length) {
    // Collection of interesting UTF-8 substrings, otherwise
    // trying to randomly generate utf8 is too complicated
    static const std::vector<std::string> utf8_parts = {
      // ASCII
      "Hello",
      "World",
      "Test",
      "Code",
      "Data",

      // Latin with diacritics
      "café",
      "naïve",
      "résumé",
      "Åse",
      "Björk",

      // Greek letters
      "α",
      "β",
      "ω",
      "Α",
      "Β",
      "Σ",
      "Ω",

      // Cyrillic
      "привет",
      "мир",
      "Санкт",
      "Петербург",

      // Chinese characters
      "你好",
      "中国",
      "汉字",

      // Japanese Hiragana
      "こんにちは",
      "ひらがな",
      "かたかな",

      // Japanese Katakana
      "コンニチハ",
      "セカイ",
      "コード",
      "カタカナ",
      "データ",

      // Korean
      "안녕",
      "세계",
      "한국어",
      "서울",

      // Arabic (RTL text)
      "مرحبا",
      "عالم",
      "بيانات",

      // Emojis and symbols
      "😀",
      "😂",
      "🔥",
      "💻",
      "🚀",
      "⚡",

      // Mathematical symbols
      "∑",
      "∏",
      "∫",
      "∪",
      "∩",
    };

    std::string result;
    // Keep adding random parts until we reach approximate desired length
    while (result.length() < approx_length) {
        // Add a random UTF-8 substring
        result += random_generators::random_choice(utf8_parts);
    }
    return result;
}

rapidjson::Value generate_random_json(
  rapidjson::Value::AllocatorType& a, int max_depth, int current_depth) {
    auto generate_str = [&a](size_t size) {
        auto str = generate_random_utf8(size);
        rapidjson::Value v(rapidjson::kStringType);
        v.SetString(str.data(), str.size(), a);
        return v;
    };

    switch (random_generators::get_int(0, current_depth < max_depth ? 5 : 3)) {
    case 0:
        return {};
    case 1:
        return rapidjson::Value(bool(random_generators::get_int(0, 1)));
    case 2: {
        auto v = random_generators::get_real<double>();
        // We only support the precision that absl supports, so truncate to that
        // precision immediately.
        vassert(absl::SimpleAtod(absl::StrCat(v), &v), "impossible");
        return rapidjson::Value(v);
    }
    case 3: {
        return generate_str(random_generators::get_int(4, 1024));
    }
    case 4: {
        if (++current_depth > max_depth) {
            break;
        }
        rapidjson::Value v(rapidjson::kObjectType);
        size_t count = random_generators::get_int(0, 5);
        std::set<std::string> keys;
        for (size_t i = 0; i < count; ++i) {
            auto key = generate_random_utf8(random_generators::get_int(2, 16));
            while (keys.contains(key)) {
                key = generate_random_utf8(random_generators::get_int(2, 16));
            }
            keys.insert(key);
            rapidjson::Value name(rapidjson::kStringType);
            name.SetString(key.data(), key.size(), a);
            v.AddMember(
              name, generate_random_json(a, max_depth, current_depth), a);
        }
        return v;
    }
    case 5: {
        if (++current_depth > max_depth) {
            break;
        }
        rapidjson::Value v(rapidjson::kArrayType);
        size_t count = random_generators::get_int(0, 5);
        for (size_t i = 0; i < count; ++i) {
            v.PushBack(generate_random_json(a, max_depth, current_depth), a);
        }
        return v;
    }
    default:
        // fallthrough
    }
    // Otherwise return `null`
    return {};
}

void serialize_rapidjson(const rapidjson::Value& v, serde::json::writer& w) {
    switch (v.GetType()) {
    case rapidjson::kNullType:
        w.null();
        break;
    case rapidjson::kFalseType:
        w.boolean(false);
        break;
    case rapidjson::kTrueType:
        w.boolean(true);
        break;
    case rapidjson::kObjectType: {
        w.begin_object();
        for (const auto& member : v.GetObject()) {
            w.key({member.name.GetString(), member.name.GetStringLength()});
            serialize_rapidjson(member.value, w);
        }
        w.end_object();
        break;
    }
    case rapidjson::kArrayType: {
        w.begin_array();
        for (const auto& elem : v.GetArray()) {
            serialize_rapidjson(elem, w);
        }
        w.end_array();
        break;
    }
    case rapidjson::kStringType:
        w.string({v.GetString(), v.GetStringLength()});
        break;
    case rapidjson::kNumberType:
        w.number(v.GetDouble());
        break;
    }
}

std::string rapidjson_to_string(const rapidjson::Value& v) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    v.Accept(writer);
    writer.Flush();
    return buffer.GetString();
}

TEST(RandomizedJsonWriterTest, RapidJSONRoundtrip) {
    for (int i = 0; i < 1024; ++i) {
        rapidjson::Document doc;
        auto value = generate_random_json(doc.GetAllocator(), 4, 0);
        serde::json::writer w;
        serialize_rapidjson(value, w);
        rapidjson::Document got;
        EXPECT_NO_THROW(got.Parse(to_string(std::move(w))));
        EXPECT_EQ(value, got)
          << rapidjson_to_string(value) << " vs " << rapidjson_to_string(got);
    }
}

} // namespace
