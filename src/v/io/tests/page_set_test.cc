/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "io/page_set.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

namespace io = experimental::io;

namespace {
auto make_page(uint64_t offset, uint64_t size) {
    return seastar::make_lw_shared<io::page>(
      offset, seastar::temporary_buffer<char>(size));
}
} // namespace

TEST(PageSet, BeginEndEqualWhenEmpty) {
    const io::page_set set;
    EXPECT_EQ(set.begin(), set.end());
}

TEST(PageSet, FindReturnsEndWhenEmpty) {
    const io::page_set set;
    EXPECT_EQ(set.find(0), set.end());
}

TEST(PageSet, Size) {
    io::page_set set;
    EXPECT_EQ(set.size(), 0);
    (void)set.insert(make_page(0, 10));
    EXPECT_EQ(set.size(), 1);
    (void)set.insert(make_page(10, 10));
    EXPECT_EQ(set.size(), 2);
    set.erase(set.begin());
    EXPECT_EQ(set.size(), 1);
    set.erase(set.begin());
    EXPECT_EQ(set.size(), 0);
}

TEST(PageSet, InsertOne) {
    io::page_set set;

    const auto page = make_page(0, 10);
    const auto res = set.insert(page);
    EXPECT_TRUE(res.second);
    EXPECT_EQ(res.first, set.find(0));
    EXPECT_NE(set.begin(), set.end());

    // map any offset in a page to the containing page
    for (int i = 0; i < 10; ++i) {
        const auto it = set.find(i);
        EXPECT_EQ((*it).get(), page.get());
    }

    // end should be returned for an offset beyond the mapped range
    EXPECT_EQ(set.find(10), set.end());
}

TEST(PageSet, InsertOverlap) {
    io::page_set set;

    const auto res = set.insert(make_page(0, 10));
    EXPECT_TRUE(res.second);
    EXPECT_EQ(res.first, set.find(0));

    const auto res2 = set.insert(make_page(5, 10));
    EXPECT_FALSE(res2.second);

    // res.first is the conflicting page
    EXPECT_EQ(res2.first, res.first);
}

TEST(PageSet, InsertZeroLengthPageRejected) {
    io::page_set set;

    // zero length rejected
    auto res = set.insert(make_page(0, 0));
    EXPECT_FALSE(res.second);
    EXPECT_EQ(res.first, set.end());
    EXPECT_EQ(set.begin(), set.end());

    // non-zero length accepted
    res = set.insert(make_page(0, 10));
    EXPECT_TRUE(res.second);
    EXPECT_EQ(set.find(0), res.first);
    EXPECT_NE(set.begin(), set.end());

    // still reject zero length
    res = set.insert(make_page(0, 0));
    EXPECT_FALSE(res.second);
    EXPECT_EQ(res.first, set.end());
}

TEST(PageSet, Erase) {
    io::page_set set;

    // add two pages [0, 10) [20, 30)
    auto res = set.insert(make_page(0, 10));
    EXPECT_TRUE(res.second);
    res = set.insert(make_page(20, 10));
    EXPECT_TRUE(res.second);
    EXPECT_NE(set.begin(), set.end());

    // remove the first page. set is not empty
    auto next = set.erase(set.find(5));
    EXPECT_NE(set.begin(), set.end());
    EXPECT_EQ(next, set.begin());

    // remove the second
    next = set.erase(set.find(25));

    // now the set is empty
    EXPECT_EQ(set.begin(), set.end());
    EXPECT_EQ(next, set.end());
}
