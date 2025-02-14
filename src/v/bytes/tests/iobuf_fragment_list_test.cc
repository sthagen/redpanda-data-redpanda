// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/details/io_fragment.h"

#include <gtest/gtest-matchers.h>
#include <gtest/gtest.h>

#include <list>
#include <ranges>

using namespace details;

namespace {

TEST(FragmentListTests, Back) {
    io_fragment_list list;
    io_fragment a{0};
    io_fragment b{0};
    io_fragment c{0};
    EXPECT_TRUE(list.empty());
    list.push_back(a);
    EXPECT_EQ(&list.front(), &a);
    EXPECT_EQ(&list.back(), &a);
    EXPECT_FALSE(list.empty());
    list.push_back(b);
    EXPECT_EQ(&list.front(), &a);
    EXPECT_EQ(&list.back(), &b);
    EXPECT_FALSE(list.empty());
    list.push_back(c);
    EXPECT_EQ(&list.front(), &a);
    EXPECT_EQ(&list.back(), &c);
    EXPECT_FALSE(list.empty());
    list.pop_back();
    EXPECT_EQ(&list.front(), &a);
    EXPECT_EQ(&list.back(), &b);
    EXPECT_FALSE(list.empty());
    list.pop_back();
    EXPECT_EQ(&list.front(), &a);
    EXPECT_EQ(&list.back(), &a);
    EXPECT_FALSE(list.empty());
    list.pop_back();
    EXPECT_TRUE(list.empty());
}

TEST(FragmentListTests, Front) {
    io_fragment_list list;
    io_fragment a{0};
    io_fragment b{0};
    io_fragment c{0};
    EXPECT_TRUE(list.empty());
    list.push_front(a);
    EXPECT_EQ(&list.front(), &a);
    EXPECT_EQ(&list.back(), &a);
    EXPECT_FALSE(list.empty());
    list.push_front(b);
    EXPECT_EQ(&list.front(), &b);
    EXPECT_EQ(&list.back(), &a);
    EXPECT_FALSE(list.empty());
    list.push_front(c);
    EXPECT_EQ(&list.front(), &c);
    EXPECT_EQ(&list.back(), &a);
    EXPECT_FALSE(list.empty());
    list.pop_front();
    EXPECT_EQ(&list.front(), &b);
    EXPECT_EQ(&list.back(), &a);
    EXPECT_FALSE(list.empty());
    list.pop_front();
    EXPECT_EQ(&list.front(), &a);
    EXPECT_EQ(&list.back(), &a);
    EXPECT_FALSE(list.empty());
    list.pop_front();
    EXPECT_TRUE(list.empty());
}

TEST(FragmentListTests, FrontBack) {
    io_fragment_list list;
    io_fragment a{0};
    io_fragment b{0};
    io_fragment c{0};
    EXPECT_TRUE(list.empty());
    list.push_front(a);
    EXPECT_EQ(&list.front(), &a);
    EXPECT_EQ(&list.back(), &a);
    EXPECT_FALSE(list.empty());
    list.push_front(b);
    EXPECT_EQ(&list.front(), &b);
    EXPECT_EQ(&list.back(), &a);
    EXPECT_FALSE(list.empty());
    list.push_front(c);
    EXPECT_EQ(&list.front(), &c);
    EXPECT_EQ(&list.back(), &a);
    EXPECT_FALSE(list.empty());
    list.pop_back();
    EXPECT_EQ(&list.front(), &c);
    EXPECT_EQ(&list.back(), &b);
    EXPECT_FALSE(list.empty());
    list.pop_back();
    EXPECT_EQ(&list.front(), &c);
    EXPECT_EQ(&list.back(), &c);
    EXPECT_FALSE(list.empty());
    list.pop_back();
    EXPECT_TRUE(list.empty());
}

TEST(FragmentListTests, BackFront) {
    io_fragment_list list;
    io_fragment a{0};
    io_fragment b{0};
    io_fragment c{0};
    EXPECT_TRUE(list.empty());
    list.push_back(a);
    EXPECT_EQ(&list.front(), &a);
    EXPECT_EQ(&list.back(), &a);
    EXPECT_FALSE(list.empty());
    list.push_back(b);
    EXPECT_EQ(&list.front(), &a);
    EXPECT_EQ(&list.back(), &b);
    EXPECT_FALSE(list.empty());
    list.push_back(c);
    EXPECT_EQ(&list.front(), &a);
    EXPECT_EQ(&list.back(), &c);
    EXPECT_FALSE(list.empty());
    list.pop_front();
    EXPECT_EQ(&list.front(), &b);
    EXPECT_EQ(&list.back(), &c);
    EXPECT_FALSE(list.empty());
    list.pop_front();
    EXPECT_EQ(&list.front(), &c);
    EXPECT_EQ(&list.back(), &c);
    EXPECT_FALSE(list.empty());
    list.pop_front();
    EXPECT_TRUE(list.empty());
}

TEST(FragmentListTests, IteratorIncrement) {
    io_fragment_list list;
    io_fragment a{0};
    io_fragment b{0};
    io_fragment c{0};
    list.push_back(a);
    list.push_back(b);
    list.push_back(c);
    std::list<io_fragment*> shadow = {&a, &b, &c};
    while (!list.empty()) {
        auto it = list.begin();
        for (io_fragment* frag : shadow) {
            ASSERT_NE(it, list.end());
            EXPECT_EQ(&*it, frag);
            ++it;
        }
        EXPECT_EQ(it, list.end());
        auto rit = list.rbegin();
        for (io_fragment* frag : std::ranges::reverse_view(shadow)) {
            ASSERT_NE(rit, list.rend());
            EXPECT_EQ(&*rit, frag);
            ++rit;
        }
        EXPECT_EQ(rit, list.rend());
        auto cit = list.cbegin();
        for (io_fragment* frag : shadow) {
            ASSERT_NE(cit, list.cend());
            EXPECT_EQ(&*cit, frag);
            ++cit;
        }
        EXPECT_EQ(cit, list.cend());
        list.pop_back();
        shadow.pop_back();
    }
    EXPECT_EQ(list.begin(), list.end());
    EXPECT_EQ(list.cbegin(), list.cend());
    EXPECT_EQ(list.rbegin(), list.rend());
}

TEST(FragmentListTests, Clear) {
    io_fragment_list list;
    io_fragment a{0};
    io_fragment b{0};
    io_fragment c{0};
    list.push_back(a);
    list.push_back(b);
    list.push_back(c);
    std::list<io_fragment*> shadow = {&a, &b, &c};
    list.clear_and_dispose([&shadow](io_fragment* f) {
        EXPECT_EQ(shadow.front(), f);
        shadow.pop_front();
    });
    EXPECT_TRUE(shadow.empty());
    EXPECT_TRUE(list.empty());
}

} // namespace
