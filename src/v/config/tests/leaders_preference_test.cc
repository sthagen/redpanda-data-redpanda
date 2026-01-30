/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config/leaders_preference.h"

#include <seastar/testing/thread_test_case.hh>

#include <stdexcept>

SEASTAR_THREAD_TEST_CASE(parse_leaders_preference) {
    using lp = config::leaders_preference;
    using lp_t = config::leaders_preference::type_t;

    { // config none -> lp none
        auto lp = lp::parse(lp::none_prefix);
        BOOST_CHECK_EQUAL(lp.type, lp_t::none);
        BOOST_CHECK_EQUAL(lp.racks.size(), 0);
    }

    { // default round trip lp -> str -> lp
        lp orig{};
        auto parsed = lp::parse(fmt::to_string(orig));
        BOOST_CHECK_EQUAL(orig, parsed);
    }

    static constexpr auto parse_combos = {
      std::pair{lp_t::racks, lp::racks_prefix},
      std::pair{lp_t::ordered_racks, lp::ordered_racks_prefix}};

    static const auto rack_a = model::rack_id{"A"};
    static const auto rack_b = model::rack_id{"B"};

    for (const auto& [pref_type, pref_prefix] : parse_combos) {
        { // single rack str -> lp
            auto lp = lp::parse(fmt::format("{}{}", pref_prefix, rack_a));
            BOOST_CHECK_EQUAL(lp.type, pref_type);
            BOOST_CHECK_EQUAL(lp.racks.size(), 1);
            BOOST_CHECK_EQUAL(lp.racks[0], rack_a);
        }

        { // multi rack str -> lp
            // backwards just to check that its not incidentally sorting
            auto lp = lp::parse(
              fmt::format("{}{}, {} ", pref_prefix, rack_b, rack_a));
            BOOST_CHECK_EQUAL(lp.type, pref_type);
            BOOST_CHECK_EQUAL(lp.racks.size(), 2);
            BOOST_CHECK_EQUAL(lp.racks[0], rack_b);
            BOOST_CHECK_EQUAL(lp.racks[1], rack_a);
        }

        { // multi rack rountrip lp -> str -> lp
            lp orig{};
            orig.type = pref_type;
            orig.racks = {rack_a, rack_b};
            auto parsed = lp::parse(fmt::to_string(orig));
            BOOST_CHECK_EQUAL(orig, parsed);
        }

        // no racks specified
        BOOST_CHECK_THROW(
          lp::parse(fmt::format("{} ", pref_prefix)), std::runtime_error);

        // empty string rack
        BOOST_CHECK_THROW(
          lp::parse(fmt::format("{} {},,{}", pref_prefix, rack_a, rack_b)),
          std::runtime_error);

        // trailing comma / empty string rack
        BOOST_CHECK_THROW(
          lp::parse(fmt::format("{}{},", pref_prefix, rack_a)),
          std::runtime_error);
    }

    // normal invalid racks config
    BOOST_CHECK_THROW(lp::parse("quux"), std::runtime_error);

    // ordered: duplicate racks should throw
    // if there's two instances of a rack in the config order, which should be
    // used for priority?
    BOOST_CHECK_THROW(
      lp::parse(
        fmt::format(
          "{} {},{},{}", lp::ordered_racks_prefix, rack_a, rack_a, rack_b)),
      std::runtime_error);

    // prior to ordered, racks were stored as a set, so duplicates were allowed
    // and deduplicated. behavior change: if theres duplicates return them as
    // provided
    { // unordered: duplicate racks are fine, check no error and roundtrip
        lp orig{};
        orig.type = lp_t::racks;
        orig.racks = {rack_a, rack_b};
        auto parsed = lp::parse(fmt::to_string(orig));
        BOOST_CHECK_EQUAL(orig, parsed);
    }
}
