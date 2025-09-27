/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <random>

///
/// @brief Forward declaration for accessor struct that can access
/// private members of rng for testing or internal purposes.
/// Only rng declares this as a friend.
///
struct random_state_accessor;

// Random generators useful for testing.
namespace random_generators {

struct random_seed_tag {};

/**
 * @brief Holds random generator state and allows generation of random values.
 *
 * Each object of this class encapsulates its own state. I.e., two objects
 * created with the same seed will generate the same sequence of values.
 *
 * It is similar in principle to a std "random engine", but with methods to
 * directly generate random values of various types, and with a default
 * seeding policy that allows seeding to vary between fuzz and non-fuzz tests.
 */
class rng {
public:
    using seed_type = uint32_t;
    using engine_type = std::default_random_engine;

    // Initializes an rng object using the _default_ seed
    // policy, which is fixed in most contexts, but random in
    // fuzz test contexts.
    rng();

    // Initializes an rng object using a random seed regardless
    // of the default policy.
    explicit rng(random_seed_tag);

    // Initializes with a given seed.
    explicit rng(seed_type seed);

    // The initial seed used to create this rng object.
    // If you create another object with the same seed it should
    // generate the same sequence of values.
    seed_type initial_seed() const { return initial_seed_; }

    template<typename T>
    T get_int() {
        std::uniform_int_distribution<T> dist;
        return dist(gen_);
    }

    template<typename T>
    T get_int(T min, T max) {
        std::uniform_int_distribution<T> dist(min, max);
        return dist(gen_);
    }

    template<typename T>
    T get_int(T max) {
        return get_int<T>(0, max);
    }

    template<typename T>
    const T& random_choice(const std::vector<T>& elements) {
        auto idx = get_int<size_t>(0, elements.size() - 1);
        return elements[idx];
    }

    template<typename T>
    T& random_choice(std::vector<T>& elements) {
        auto idx = get_int<size_t>(0, elements.size() - 1);
        return elements[idx];
    }

    template<typename T>
    T random_choice(std::initializer_list<T> choices) {
        auto idx = get_int<size_t>(0, choices.size() - 1);
        auto& choice = *(choices.begin() + idx);
        return std::move(choice);
    }

    template<typename T>
    T get_real() {
        std::uniform_real_distribution<T> dist;
        return dist(gen_);
    }

    template<typename T>
    T get_real(T min, T max) {
        std::uniform_real_distribution<T> dist(min, max);
        return dist(gen_);
    }

    template<typename T>
    T get_real(T max) {
        std::uniform_real_distribution<T> dist(0, max);
        return dist(gen_);
    }

    template<typename T>
    std::vector<T> randomized_range(T min, T max) {
        std::vector<T> r(max - min);
        std::iota(r.begin(), r.end(), min);
        std::shuffle(r.begin(), r.end(), gen_);
        return r;
    }

    // Generate a random string of alphanumeric characters of length n.
    ss::sstring gen_alphanum_string(size_t n);

    // Returns the underlying random engine, for use in
    // algorithms that require one.
    engine_type& engine() { return gen_; }

private:
    engine_type gen_;
    seed_type initial_seed_;
    friend random_state_accessor;
};

// Return a rng object which is seeded randomly regardless of
// the default seeding policy.
rng with_random_seed();

rng& global();

namespace internal {

enum class seeding_mode {
    random_seed = 1,
    fixed_seed = 2,
};

// Get the global seeding mode
seeding_mode default_seeding_policy();

// Increment the seed generation which causes the subsequent call to global()
// to reseed the global generator based on the default seed policy.
void increment_seed_generation();

static constexpr std::string_view chars
  = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

} // namespace internal

/**
 * Random string generator. Total number of distinct values that may be
 * generated is unlimited (within all possible values of given size).
 */
ss::sstring gen_alphanum_string(size_t n);

inline constexpr size_t alphanum_max_distinct_strlen = 32;
/**
 * Random string generator that limits the maximum number of distinct values
 * that will be returned. That is, this function is a generator, which creates
 * members of a set of strings, one at a time. Each generated string has maximum
 * length `alphanum_max_distinct_strlen`. The total set of generated strings
 * will have a maximum cardinality of `max_cardinality`. See the unit test
 * `alphanum_max_distinct_generator` for an example.
 */
ss::sstring gen_alphanum_max_distinct(size_t max_cardinality);

void fill_buffer_randomchars(char* start, size_t amount);

template<typename T>
std::vector<T> randomized_range(T min, T max) {
    return global().randomized_range(min, max);
}

// Global forwarding functions: each of these just calls the corresponding
// method on the global() rng object.

template<typename T>
T get_int() {
    return global().get_int<T>();
}

template<typename T>
T get_int(T min, T max) {
    return global().get_int(min, max);
}

template<typename T>
T get_int(T max) {
    return global().get_int(max);
}

template<typename T>
const T& random_choice(const std::vector<T>& elements) {
    return global().random_choice(elements);
}

template<typename T>
T& random_choice(std::vector<T>& elements) {
    return global().random_choice(elements);
}

template<typename T>
T random_choice(std::initializer_list<T> choices) {
    return global().random_choice(choices);
}

template<typename T>
T get_real() {
    return global().get_real<T>();
}

template<typename T>
T get_real(T min, T max) {
    return global().get_real<T>(min, max);
}

template<typename T>
T get_real(T max) {
    return global().get_real<T>(max);
}

} // namespace random_generators
