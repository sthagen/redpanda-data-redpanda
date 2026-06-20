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
#include <cstddef>

// https://en.wikipedia.org/wiki/Gibibyte
// powers of 2

constexpr size_t KiB = 1024;
constexpr size_t MiB = 1024 * KiB;
constexpr size_t GiB = 1024 * MiB;
constexpr size_t TiB = 1024 * GiB;

// powers of 10
constexpr size_t KB = 1'000;
constexpr size_t MB = 1'000 * KB;
constexpr size_t GB = 1'000 * MB;
constexpr size_t TB = 1'000 * GB;

// NOLINTBEGIN(google-runtime-int)
// kibibytes (2^10 bytes) -> bytes
constexpr size_t operator""_KiB(unsigned long long val) { return val * KiB; }
// mebibytes (2^20 bytes) -> bytes
constexpr size_t operator""_MiB(unsigned long long val) { return val * MiB; }
// gibibytes (2^30 bytes) -> bytes
constexpr size_t operator""_GiB(unsigned long long val) { return val * GiB; }
// tebibytes (2^40 bytes) -> bytes
constexpr size_t operator""_TiB(unsigned long long val) { return val * TiB; }
// kilobytes (10^3 bytes) -> bytes
constexpr size_t operator""_KB(unsigned long long val) { return val * KB; }
// megabytes (10^6 bytes) -> bytes
constexpr size_t operator""_MB(unsigned long long val) { return val * MB; }
// gigabytes (10^9 bytes) -> bytes
constexpr size_t operator""_GB(unsigned long long val) { return val * GB; }
// terabytes (10^12 bytes) -> bytes
constexpr size_t operator""_TB(unsigned long long val) { return val * TB; }
// NOLINTEND(google-runtime-int)
