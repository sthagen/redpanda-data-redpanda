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

#include "crash_tracker/recorder.h"

#include "config/node_config.h"
#include "crash_tracker/logger.h"
#include "crash_tracker/types.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "utils/file_io.h"

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/print_safe.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace crash_tracker {

static constexpr std::string_view crash_report_suffix = ".crash";

recorder& get_recorder() {
    static recorder inst;
    return inst;
}

ss::future<> recorder::start() {
    // Ensure that the crash report directory exists
    auto crash_report_dir = config::node().crash_report_dir_path();
    if (!co_await ss::file_exists(crash_report_dir.string())) {
        vlog(
          ctlog.info,
          "Creating crash report directory {}",
          crash_report_dir.string());
        co_await ss::recursive_touch_directory(crash_report_dir.string());
        vlog(
          ctlog.debug,
          "Successfully created crash report directory {}",
          crash_report_dir.string());
    }

    // Loop a few times to avoid (very unlikely) collisions in the filename
    std::optional<std::filesystem::path> crash_file_name{};
    for (int i = 0; i < 10; ++i) {
        auto time_now = model::timestamp::now().value();
        auto random_int = random_generators::get_int(0, 10000);
        auto try_name = crash_report_dir
                        / fmt::format(
                          "{}_{}{}", time_now, random_int, crash_report_suffix);
        if (co_await ss::file_exists(try_name.string())) {
            // Try again in the rare case of a collision
            continue;
        }

        crash_file_name = try_name;
        break;
    }
    if (!crash_file_name) {
        // The anti-collision above should ensure that we never reach this
        throw std::runtime_error(
          "Failed to create a unique crash recorder file");
    }

    co_await _writer.initialize(*crash_file_name);
}

namespace {

void record_backtrace(crash_description& cd) {
    size_t pos = 0;
    ss::backtrace([&cd, &pos](ss::frame f) {
        if (pos >= cd.stacktrace.capacity()) {
            return; // Prevent buffer overflow
        }

        const bool first = pos == 0;
        auto result = fmt::format_to_n(
          cd.stacktrace.begin() + pos,
          cd.stacktrace.capacity() - pos,
          "{}{:#x}",
          first ? "" : " ",
          f.addr);

        pos += result.size;
    });
}

void print_skipping() {
    constexpr static std::string_view skipping
      = "Skipping recording crash reason to crash file.\n";
    ss::print_safe(skipping.data(), skipping.size());
}

} // namespace

void recorder::record_crash_exception(std::exception_ptr eptr) {
    if (is_crash_loop_limit_reached(eptr)) {
        // We specifically do not want to record crash_loop_limit_reached errors
        // as crashes because they are not informative and would build up
        // garbage on disk and would force to expire earlier useful crash logs.
        return;
    }

    auto* cd_opt = _writer.fill();
    if (!cd_opt) {
        // The writer has already been consumed by another crash
        print_skipping();
        return;
    }
    auto& cd = *cd_opt;

    record_backtrace(cd);
    cd.type = crash_type::startup_exception;

    auto& format_buf = cd.crash_message;
    fmt::format_to_n(
      format_buf.begin(),
      format_buf.capacity(),
      "Failure during startup: {}",
      eptr);

    _writer.write();
}

ss::future<std::vector<recorder::recorded_crash>>
recorder::get_recorded_crashes() const {
    auto result = std::vector<recorded_crash>{};
    auto crash_report_dir = config::node().crash_report_dir_path();
    if (!co_await ss::file_exists(crash_report_dir.string())) {
        co_return result;
    }

    for (const auto& entry :
         std::filesystem::directory_iterator(crash_report_dir)) {
        if (!entry.path().string().ends_with(crash_report_suffix)) {
            // Filter only for crash files
            continue;
        }

        auto buf = co_await read_fully(entry.path());
        try {
            auto crash_desc = serde::from_iobuf<crash_description>(
              std::move(buf));
            result.emplace_back(std::move(crash_desc));
        } catch (const serde::serde_exception&) {
            vlog(
              ctlog.warn,
              "Ignoring malformed crash report file {}",
              entry.path());
        }
    }

    std::sort(
      result.begin(),
      result.end(),
      [](const recorded_crash& a, const recorded_crash& b) {
          return a.crash.crash_time < b.crash.crash_time;
      });

    co_return result;
}

ss::future<> recorder::stop() { co_await _writer.release(); }

} // namespace crash_tracker
