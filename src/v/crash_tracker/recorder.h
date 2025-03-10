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

#pragma once

#include "base/seastarx.h"
#include "crash_tracker/prepared_writer.h"
#include "crash_tracker/types.h"

#include <seastar/util/bool_class.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>

namespace crash_tracker {

/// Thread-safe global singleton crash recorder
/// The singleton pattern is used to allow access to the recorder from signal
/// handlers which have to be static functions (/non-capturing lambdas).
class recorder {
public:
    static constexpr auto crash_files_to_keep = 50;
    static constexpr std::string upload_marker_suffix = ".uploaded";

    struct recorded_crash {
        std::filesystem::path file_path;
        std::optional<crash_description> crash;
        std::chrono::system_clock::time_point last_write_time;

        ss::future<bool> is_uploaded() const;
        ss::future<> mark_uploaded() const;
        std::chrono::system_clock::time_point timestamp() const;
    };

    using include_malformed_files
      = ss::bool_class<struct include_malformed_files_tag>;
    using include_current = ss::bool_class<struct include_current_tag>;

    enum class recorded_signo { sigsegv, sigabrt, sigill };

    /// Visible for testing
    ~recorder() = default;

    ss::future<> start();
    ss::future<> stop();
    /// _ONLY_ to be used during testing, will reset the underlying writer and
    /// prepare it for `start()`
    void reset();

    /// Async-signal safe
    void record_crash_sighandler(recorded_signo signo);

    void record_crash_exception(std::exception_ptr eptr);

    void record_crash_vassert(std::string_view msg);

    using oom_recorder = ss::noncopyable_function<void(std::string_view)>;

    // This function is used to begin recording messages on an OOM crash.
    // If able to, it will return a function that can be called to append
    // messages to the crash description until finish_oom_recording()
    // is called.
    std::optional<oom_recorder> begin_oom_recording();
    // Called when finished recording OOM messages
    void finish_oom_recording();

    /// Returns the list of recorded crashes in increasing crash_time order
    ///  - incl_malformed: whether to include unparseable crash files
    ///  - incl_current: whether to include the crash file corresponding to the
    ///    current process
    ss::future<std::vector<recorded_crash>> get_recorded_crashes(
      include_malformed_files incl_malformed = include_malformed_files::no,
      include_current incl_current = include_current::no) const;

private:
    recorder() = default;

    ss::future<> ensure_crashdir_exists() const;
    ss::future<std::filesystem::path> generate_crashfile_name() const;
    ss::future<> remove_old_crashfiles() const;
    ss::future<> remove_dangling_upload_markers() const;

    prepared_writer _writer;

    class oom_writer {
    public:
        using iterator =
          typename crash_description::reserved_string_t::iterator;
        explicit oom_writer(crash_description* oom_cd)
          : _oom_cd(oom_cd)
          , _oom_msg_pos(_oom_cd->crash_message.begin()) {}

        void operator()(std::string_view) noexcept;

    private:
        crash_description* _oom_cd{nullptr};
        iterator _oom_msg_pos;
    };

    std::optional<oom_writer> _oom_writer;

    friend recorder& get_recorder();
    friend recorder get_test_recorder();
};

/// Singleton access to global static recorder
recorder& get_recorder();

/// Make a test instance of the recorder that is not a static singleton
recorder get_test_recorder();

} // namespace crash_tracker
