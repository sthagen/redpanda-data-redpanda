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

#include "crash_tracker/logger.h"
#include "crash_tracker/recorder.h"

#include <seastar/core/smp.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/log.hh>
#include <seastar/util/print_safe.hh>

namespace crash_tracker {

namespace {

// Installs handler for Signal which ensures that Func is invoked only once
// in the whole program. After Func is invoked, the previously-installed signal
// handler is restored and triggered.
template<int Signal, void (*Func)()>
void install_oneshot_signal_handler() {
    static ss::util::spinlock lock{};

    // Capture the original (seastar's or SIG_DFL) signal handler so we can
    // run it after redpanda's signal handler
    static struct sigaction original_sa;

    struct sigaction sa;
    sa.sa_sigaction = [](int sig, siginfo_t*, void*) {
        std::lock_guard<ss::util::spinlock> g{lock};

        Func();

        // Reinstall the original signal handler after calling Func() to ensure
        // that a concurrent signal on another thread does not terminate the
        // process before Func() finishes
        auto r = ::sigaction(sig, &original_sa, nullptr);
        if (r == -1) {
            // This should not be possible given the man page of sigaction
            // but it is prudent to handle this case by reinstalling the
            // default sighandler
            constexpr static std::string_view sigaction_failure
              = "Failed to reinstall original signal handler\n";
            ss::print_safe(sigaction_failure.data(), sigaction_failure.size());

            ::signal(sig, SIG_DFL);
        }

        // Reraise the signal to trigger the original signal handler
        pthread_kill(pthread_self(), sig);
    };
    sigfillset(&sa.sa_mask);
    sa.sa_flags = SA_SIGINFO | SA_RESTART;
    if (Signal == SIGSEGV) {
        sa.sa_flags |= SA_ONSTACK;
    }
    auto r = ::sigaction(Signal, &sa, &original_sa);
    ss::throw_system_error_on(r == -1);

    // Unblock the signal on all reactor threads to ensure that our handler is
    // called
    ss::smp::invoke_on_all([] {
        auto mask = ss::make_sigset_mask(Signal);
        ss::throw_pthread_error(::pthread_sigmask(SIG_UNBLOCK, &mask, nullptr));
    }).get();
}

static void maybe_print_backtrace(std::string_view signal_msg) noexcept {
    if (ctlog.is_enabled(ss::log_level::debug)) {
        ss::print_safe(signal_msg.data(), signal_msg.size());
        ss::print_safe(":\n");
        ss::backtrace(
          [](const ss::frame& f) {
              ss::print_safe("0x");
              ss::print_zero_padded_hex_safe(f.addr);
              ss::print_safe(" in ");
              ss::print_safe(f.so->name.c_str());
              ss::print_safe("\n");
          },
          true);
    }
}

static void sigsegv_action() noexcept {
    maybe_print_backtrace("Segmentation fault");
    constexpr auto signo = crash_tracker::recorder::recorded_signo::sigsegv;
    crash_tracker::get_recorder().record_crash_sighandler(signo);
}
void sigabrt_action() {
    maybe_print_backtrace("Aborting");
    constexpr auto signo = crash_tracker::recorder::recorded_signo::sigabrt;
    crash_tracker::get_recorder().record_crash_sighandler(signo);
}
void sigill_action() {
    maybe_print_backtrace("Illegal instruction");
    constexpr auto signo = crash_tracker::recorder::recorded_signo::sigill;
    crash_tracker::get_recorder().record_crash_sighandler(signo);
}

} // namespace

void install_sighandlers() {
    install_oneshot_signal_handler<SIGSEGV, sigsegv_action>();
    install_oneshot_signal_handler<SIGABRT, sigabrt_action>();
    install_oneshot_signal_handler<SIGILL, sigill_action>();
}

} // namespace crash_tracker
