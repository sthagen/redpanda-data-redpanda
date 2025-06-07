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

#include "cluster/self_test/diskcheck.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "cluster/logger.h"
#include "random/generators.h"
#include "ssx/sformat.h"
#include "utils/directory_walker.h"
#include "utils/uuid.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>

#include <boost/range/irange.hpp>

#include <cstdint>

namespace cluster::self_test {

void diskcheck::validate_options(const diskcheck_opts& opts) {
    using namespace std::chrono_literals;
    if (opts.skip_write == true && opts.skip_read == true) {
        throw diskcheck_option_out_of_range(
          "Both skip_write and skip_read are true");
    }
    const auto duration = std::chrono::duration_cast<std::chrono::seconds>(
      opts.duration);
    if (duration < 1s || duration > (5 * 60s)) {
        throw diskcheck_option_out_of_range(
          "Duration out of range, min is 1s max is 5 minutes");
    }
    if (opts.parallelism < 1 || opts.parallelism > 256) {
        throw diskcheck_option_out_of_range(
          "IO Queue depth (parallelism) out of range, min is 1, max 256");
    }
}

diskcheck::diskcheck(ss::sharded<node::local_monitor>& nlm)
  : _nlm(nlm) {}

ss::future<> diskcheck::start() { return ss::now(); }

ss::future<> diskcheck::stop() {
    /// If test is currently running, expect `benchmark_aborted_exception`
    auto f = _gate.close();
    _as.request_abort();
    _intent.cancel();
    return f;
}

void diskcheck::cancel() {
    _cancelled = true;
    _intent.cancel();
}

ss::future<> diskcheck::verify_remaining_space(size_t dataset_size) {
    co_await _nlm.invoke_on(
      node::local_monitor::shard,
      [](node::local_monitor& lm) { return lm.update_state(); });
    const auto disk_state = co_await _nlm.invoke_on(
      node::local_monitor::shard,
      [](node::local_monitor& lm) { return lm.get_state_cached(); });
    if (disk_state.data_disk.free <= dataset_size) {
        throw diskcheck_option_out_of_range(fmt::format(
          "Not enough disk space to run benchmark, requested: {}, existing: {}",
          dataset_size,
          disk_state.data_disk.free));
    }
}

ss::future<std::vector<self_test_result>> diskcheck::run(diskcheck_opts opts) {
    if (_gate.is_closed()) {
        vlog(clusterlog.debug, "diskcheck - gate already closed");
        co_return std::vector<self_test_result>();
    }
    auto g = _gate.hold();
    co_await ss::futurize_invoke(validate_options, opts);
    co_await verify_remaining_space(opts.data_size);
    vlog(
      clusterlog.info,
      "Starting redpanda self-test disk benchmark, with options: {}",
      opts);
    _cancelled = false;
    _opts = opts;
    if (std::filesystem::exists(_opts.dir)) {
        /// Ensure no leftover large files in the event there was a
        /// crash mid run and cleanup didn't get a chance to occur
        std::filesystem::remove_all(_opts.dir);
    }
    std::filesystem::create_directory(_opts.dir);
    const auto self_test_prefix = "rp-self-test";
    const auto fname = ssx::sformat(
      "{}/{}-{}-{}",
      _opts.dir.string(),
      self_test_prefix,
      uuid_t::create(),
      ss::this_shard_id());
    co_return co_await initialize_benchmark(fname).finally(
      [this, &self_test_prefix] {
          vlog(
            clusterlog.debug,
            "redpanda self-test disk benchmark completed gracefully");
          return directory_walker::walk(
            _opts.dir.string(),
            [this,
             &self_test_prefix](const ss::directory_entry& de) -> ss::future<> {
                if (!de.name.contains(self_test_prefix)) {
                    return ss::now();
                }
                auto full_name = fmt::format(
                  "{}/{}", _opts.dir.string(), de.name);
                vlog(clusterlog.debug, "Deleting file: {}", full_name);
                return ss::remove_file(full_name).handle_exception_type(
                  [full_name](const std::filesystem::filesystem_error& fs_ex)
                    -> ss::future<> {
                      vlog(
                        clusterlog.error,
                        "Couldn't delete {}, reason {}",
                        full_name,
                        fs_ex);
                      return ss::now();
                  });
            });
      });
}

ss::future<std::vector<self_test_result>>
diskcheck::initialize_benchmark(ss::sstring basename) {
    auto flags = ss::open_flags::create | ss::open_flags::rw;
    ss::file_open_options file_opts{
      .extent_allocation_size_hint = _opts.file_size(),
      .append_is_unlikely = true};
    try {
        std::vector<ss::file> files;
        for (size_t i = 0; i < _opts.parallelism; ++i) {
            auto filename = fmt::format("{}-{}", basename, i);
            vlog(clusterlog.debug, "Creating file: {}", filename);
            auto file = co_await ss::open_file_dma(filename, flags, file_opts);
            co_await file.allocate(0, _opts.file_size());
            co_await file.truncate(_opts.file_size());
            co_await file.flush();
            files.push_back(std::move(file));
        }
        co_return co_await ss::with_scheduling_group(
          _opts.sg, [this, &files]() mutable {
              return run_configured_benchmarks(files);
          });
    } catch (const diskcheck_aborted_exception& ex) {
        vlog(clusterlog.debug, "diskcheck stopped due to call to stop()");
    }
    co_return std::vector<self_test_result>{};
}

ss::future<std::vector<self_test_result>>
diskcheck::run_configured_benchmarks(std::vector<ss::file>& files) {
    std::vector<self_test_result> r;
    auto write_metrics = co_await do_run_benchmark<read_or_write::write>(files);
    auto result = write_metrics.to_st_result();
    result.name = _opts.name;
    result.info = fmt::format(
      "write run (iodepth: {}, dsync: {})", _opts.parallelism, _opts.dsync);
    result.test_type = "disk";
    if (_cancelled) {
        result.warning = "Run was manually cancelled";
    }
    r.push_back(std::move(result));
    if (!_opts.skip_read) {
        auto read_metrics = co_await do_run_benchmark<read_or_write::read>(
          files);
        auto result = read_metrics.to_st_result();
        result.name = _opts.name;
        result.info = "read run";
        result.test_type = "disk";
        if (_cancelled) {
            result.warning = "Run was manually cancelled";
        }
        r.push_back(std::move(result));
    }
    co_return r;
}

template<diskcheck::read_or_write mode>
ss::future<metrics> diskcheck::do_run_benchmark(std::vector<ss::file>& files) {
    auto irange = boost::irange<uint16_t>(0, _opts.parallelism);
    auto start = ss::lowres_clock::now();
    auto start_highres = ss::lowres_system_clock::now();
    static const auto five_seconds_us = 500000;
    metrics m{five_seconds_us};
    ss::timer<ss::lowres_clock> timer;
    timer.set_callback([this] { _intent.cancel(); });
    timer.rearm(start + _opts.duration);
    try {
        co_await ss::parallel_for_each(
          irange, [this, &start, &files, &m](uint64_t i) {
              return this->run_benchmark_fiber<mode>(start, files[i], m);
          });
    } catch (const ss::cancelled_error&) {
        /// Expect this to be thrown from cancelled futures via io calls, due to
        /// _intent.cancel() having been called
        vlog(clusterlog.debug, "Benchmark completed (duration reached)");
    }
    timer.cancel();
    auto end = ss::lowres_system_clock::now();
    m.set_start_end_time(start_highres, end);
    m.set_total_time(end - start_highres);
    co_return m;
}

ss::future<size_t> write_and_maybe_flush(
  ss::file& file,
  uint64_t pos,
  const std::vector<iovec>& iov,
  bool dsync,
  ss::io_intent* intent) {
    auto bytes_written = co_await file.dma_write(pos, iov, intent);
    if (dsync) {
        co_await file.flush();
    }
    co_return bytes_written;
}

template<diskcheck::read_or_write mode>
ss::future<> diskcheck::run_benchmark_fiber(
  ss::lowres_clock::time_point start, ss::file& file, metrics& m) {
    const auto buf_len = std::min(_opts.request_size, 128_KiB);
    auto buf = ss::allocate_aligned_buffer<char>(buf_len, _opts.alignment());
    random_generators::fill_buffer_randomchars(buf.get(), buf_len);

    std::vector<iovec> iov;
    iov.reserve(_opts.request_size / buf_len);
    for (size_t offset = 0; offset < _opts.request_size; offset += buf_len) {
        size_t len = std::min(_opts.request_size - offset, buf_len);
        iov.push_back(iovec{buf.get(), len});
    }

    uint64_t pos = 0;
    auto stop = start + _opts.duration;
    while (stop > ss::lowres_clock::now() && !_cancelled) {
        if (unlikely(_as.abort_requested())) {
            throw diskcheck_aborted_exception();
        }
        co_await m.measure([this, &iov, &file, &pos] {
            if constexpr (mode == read_or_write::write) {
                return write_and_maybe_flush(
                  file, pos, iov, _opts.dsync, &_intent);
            } else {
                return file.dma_read(pos, iov, &_intent);
            }
        });
        pos = get_next_pos(pos);
    }
}

uint64_t diskcheck::get_next_pos(uint64_t pos) {
    uint64_t next_pos = pos + _opts.request_size;
    if (next_pos >= _opts.file_size()) {
        return 0;
    }
    return next_pos;
}

} // namespace cluster::self_test
