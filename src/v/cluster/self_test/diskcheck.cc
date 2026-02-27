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
#include "cluster/self_test/metrics.h"
#include "random/generators.h"
#include "ssx/sformat.h"
#include "utils/directory_walker.h"
#include "utils/uuid.h"

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/switch_to.hh>

#include <boost/range/irange.hpp>

#include <algorithm>
#include <cstdint>

namespace cluster::self_test {

namespace {

enum class read_or_write { read, write };

static const auto five_hundred_us = 500000;

struct shard_benchmark_results {
    metrics write;
    std::optional<metrics> read;
};

metrics merge_metrics(std::vector<metrics> shard_metrics) {
    if (shard_metrics.empty()) {
        // This will be bogus but we should never get here with no metrics
        // anyway
        return metrics{five_hundred_us};
    }

    return std::ranges::fold_left(
      std::views::drop(shard_metrics, 1),
      std::move(shard_metrics.front()),
      [](metrics acc, const metrics& m) {
          acc.merge_from(m);
          return acc;
      });
}

uint64_t get_next_pos(uint64_t pos, const diskcheck_opts& opts) {
    uint64_t next_pos = pos + opts.request_size;
    if (next_pos >= opts.file_size()) {
        return 0;
    }
    return next_pos;
}

ss::future<size_t> write_and_maybe_flush(
  ss::file& file, uint64_t pos, const std::vector<iovec>& iov, bool dsync) {
    auto bytes_written = co_await file.dma_write(pos, iov);
    if (dsync) {
        co_await file.flush();
    }
    co_return bytes_written;
}

template<read_or_write mode>
ss::future<> run_benchmark_fiber(
  ss::lowres_clock::time_point start,
  ss::file& file,
  metrics& m,
  const diskcheck_opts& opts,
  ss::abort_source& cancelled) {
    const auto buf_len = std::min(opts.request_size, 128_KiB);
    auto buf = ss::allocate_aligned_buffer<char>(buf_len, opts.alignment());
    random_generators::fill_buffer_randomchars(buf.get(), buf_len);

    std::vector<iovec> iov;
    iov.reserve(opts.request_size / buf_len);
    for (size_t offset = 0; offset < opts.request_size; offset += buf_len) {
        size_t len = std::min(opts.request_size - offset, buf_len);
        iov.push_back(iovec{buf.get(), len});
    }

    uint64_t pos = 0;
    auto stop = start + opts.duration;
    while (stop > ss::lowres_clock::now() && !cancelled.abort_requested()) {
        if constexpr (mode == read_or_write::write) {
            co_await m.measure([&iov, &file, &pos, dsync = opts.dsync] {
                return write_and_maybe_flush(file, pos, iov, dsync);
            });
        } else {
            co_await m.measure(
              [&iov, &file, &pos] { return file.dma_read(pos, iov); });
        }
        pos = get_next_pos(pos, opts);
    }
}

template<read_or_write mode>
ss::future<metrics> do_run_benchmark(
  std::vector<ss::file>& files,
  const diskcheck_opts& opts,
  ss::abort_source& cancelled) {
    auto irange = boost::irange<uint16_t>(0, opts.parallelism);
    auto start = ss::lowres_clock::now();
    auto start_highres = ss::lowres_system_clock::now();
    metrics m{five_hundred_us};
    co_await ss::parallel_for_each(
      irange, [&start, &files, &m, &opts, &cancelled](uint64_t i) {
          return run_benchmark_fiber<mode>(start, files[i], m, opts, cancelled);
      });
    auto end = ss::lowres_system_clock::now();
    m.set_start_end_time(start_highres, end);
    m.set_total_time(end - start_highres);
    co_return m;
}

ss::future<shard_benchmark_results> run_shard_benchmark(
  diskcheck_opts opts, ss::sstring basename, ss::abort_source& cancelled) {
    auto flags = ss::open_flags::create | ss::open_flags::rw;
    ss::file_open_options file_opts{
      .extent_allocation_size_hint = opts.file_size(),
      .append_is_unlikely = true};

    std::vector<ss::file> files;
    files.reserve(opts.parallelism);
    for (size_t i = 0; i < opts.parallelism; ++i) {
        auto filename = fmt::format(
          "{}-{}-{}", basename, ss::this_shard_id(), i);
        vlog(clusterlog.debug, "Creating file: {}", filename);
        auto file = co_await ss::open_file_dma(filename, flags, file_opts);
        co_await file.allocate(0, opts.file_size());
        co_await file.truncate(opts.file_size());
        co_await file.flush();
        files.push_back(std::move(file));
    }

    auto write_metrics = co_await do_run_benchmark<read_or_write::write>(
      files, opts, cancelled);

    std::optional<metrics> read_result;
    if (!opts.skip_read) {
        read_result = co_await do_run_benchmark<read_or_write::read>(
          files, opts, cancelled);
    }

    co_return shard_benchmark_results{
      .write = std::move(write_metrics), .read = std::move(read_result)};
}

} // namespace

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
    if (_cancel_parent.has_value()) {
        _cancel_parent->request_abort();
    }
    co_await _gate.close();
    if (_cancelled.has_value()) {
        co_await _cancelled->stop();
        _cancelled.reset();
    }
    _cancel_parent.reset();
}

void diskcheck::cancel() {
    if (_cancel_parent.has_value()) {
        _cancel_parent->request_abort();
    }
}

ss::future<> diskcheck::verify_remaining_space(size_t dataset_size) {
    co_await _nlm.invoke_on(
      node::local_monitor::shard,
      [](node::local_monitor& lm) { return lm.update_state(); });
    const auto disk_state = co_await _nlm.invoke_on(
      node::local_monitor::shard,
      [](node::local_monitor& lm) { return lm.get_state_cached(); });
    if (disk_state.data_disk.free <= dataset_size) {
        throw diskcheck_option_out_of_range(
          fmt::format(
            "Not enough disk space to run benchmark, requested: {}, existing: "
            "{}",
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
    if (_cancelled.has_value()) {
        co_await _cancelled->stop();
        _cancelled.reset();
    }
    _cancel_parent.emplace();
    _cancelled.emplace();
    co_await _cancelled->start(*_cancel_parent);
    _opts = opts;
    if (std::filesystem::exists(_opts.dir)) {
        /// Ensure no leftover large files in the event there was a
        /// crash mid run and cleanup didn't get a chance to occur
        std::filesystem::remove_all(_opts.dir);
    }
    std::filesystem::create_directory(_opts.dir);
    const auto self_test_prefix = "rp-self-test";
    const auto fname = ssx::sformat(
      "{}/{}-{}", _opts.dir.string(), self_test_prefix, uuid_t::create());
    co_return co_await run_configured_benchmarks(fname).finally(
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
diskcheck::run_configured_benchmarks(ss::sstring basename) {
    auto active_shards = std::min<std::uint32_t>(
      _opts.parallelism, ss::smp::count);
    auto parallelism_per_shard = _opts.parallelism / active_shards;
    auto remainder = _opts.parallelism % active_shards;

    std::vector<ss::future<shard_benchmark_results>> shard_futures;
    for (size_t shard_index = 0; shard_index < active_shards; ++shard_index) {
        auto shard_parallelism = parallelism_per_shard
                                 + (shard_index < remainder ? 1 : 0);
        auto local_opts = _opts;
        local_opts.parallelism = shard_parallelism;
        local_opts.data_size /= active_shards;
        shard_futures.push_back(
          ss::smp::submit_to(
            shard_index,
            [local_opts, basename, this](
              this auto) -> ss::future<shard_benchmark_results> {
                co_await ss::coroutine::switch_to(local_opts.sg);
                co_return co_await run_shard_benchmark(
                  std::move(local_opts), basename, _cancelled->local());
            }));
    }

    auto per_shard_results = co_await ss::when_all_succeed(
      shard_futures.begin(), shard_futures.end());

    std::vector<self_test_result> r;
    r.reserve(_opts.skip_read ? 1 : 2);

    std::vector<metrics> write_shard_results;
    write_shard_results.reserve(per_shard_results.size());
    for (auto& shard_result : per_shard_results) {
        write_shard_results.push_back(std::move(shard_result.write));
    }

    auto write_result
      = merge_metrics(std::move(write_shard_results)).to_st_result();
    write_result.name = _opts.name;
    write_result.info = fmt::format(
      "write run (iodepth: {}, dsync: {}, shards: {})",
      _opts.parallelism,
      _opts.dsync,
      active_shards);
    write_result.test_type = "disk";
    if (_cancelled->abort_requested()) {
        write_result.warning = "Run was manually cancelled";
    }
    r.push_back(std::move(write_result));

    if (!_opts.skip_read) {
        std::vector<metrics> read_shard_results;
        read_shard_results.reserve(per_shard_results.size());
        for (auto& shard_result : per_shard_results) {
            if (shard_result.read.has_value()) {
                read_shard_results.push_back(
                  std::move(shard_result.read.value()));
            }
        }

        auto read_result
          = merge_metrics(std::move(read_shard_results)).to_st_result();
        read_result.name = _opts.name;
        read_result.info = fmt::format("read run (shards: {})", active_shards);
        read_result.test_type = "disk";
        if (_cancelled->abort_requested()) {
            read_result.warning = "Run was manually cancelled";
        }
        r.push_back(std::move(read_result));
    }

    co_return r;
}

} // namespace cluster::self_test
