/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/frontend_reader/level_zero_reader.h"

#include "base/vassert.h"
#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/state_accessors.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition.h"
#include "config/configuration.h"
#include "model/timeout_clock.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <chrono>
#include <exception>
#include <iterator>
#include <utility>
#include <variant>

namespace cloud_topics {
namespace {

/// Thrown when the topic config is no longer available, e.g. because the
/// topic has been deleted from the topic_table but the partition has not
/// yet been shut down. Caught in do_load_slice to end the stream
/// gracefully instead of propagating as a hard error.
struct topic_config_not_found final : std::runtime_error {
    using std::runtime_error::runtime_error;
};

model::topic_id_partition
get_topic_id_partition(const ss::lw_shared_ptr<cluster::partition>& partition) {
    const auto& ntp = partition->ntp();
    auto ct_state = partition->get_cloud_topics_state();
    auto metadata_cache = ct_state->local().get_metadata_cache();
    auto topic_cfg = metadata_cache->get_topic_cfg(
      model::topic_namespace_view(ntp));
    if (!topic_cfg) {
        throw topic_config_not_found(
          fmt::format("no config found for cloud topic {}", ntp));
    }
    return model::topic_id_partition{
      topic_cfg->tp_id.value(), ntp.tp.partition};
}

} // namespace

level_zero_log_reader_impl::level_zero_log_reader_impl(
  const cloud_topic_log_reader_config& cfg,
  ss::lw_shared_ptr<cluster::partition> ctp,
  data_plane_api* ct_api)
  : _config(cfg)
  , _next_offset(_config.start_offset)
  , _ctp(std::move(ctp))
  , _ct_api(ct_api)
  , _log(cd_log, fmt::format("[{}/{}]", fmt::ptr(this), _ctp->ntp())) {
    // Cap the reader's max_bytes at the read pipeline's memory quota to prevent
    // a single materialize call from exceeding what the pipeline can handle.
    _config.max_bytes = std::min(
      _config.max_bytes, _ct_api->materialize_max_bytes());
}

ss::future<model::record_batch_reader::storage_t>
level_zero_log_reader_impl::do_load_slice(
  model::timeout_clock::time_point deadline) {
    try {
        return read_some(deadline);
    } catch (const topic_config_not_found& e) {
        vlog(_log.debug, "Reader ending stream: {}", e.what());
        set_end_of_stream();
        return ss::make_ready_future<model::record_batch_reader::storage_t>();
    } catch (...) {
        vlog(
          _log.error, "Reader caught exception: {}", std::current_exception());
        set_end_of_stream();
        throw;
    }
}

ss::future<model::record_batch_reader::storage_t>
level_zero_log_reader_impl::read_some(
  model::timeout_clock::time_point deadline) {
    if (_next_offset > _config.max_offset) {
        vlog(
          _log.debug,
          "reached end of stream, start offset: {}, max offset: {}, "
          "next offset: {}",
          _config.start_offset,
          _config.max_offset,
          _next_offset);
        set_end_of_stream();
        co_return chunked_circular_buffer<model::record_batch>{};
    }

    // Wait briefly for the write path to cache the batch we need.
    // This closes the race window between replicate() completing (HWM
    // advance) and cache_put() running on the write continuation.
    if (cache_enabled() && _ctp->is_leader()) {
        auto tidp = get_topic_id_partition(_ctp);
        auto wait_deadline = model::timeout_clock::now()
                             + std::chrono::milliseconds(25);
        try {
            // Translate committed_offset from raft-space to kafka-space.
            // The batch cache monitor tracks kafka offsets (put() notifies
            // with kafka offsets), so the seed must also be in kafka-space.
            // Using a raft offset here would over-seed the monitor by the
            // offset delta, causing the wait to resolve immediately.
            auto ot_state = _ctp->get_offset_translator_state();
            auto committed_kafka = ot_state->from_log_offset(
              _ctp->raft()->committed_offset());
            co_await _ct_api->cache_wait(
              tidp,
              kafka::offset_cast(_next_offset),
              model::prev_offset(committed_kafka),
              wait_deadline,
              _config.abort_source);
        } catch (const ss::timed_out_error&) {
            // Timeout is expected for non-tailing reads where no
            // concurrent writer is caching data. Fall through to the
            // normal fetch-and-materialize path.
        }
    }

    // We're only fetching from the record batch cache if the reader is in
    // the 'empty' state. It doesn't make any difference if the reader is in
    // the 'materialized' state. If we're in 'ready' state we risk to go out
    // of sync with cached metadata so it's safer to hydrate.
    if (auto cached = maybe_read_batches_from_cache(); !cached.empty()) {
        co_return cached;
    }

    /*
     * Read metadata (raft batches, placeholders, etc...) from the underlying
     * cloud topics partition. In the case of placeholders, the payloads are
     * missing, and will be handled below.
     */
    auto log_read_cfg = ctp_read_config();
    auto log_read_metadata = co_await fetch_metadata(log_read_cfg, deadline);
    if (log_read_metadata.empty()) {
        vlog(
          _log.debug,
          "No L0 meta batches fetched from the underlying partition, "
          "start offset: {}, max offset: {}",
          log_read_cfg.start_offset,
          log_read_cfg.max_offset);
        set_end_of_stream();
        co_return model::record_batch_reader::storage_t{};
    }

    vlog(
      _log.debug,
      "Fetched {} L0 meta batches from the underlying "
      "partition, first offset: {}, last offset: {}",
      log_read_metadata.size(),
      log_read_metadata.front().header.base_offset,
      log_read_metadata.back().header.last_offset());

    /*
     * Combine metadata with payloads (from cache or object storage) to
     * construct the full batches expected by the caller (e.g. Kafka Fetch).
     */
    auto maybe_batches = co_await materialize_batches(
      std::move(log_read_metadata), deadline);
    if (!maybe_batches.has_value()) {
        if (maybe_batches.error() == errc::timeout) {
            if (_bytes_consumed >= _config.min_bytes) {
                co_return model::record_batch_reader::storage_t{};
            }
            throw ss::timed_out_error();
        }
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Reader experienced unhandled error: {}",
          maybe_batches.error()));
    }

    auto batches = std::move(maybe_batches).value();
    if (batches.empty()) {
        set_end_of_stream();
        co_return model::record_batch_reader::storage_t{};
    }

    vlog(_log.debug, "consuming {} materialized batches", batches.size());
    _next_offset = model::offset_cast(
      model::next_offset(batches.back().last_offset()));

    co_return batches;
}

chunked_circular_buffer<model::record_batch>
level_zero_log_reader_impl::maybe_read_batches_from_cache() {
    chunked_circular_buffer<model::record_batch> ret;
    if (!cache_enabled()) {
        return ret;
    }

    auto tidp = get_topic_id_partition(_ctp);

    /*
     * Fetch batches from the cache starting at `_next_offset` until we hit a
     * gap or a control batch and must then fetch the data from object storage.
     */
    while (_next_offset <= _config.max_offset) {
        auto batch = _ct_api->cache_get(tidp, kafka::offset_cast(_next_offset));
        if (!batch.has_value()) {
            break;
        }

        vlog(
          _log.trace,
          "Loaded batch from cache for {}: {} @ term {}",
          _next_offset,
          batch.value().base_offset(),
          batch.value().term());

        auto batch_size = batch.value().size_bytes();
        if (is_over_limit_with_bytes(batch_size)) {
            set_end_of_stream();
            break;
        }
        _bytes_consumed += batch_size;

        ret.push_back(std::move(batch.value()));
        _next_offset = model::offset_cast(
          model::next_offset(ret.back().last_offset()));
    }

    return ret;
}

storage::local_log_reader_config
level_zero_log_reader_impl::ctp_read_config() const {
    /*
     * The requested offset range in the cloud topic reader configuration are
     * specified as offsets in the kafka address space and need to first be
     * converted into physical log offsets for log reader configuration.
     */
    auto ot_state = _ctp->get_offset_translator_state();
    auto start_offset = ot_state->to_log_offset(
      kafka::offset_cast(_next_offset));
    auto max_offset = ot_state->to_log_offset(
      kafka::offset_cast(_config.max_offset));

    /*
     * Used to set max_bytes on the log reader for the L0 CTP. Placeholder
     * batches in the CTP lack a payload, so a few small batches may materialize
     * into a large read. For example, a placeholder batch is roughly 110 bytes.
     * If the maximum read size is set to 4K, then ~40 batches will be returned
     * per read. If each materialized batch is 1 MB then the reader will be able
     * to stream 40 MB without performing another read from the CTP. The default
     * Kafka fetch size is 64 MB, so it is useful to read more than 4K.
     *
     * Default to the size of the segment reader buffer (32 KiB at the time of
     * writing). This is a small size used across all existing workloads, and
     * for cloud topics it provides plenty of metadata to drive large scans when
     * materialized batches are big.
     */
    const auto ctp_reader_max_bytes
      = storage::local_log_reader_config::segment_reader_max_buffer_size;

    storage::local_log_reader_config cfg(
      start_offset,
      max_offset,
      ctp_reader_max_bytes,
      // We need to fetch both raft data batches for transaction control
      // markers as well as placeholder batches to hydrate from object
      // storage, so we don't include a typefilter and instead postfilter
      // here.
      /*type_filter=*/std::nullopt,
      _config.first_timestamp,
      _config.abort_source,
      _config.client_address);

    // The cloud topics reader (user of this log reader) operates in kafka
    // offset space so this automatic translation saves a few steps.
    cfg.translate_offsets = model::translate_offsets::yes;

    return cfg;
}

ss::future<chunked_circular_buffer<level_zero_log_reader_impl::local_log_batch>>
level_zero_log_reader_impl::fetch_metadata(
  storage::local_log_reader_config cfg,
  model::timeout_clock::time_point deadline) const {
    chunked_circular_buffer<local_log_batch> ret;
    auto reader = co_await _ctp->make_local_reader(cfg);
    auto batches = std::move(reader).generator(deadline);

    // Convert L0 meta batches to extent_meta structures.
    while (auto maybe_batch = co_await batches()) {
        auto batch = std::move(maybe_batch->get());
        auto& header = batch.header();
        if (header.type == model::record_batch_type::raft_data) {
            local_log_batch local_batch{.header = header};
            local_batch.data = std::move(batch).release_data();
            ret.push_back(std::move(local_batch));
            continue;
        }
        if (header.type != model::record_batch_type::ctp_placeholder) {
            continue;
        }
        cloud_topics::extent_meta e{
          .base_offset = model::offset_cast(batch.base_offset()),
          .last_offset = model::offset_cast(batch.last_offset()),
        };
        auto placeholder = parse_placeholder_batch(std::move(batch));
        e.id = placeholder.id;
        e.first_byte_offset = placeholder.offset;
        e.byte_range_size = placeholder.size_bytes;
        ret.push_back(local_log_batch{.header = header, .data = e});
    }

    co_return ret;
}

ss::future<std::expected<chunked_circular_buffer<model::record_batch>, errc>>
level_zero_log_reader_impl::materialize_batches(
  chunked_circular_buffer<local_log_batch> unhydrated,
  model::timeout_clock::time_point deadline) {
    auto tidp = get_topic_id_partition(_ctp);
    // Cherry-pick enough L0 meta batches to materialize.
    chunked_vector<cloud_topics::extent_meta> to_materialize;
    auto unhydrated_it = unhydrated.begin();
    size_t materialize_bytes = 0;
    for (; unhydrated_it != unhydrated.end(); ++unhydrated_it) {
        size_t hydrated_batch_size = ss::visit(
          unhydrated_it->data,
          [](const local_log_batch::payload& payload) {
              return payload.size_bytes();
          },
          [](const cloud_topics::extent_meta& meta) {
              return meta.byte_range_size();
          },
          [](const local_log_batch::cached_batch&) -> size_t {
              vunreachable("Unexpected cached_batch state");
          });
        if (is_over_limit_with_bytes(hydrated_batch_size)) {
            // If the next meta batch exceeds the max bytes limit, we stop
            // materializing. The only exception is if we didn't collect any
            // batches yet, in which case we still materialize the next
            // batch. This could happen if the first meta batch is larger
            // than the max bytes limit (oversized batch or too small
            // limit). In this case we don't want to stall the reader
            // completely.
            vlog(
              _log.trace,
              "Materialize batches overshot at {} bytes, config: {}, last "
              "hydrated batch size: {}",
              materialize_bytes,
              _config,
              hydrated_batch_size);
            set_end_of_stream();
            break;
        }
        _bytes_consumed += hydrated_batch_size;
        if (
          auto* meta = std::get_if<cloud_topics::extent_meta>(
            &unhydrated_it->data)) {
            // Try the batch cache before scheduling an S3 download.
            // This prevents gap amplification where a single evicted
            // cache entry causes all subsequent batches in this read
            // to bypass the cache.
            if (cache_enabled()) {
                auto cached = _ct_api->cache_get(
                  tidp, kafka::offset_cast(meta->base_offset));
                if (cached.has_value()) {
                    vlog(
                      _log.trace,
                      "Cache hit for extent at offset {} during "
                      "materialize",
                      meta->base_offset);
                    unhydrated_it->data = local_log_batch::cached_batch{
                      .batch = std::move(cached.value())};
                    continue;
                }
            }
            materialize_bytes += meta->byte_range_size;
            to_materialize.push_back(*meta);
            vlog(
              _log.trace, "Materialize {} bytes total...", materialize_bytes);
        }
    }
    size_t materialize_count = to_materialize.size();
    // Only invoke the data plane when there are extents that missed the
    // batch cache. When every extent was a cache hit the vector is empty
    // and we can skip the (potentially expensive) S3 round-trip entirely.
    chunked_vector<model::record_batch> batches;
    if (!to_materialize.empty()) {
        vlog(
          _log.trace,
          "Invoking 'materialize' for {}: {} bytes, {} batches to "
          "materialize",
          _ctp->ntp(),
          materialize_bytes,
          materialize_count);
        // Ask data layer to bring data from the cloud storage.
        auto mat_res = co_await _ct_api->materialize(
          _ctp->ntp(),
          materialize_bytes,
          std::move(to_materialize),
          deadline,
          _config.abort_source);
        if (!mat_res.has_value()) {
            if (mat_res.error() == errc::shutting_down) {
                vlog(_log.debug, "Materialize aborted due to shutdown");
                throw ss::abort_requested_exception();
            }
            if (mat_res.error() == errc::timeout) {
                vlog(_log.debug, "Materialize aborted due to timeout");
                co_return std::unexpected(errc::timeout);
            }
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to materialize batches from the cloud storage: {}",
              mat_res.error().message()));
        }
        batches = std::move(mat_res.value());
        if (batches.size() != materialize_count) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Materialized unexpected number of batches: {}, expected: {}",
              batches.size(),
              materialize_count));
        }
    }
    // Merge our selected subset of unhydrated batches with the materialized
    // batches, preserving control batches from the local log.
    auto batches_it = batches.begin();
    chunked_circular_buffer<model::record_batch> hydrated;
    auto range_to_materialize = std::ranges::subrange(
      unhydrated.begin(), unhydrated_it);
    for (local_log_batch& local_batch : range_to_materialize) {
        if (_config.abort_source.has_value()) {
            _config.abort_source.value().get().check();
        }
        auto& local_batch_header = local_batch.header;
        model::record_batch batch = ss::visit(
          local_batch.data,
          [this, &local_batch_header, &batches_it, &tidp](
            const cloud_topics::extent_meta&) {
              model::record_batch batch = apply_placeholder_to_batch(
                local_batch_header, std::move(*batches_it));
              ++batches_it;
              // Propagate materialized batches to the record batch cache
              if (cache_enabled()) {
                  vlog(
                    _log.trace,
                    "Putting batch for {} to cache: {}, term: {}",
                    _ctp->ntp(),
                    batch.base_offset(),
                    batch.term());
                  _ct_api->cache_put(tidp, batch);
              }
              return batch;
          },
          [&local_batch_header](local_log_batch::payload& payload) {
              return model::record_batch(
                local_batch_header,
                std::move(payload),
                model::record_batch::tag_ctor_ng{});
          },
          [](local_log_batch::cached_batch& cb) {
              // Cache hit resolved during the collection loop above.
              // The batch is already fully formed (apply_placeholder_to_batch
              // was applied before cache_put on the path that populated it).
              return std::move(cb.batch);
          });
        hydrated.push_back(std::move(batch));
        co_await ss::coroutine::maybe_yield();
    }
    vassert(
      batches_it == batches.end(), "All materialized batches should be used");
    vlog(
      _log.debug,
      "Materialized {} batches from the L0 meta batches",
      hydrated.size());
    co_return hydrated;
}

bool level_zero_log_reader_impl::cache_enabled() const {
    if (_config.skip_cache) {
        return false;
    }
    if (!_ctp->log()->config().cache_enabled()) {
        return false;
    }
    if (config::shard_local_cfg().disable_batch_cache()) {
        return false;
    }
    return true;
}

void level_zero_log_reader_impl::print(std::ostream& o) {
    o << "cloud_topics_reader";
}

void level_zero_log_reader_impl::set_end_of_stream() { _end_of_stream = true; }

bool level_zero_log_reader_impl::is_end_of_stream() const {
    return _end_of_stream;
}

bool level_zero_log_reader_impl::is_over_limit_with_bytes(size_t size) const {
    return (_config.strict_max_bytes || _bytes_consumed > 0)
           && (_bytes_consumed + size) > _config.max_bytes;
}
} // namespace cloud_topics
