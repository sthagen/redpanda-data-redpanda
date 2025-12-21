/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/gc/level_zero_gc.h"

#include "base/vlog.h"
#include "cloud_io/remote.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/object_utils.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/topic_table.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

namespace cloud_topics {

class object_storage_remote_impl : public level_zero_gc::object_storage {
public:
    // TODO(noah) some random-but-not-awful values for the retry chain that
    // cloud io requires. will need to be fine tuned at some point.
    static constexpr std::chrono::seconds timeout{5};
    static constexpr std::chrono::seconds backoff{1};

    object_storage_remote_impl(
      cloud_io::remote* remote, cloud_storage_clients::bucket_name bucket)
      : remote_(remote)
      , bucket_(std::move(bucket)) {}

    seastar::future<std::expected<
      cloud_storage_clients::client::list_bucket_result,
      cloud_storage_clients::error_outcome>>
    list_objects(seastar::abort_source* asrc) override {
        retry_chain_node rtc(*asrc, timeout, backoff);
        auto res = co_await remote_->list_objects(
          bucket_, rtc, object_path_factory::level_zero_data_dir());
        if (res.has_value()) {
            co_return std::move(res).assume_value();
        }
        co_return std::unexpected(res.assume_error());
    }

    seastar::future<std::expected<void, cloud_io::upload_result>>
    delete_objects(
      seastar::abort_source* asrc,
      std::vector<cloud_storage_clients::client::list_bucket_item> objects)
      override {
        retry_chain_node rtc(*asrc, timeout, backoff);
        auto keys
          = objects | std::views::transform([](auto& obj) { return obj.key; })
            | std::ranges::to<std::vector<cloud_storage_clients::object_key>>();
        auto res = co_await remote_->delete_objects(
          bucket_, keys, rtc, [](auto) {});
        if (res == cloud_io::upload_result::success) {
            co_return std::expected<void, cloud_io::upload_result>();
        }
        co_return std::unexpected(res);
    }

private:
    cloud_io::remote* remote_;
    const cloud_storage_clients::bucket_name bucket_;
};

seastar::future<std::expected<std::optional<cluster_epoch>, std::string>>
level_zero_gc::epoch_source::max_gc_eligible_epoch(seastar::abort_source* as) {
    /*
     * First retrieve a consistent snapshot of cloud topic partitions. This
     * establishes a set of partitions from which we must obtain an epoch
     * bound on garbage collection.
     */
    auto partitions = co_await get_partitions(as);
    if (!partitions.has_value()) {
        co_return std::unexpected(partitions.error());
    }
    if (partitions.value().partitions.empty()) {
        co_return std::nullopt;
    }

    /*
     * Next we retrieve the latest reported epoch bounds from all cloud
     * topic partitions. The source for this information is distributed,
     * while the source for the `partitions` set above is centralized, and
     * this is why we have these two different collection steps.
     */
    auto gc_epochs = co_await get_partitions_max_gc_epoch(as);
    if (!gc_epochs.has_value()) {
        co_return std::unexpected(gc_epochs.error());
    }

    /*
     * The final result begins as the maximum epoch for the given snapshot.
     * Below we merge the two result sets and walk the final result back to
     * account for the partition with the smallest eligible gc epoch.
     */
    auto result = partitions.value().snap_revision;

    vlog(
      cd_log.debug,
      "Calculating max GC eligible epoch with snapshot epoch {}",
      result);

    for (const auto& partition : partitions.value().partitions) {
        const auto& tp_ns = partition.first;
        auto nit = gc_epochs.value().find(tp_ns);
        if (nit == gc_epochs.value().end()) {
            co_return std::unexpected(
              fmt::format(
                "Topic '{}' in snapshot has no reported max GC epoch", tp_ns));
        }

        for (const auto p_id : partition.second) {
            auto pit = nit->second.find(p_id);
            if (pit == nit->second.end()) {
                co_return std::unexpected(
                  fmt::format(
                    "Partition '{}/{}' in snapshot has no reported max GC "
                    "epoch",
                    tp_ns,
                    p_id));
            }

            // this partition may hold back the max GC eligible epoch
            const auto prev_result = result;
            result = std::min(result, pit->second);

            vlog(
              cd_log.debug,
              "Reducing result {} from min(result={}, p={}) for {}/{}",
              result,
              prev_result,
              pit->second,
              tp_ns,
              p_id);
        }
    }

    co_return result;
}

class epoch_source_impl : public level_zero_gc::epoch_source {
public:
    explicit epoch_source_impl(
      seastar::sharded<cluster::health_monitor_frontend>* health_monitor,
      seastar::sharded<cluster::controller_stm>* controller_stm,
      seastar::sharded<cluster::topic_table>* topic_table)
      : health_monitor_(health_monitor)
      , controller_stm_(controller_stm)
      , topic_table_(topic_table) {}

    seastar::future<std::expected<partitions_snapshot, std::string>>
    get_partitions(seastar::abort_source* as) override {
        const auto& topic_table = topic_table_->local();

        // this revision is for detecting concurrent modifications
        const auto iter_start_rev = topic_table.topics_map_revision();

        /*
         * The controller stm last applied offset is used, as opposed to using
         * the topic table last applied offset, because we need the version to
         * move forward. the controller stm offset is a at the top of the stm
         * hierarchy and is consistent with the topic table last applied offset.
         */
        partitions_snapshot snap;
        snap.snap_revision = cluster_epoch(
          co_await controller_stm_->invoke_on(
            cluster::controller_stm_shard, [](auto& stm) {
                return model::revision_id(stm.get_last_applied_offset());
            }));

        for (const auto& topic : topic_table.topics_map()) {
            // we only care about cloud topics
            if (!topic.second.get_metadata()
                   .get_configuration()
                   .is_cloud_topic()) {
                continue;
            }

            auto& partitions = snap.partitions[topic.first];
            for (const auto& partition : topic.second.partitions) {
                partitions.push_back(model::partition_id(partition.first));
            }

            co_await seastar::maybe_yield();

            if (as && as->abort_requested()) {
                co_return std::unexpected("Abort requested");
            }

            // Detect concurrent changes to the topic table to avoid accessing
            // an invalid iterator.
            try {
                topic_table.check_topics_map_stable(iter_start_rev);
            } catch (...) {
                // TODO: its rare, so should we retry immediately or abort this
                // round and wait for the next GC loop? i think it's a balance
                // of more code/complexity and behavior. For now I think it is
                // fine.
                co_return std::unexpected(
                  "Concurrent container iteration invalidation. Will retry");
            }
        }

        co_return snap;
    }

    seastar::future<std::expected<partitions_max_gc_epoch, std::string>>
    get_partitions_max_gc_epoch(seastar::abort_source* as) override {
        /*
         * Get a recent health report. Partitions use the health reporting
         * mechanism to self-report their max GC eligible epoch.
         */
        constexpr auto health_report_query_timeout = 10s;

        auto health_report
          = co_await health_monitor_->local().get_cluster_health(
            cluster::cluster_report_filter{},
            cluster::force_refresh::no,
            model::timeout_clock::now() + health_report_query_timeout);

        if (!health_report.has_value()) {
            co_return std::unexpected(
              fmt::format(
                "Error retrieving cluster health report: {}",
                health_report.error()));
        }

        partitions_max_gc_epoch result;
        for (const auto& node_health : health_report.value().node_reports) {
            for (const auto& topic_status : node_health->topics) {
                const auto& tp_ns = topic_status.first;
                for (const auto& partition_status : topic_status.second) {
                    /*
                     * calculate the max gc epoch for each partition. the catch
                     * here is that this value is reported through the health
                     * reporting system, and that system reports information for
                     * all partition replicas (leader and followers). so how do
                     * we know which value to use here? first, the max gc epoch
                     * only increases in value. second, we recognize that
                     * all reported values from any replica are valid at (and
                     * forever after) the moment they are reported. third, only
                     * the leader advances the epoch.
                     *
                     * because of the second point, using max gc epoch from any
                     * replica will result in correct behavior, however it may
                     * be pessimistic. using the one from the leader is better,
                     * but leadership is a lagging signal. instead, we can take
                     * the maximum reported as the most optimistic value.
                     *
                     * if a replica reports no epoch then it is considered to be
                     * in an indeterminite state and it has no affect on the
                     * computed result (effectively it is treated as having
                     * epoch 0 in the max reduction across replicas).
                     *
                     * if all replicas report no epoch then the partition is not
                     * included in the result set returned to the caller. this
                     * covers two cases.
                     *
                     * the first case is that the partition is part of a
                     * standard topic. in this case the partition will also not
                     * be in the set returned by `get_partitions` and thus the
                     * join in `max_gc_eligible_epoch` will ignore the topic.
                     *
                     * in the second case the join would fail, and later succeed
                     * in the once at least one replica is returning max gc
                     * epoch. this shouldn't be a problem in practice: there is
                     * a narrow window at start-up time where a partition is
                     * bootstrapping the L0 CT STM state where the state is
                     * unknown. for brand new partitions this should be the
                     * partition's creation revision ID.
                     */
                    const auto maybe_max_gc_epoch
                      = partition_status.second
                          .cloud_topic_max_gc_eligible_epoch;
                    if (!maybe_max_gc_epoch.has_value()) {
                        continue;
                    }
                    const auto max_gc_epoch = cluster_epoch(
                      maybe_max_gc_epoch.value());

                    const auto p_id = partition_status.first;
                    auto& partition_epochs = result[tp_ns];
                    const auto it = partition_epochs.find(p_id);
                    if (it == partition_epochs.end()) {
                        partition_epochs.try_emplace(p_id, max_gc_epoch);
                    } else {
                        it->second = std::max(it->second, max_gc_epoch);
                    }
                }
            }

            /*
             * A scheduling point is injected after looking at each node's
             * report. We own the list of node reports which is a set of shared
             * pointers, so iteration is safe, and the scheduling point is
             * intended to help avoid reactor stalls. If we need to inject
             * scheduling points at a finer granularity we'll need to take a
             * closer look at concurrency rules of the reports themselves.
             */
            co_await seastar::maybe_yield();

            if (as && as->abort_requested()) {
                co_return std::unexpected("Abort requested");
            }
        }

        co_return result;
    }

private:
    seastar::sharded<cluster::health_monitor_frontend>* health_monitor_;
    seastar::sharded<cluster::controller_stm>* controller_stm_;
    seastar::sharded<cluster::topic_table>* topic_table_;
};

level_zero_gc::level_zero_gc(
  level_zero_gc_config config,
  std::unique_ptr<object_storage> storage,
  std::unique_ptr<epoch_source> epoch_source)
  : config_(std::move(config))
  , storage_(std::move(storage))
  , epoch_source_(std::move(epoch_source))
  , should_run_(false) // begin in a stopped state
  , should_shutdown_(false)
  , worker_(worker())
  , probe_(config::shard_local_cfg().disable_metrics()) {}

level_zero_gc::level_zero_gc(
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  seastar::sharded<cluster::health_monitor_frontend>* health_monitor,
  seastar::sharded<cluster::controller_stm>* controller_stm,
  seastar::sharded<cluster::topic_table>* topic_table)
  : level_zero_gc(
      level_zero_gc_config{
        .deletion_grace_period
        = config::shard_local_cfg()
            .cloud_topics_short_term_gc_minimum_object_age.bind(),
        .throttle_progress
        = config::shard_local_cfg().cloud_topics_short_term_gc_interval.bind(),
        .throttle_no_progress
        = config::shard_local_cfg()
            .cloud_topics_short_term_gc_backoff_interval.bind(),
      },
      std::make_unique<object_storage_remote_impl>(remote, std::move(bucket)),
      std::make_unique<epoch_source_impl>(
        health_monitor, controller_stm, topic_table)) {}

void level_zero_gc::start() {
    vlog(cd_log.info, "Starting cloud topics L0 GC worker");
    should_run_ = true;
    worker_cv_.signal();
}

void level_zero_gc::pause() {
    vlog(cd_log.info, "Pausing cloud topics L0 GC worker");
    should_run_ = false;
    asrc_.request_abort();
}

seastar::future<> level_zero_gc::stop() {
    vlog(cd_log.info, "Stopping cloud topics L0 GC worker");
    should_shutdown_ = true;
    asrc_.request_abort();
    worker_cv_.signal();
    co_await std::exchange(worker_, seastar::make_ready_future<>());
}

// internal error codes used between the worker fiber and the main GC function
enum class level_zero_gc::collection_error : int8_t {
    // problem occurred interacting with the storage or epoch services
    service_error,
    // the cluster is reporting that no collectible epoch exists
    no_collectible_epoch,
    // object listing contained an invalid object name
    invalid_object_name,
};

seastar::future<> level_zero_gc::worker() {
    std::chrono::milliseconds backoff{0};

    while (true) {
        try {
            co_await worker_cv_.wait(
              [this] { return should_run_ || should_shutdown_; });

            if (should_shutdown_) {
                break;
            }

            // stop() and shutdown() may request an abort, but only the worker
            // may subscribe or reset the abort source since it is able to
            // ensure that the abort source is unreferenced at this time.
            asrc_ = {};

            if (backoff.count() > 0) {
                (co_await seastar::coroutine::as_future(
                   seastar::sleep_abortable(backoff, asrc_)))
                  .ignore_ready_future();
                backoff = std::chrono::seconds{0};
            }

            auto res = co_await try_to_collect();
            if (res.has_value()) {
                if (res.value() > 0) {
                    backoff = config_.throttle_progress();
                } else {
                    backoff = config_.throttle_no_progress();
                }
            } else {
                switch (res.error()) {
                case collection_error::service_error:
                case collection_error::invalid_object_name:
                case collection_error::no_collectible_epoch:
                    backoff = config_.throttle_no_progress();
                }
            }

        } catch (...) {
            vlog(
              cd_log.info,
              "Level zero GC restarting after error: {}",
              std::current_exception());
            backoff = config_.throttle_no_progress();
        }
    }

    vlog(cd_log.info, "Level zero GC worker is exiting");
}

seastar::future<std::expected<size_t, level_zero_gc::collection_error>>
level_zero_gc::try_to_collect() {
    const auto candidate_objects = co_await storage_->list_objects(&asrc_);
    if (!candidate_objects.has_value()) {
        vlog(
          cd_log.debug,
          "Received error listing objects during L0 GC: {}",
          candidate_objects.error());
        co_return std::unexpected(collection_error::service_error);
    }

    const auto maybe_max_gc_epoch
      = co_await epoch_source_->max_gc_eligible_epoch(&asrc_);
    if (!maybe_max_gc_epoch.has_value()) {
        vlog(
          cd_log.debug,
          "Received error retrieving GC eligible epoch: {}",
          maybe_max_gc_epoch.error());
        co_return std::unexpected(collection_error::service_error);
    }

    const auto max_gc_epoch = maybe_max_gc_epoch.value();
    if (!max_gc_epoch.has_value()) {
        vlog(cd_log.info, "No GC eligible epoch currently exists");
        co_return std::unexpected(collection_error::no_collectible_epoch);
    }

    const auto max_gc_birthday = std::chrono::system_clock::now()
                                 - config_.deletion_grace_period();

    vlog(
      cd_log.debug,
      "Attempting L0 GC at epoch {} and last modified limit {}",
      max_gc_epoch.value(),
      max_gc_birthday);

    // objects that can be safely deleted
    std::vector<cloud_storage_clients::client::list_bucket_item>
      eligible_objects;

    // used to detect unsorted object listings
    seastar::sstring last_key;
    std::optional<cluster_epoch> last_epoch;

    for (const auto& object : candidate_objects.value().contents) {
        const auto object_epoch = object_path_factory::level_zero_path_to_epoch(
          object.key);

        // validate expected L0 object name format, and extract epoch
        if (!object_epoch.has_value()) {
            vlog(
              cd_log.error,
              "Unable to parse epoch during L0 GC: {}",
              object_epoch.error());
            co_return std::unexpected(collection_error::invalid_object_name);
        }

        // detect non-lexicographic ordering. this may indicate that GC will not
        // operate efficiently with the underlying storage system. see the class
        // comment for more details about what this means in practice.
        if (!last_epoch.has_value()) {
            last_key = object.key;
            last_epoch = object_epoch.value();
        }

        if (object_epoch.value() < last_epoch) {
            constexpr std::chrono::minutes rate_limit{1};
            static seastar::logger::rate_limit rate(rate_limit);
            vloglr(
              cd_log,
              seastar::log_level::error,
              rate,
              "Non-lexicographic object listing detected during L0 GC {} < {}",
              object.key,
              last_key);
        }

        last_key = object.key;
        last_epoch = object_epoch.value();

        // object's epoch is not yet eligible
        if (object_epoch.value() > max_gc_epoch.value()) {
            vlog(
              cd_log.debug,
              "Ignoring object with non-collectible epoch: {} > {}",
              object.key,
              max_gc_epoch.value());
            continue;
        }

        // object is too young
        if (object.last_modified > max_gc_birthday) {
            vlog(
              cd_log.debug,
              "Ignoring object with too recent creation time: {} @ {} < {}",
              object.key,
              object.last_modified,
              max_gc_birthday);
            continue;
        }

        eligible_objects.push_back(object);
    }

    const auto num_eligible = eligible_objects.size();

    auto res = co_await storage_->delete_objects(
      &asrc_, std::move(eligible_objects));
    if (!res.has_value()) {
        vlog(
          cd_log.info,
          "Received an error deleting L0 data objects: {}",
          res.error());
        co_return std::unexpected(collection_error::service_error);
    }

    probe_.objects_deleted(num_eligible);

    vlog(
      cd_log.debug, "Deleted {} L0 data objects eligible for GC", num_eligible);

    co_return num_eligible;
}

} // namespace cloud_topics
