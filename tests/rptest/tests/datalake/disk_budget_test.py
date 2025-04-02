# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import time
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from ducktape.utils.util import wait_until
import ducktape.errors

from ducktape.mark import matrix


class DatalakeDiskUsageTest(RedpandaTest):
    """
    In this test we are going to configure redpanda initially to allow a large
    amount of data to accumulate in the datalake staging directory.

    After the data has accumulated we'll use published metrics to observe that
    the size is not decreasing which we use to infer that none of the existing
    triggers is forcing translation to finish (lag, flush bytes, disk space).

    Finally we'll lower the target disk usage and expect that space usage
    declines indicating that it was the target disk space usage monitor that was
    controlling the usage.
    """
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeDiskUsageTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": True,
                "datalake_disk_space_monitor_enable": True,
                # for reactivity
                "datalake_scheduler_time_slice_ms": 5000,
                "datalake_disk_space_monitor_interval": 5000,
                # let translator accumulate staging data
                "datalake_translator_flush_bytes": 16 * 2**30,
                "iceberg_target_lag_ms": 10 * 60 * 1000,
                "datalake_scratch_space_size_bytes": 60 * 2**30,
            },
            *args,
            **kwargs)

        self.test_ctx = test_ctx
        self.topic_name = "test"

    def datalake_staging_usage(self):
        # returns number of bytes in datalake staging directory
        metric_name = "vectorized_space_management_datalake_disk_usage_bytes"
        return self.redpanda.metric_sum(metric_name, expect_metric=True)

    def create_topic(self, num_partitions):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(self.topic_name,
                         partitions=num_partitions,
                         replicas=1,
                         config={TopicSpec.PROPERTY_ICEBERG_MODE: "key_value"})

    def produce_until_staging_size(self, target_size):
        # produce some data to the topic and then back off and let datalake do
        # its thang. after that check back in with the broker and if we haven't
        # translated enough data then try again.
        timeout_sec = 120
        start_time = time.time()
        current_size = 0
        while current_size < target_size:
            producer = RpkProducer(
                self.test_ctx,
                self.redpanda,
                self.topic_name,
                2**13,
                2**14,  # ~128mb
                acks=-1)
            producer.start()
            producer.wait()
            producer.free()
            time.sleep(2)
            current_size = self.datalake_staging_usage()
            self.logger.info(f"Staging data usage {current_size}")
            assert (
                time.time() -
                start_time) < timeout_sec, f"{current_size} < {target_size}"
        return current_size

    @cluster(num_nodes=2)
    @skip_debug_mode
    @matrix(num_partitions=[1, 2, 30],
            concurrent_translations=[1, 4],
            cloud_storage_type=supported_storage_types())
    def test_idle_finish(self, num_partitions, concurrent_translations,
                         cloud_storage_type):
        self.redpanda.set_cluster_config({
            "datalake_scheduler_max_concurrent_translations":
            concurrent_translations,
        })

        # produce data until we have a nice bit of datalake staging data on disk
        target_size = 2 * 2**30
        self.create_topic(num_partitions)
        idle_staging_size = self.produce_until_staging_size(target_size)

        # based on the configuration (see __init__) we expect that the staging
        # data is not finished and uploaded. so let's sanity check that.
        try:
            wait_until(
                lambda: self.datalake_staging_usage() < idle_staging_size,
                timeout_sec=30,
                backoff_sec=2)
            assert False, f"{self.datalake_staging_usage()} < {idle_staging_size}"
        except ducktape.errors.TimeoutError:
            # success, the data usage didn't shrink
            pass

        # now we will halve the target size and we expect to see that the
        # staging directory usage decreases below this value.
        new_target_size = target_size // 2
        self.redpanda.set_cluster_config({
            "datalake_scratch_space_size_bytes":
            new_target_size,
        })
        wait_until(lambda: self.datalake_staging_usage() <= new_target_size,
                   timeout_sec=60,
                   backoff_sec=2)
