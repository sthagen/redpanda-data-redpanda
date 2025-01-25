# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from requests.exceptions import HTTPError

from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest


class DatalakeCustomPartitioningConfigTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeCustomPartitioningConfigTest,
              self).__init__(test_ctx,
                             num_brokers=1,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={"iceberg_enabled": True},
                             *args,
                             **kwargs)

    @cluster(num_nodes=1)
    def test_configs(self):
        rpk = RpkTool(self.redpanda)
        try:
            rpk.create_topic("foo",
                             config={
                                 "redpanda.iceberg.mode":
                                 "value_schema_id_prefix",
                                 "redpanda.iceberg.partition.spec":
                                 "(hour(field"
                             })
        except RpkException:
            pass
        else:
            assert False, "creating topic with invalid spec should be forbidden"

        # should succeed
        rpk.create_topic("foo",
                         config={
                             "redpanda.iceberg.mode": "value_schema_id_prefix",
                             "redpanda.iceberg.partition.spec": "(hour(field))"
                         })

        try:
            rpk.alter_topic_config("foo", "redpanda.iceberg.partition.spec",
                                   "(unknown_transform(field))")
        except RpkException:
            pass
        else:
            assert False, "altering spec to invalid string should be forbidden"

        for spec in [
                "((unparseable", "(not_redpanda_field)", "(redpanda.offset)"
        ]:
            try:
                self.redpanda.set_cluster_config(
                    {"iceberg_default_partition_spec": spec})
            except HTTPError as e:
                if e.response.status_code != 400:
                    raise
            else:
                assert False, "setting default spec to invalid string should be forbidden"

        # should succeed
        self.redpanda.set_cluster_config(
            {"iceberg_default_partition_spec": "(day(redpanda.timestamp))"})

        # topic with default spec
        rpk.create_topic(
            "bar", config={"redpanda.iceberg.mode": "value_schema_id_prefix"})

        topic_configs = rpk.describe_topic_configs("bar")
        assert topic_configs["redpanda.iceberg.partition.spec"][0] == \
            "(day(redpanda.timestamp))"
