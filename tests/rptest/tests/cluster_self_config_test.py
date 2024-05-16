# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import re

from ducktape.mark import parametrize, matrix

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import CloudStorageType, SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.utils import LogSearchLocal


class ClusterSelfConfigTest(RedpandaTest):
    def __init__(self, ctx):
        si_settings = SISettings(
            ctx,
            #Force self configuration through setting cloud_storage_url_style to None.
            cloud_storage_url_style=None,
            cloud_storage_enable_remote_read=ctx.
            injected_args["cloud_storage_enable_remote_read"],
            cloud_storage_enable_remote_write=ctx.
            injected_args["cloud_storage_enable_remote_write"])

        super().__init__(ctx, si_settings=si_settings)

        self.log_searcher = LogSearchLocal(ctx, [], self.redpanda.logger,
                                           self.redpanda.STDOUT_STDERR_CAPTURE)
        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=1)
    @matrix(cloud_storage_enable_remote_read=[True, False],
            cloud_storage_enable_remote_write=[True, False])
    def test_s3_self_config(self, cloud_storage_enable_remote_read,
                            cloud_storage_enable_remote_write):
        """
        Verify that cloud_storage_url_style self configuration occurs for the s3_client
        when it is not specified. There aren't any endpoints for testing this, so
        it will be manually checked for from the logs.
        """

        config = self.admin.get_cluster_config()

        # Even after self-configuring, the cloud_storage_url_style setting will
        # still be left unset at the cluster config level.
        assert config['cloud_storage_url_style'] is None

        def str_in_logs(node, s):
            return any(s in log.strip()
                       for log in self.log_searcher._capture_log(node, s))

        def self_config_start_in_logs(node):
            client_self_configuration_start_string = 'Client requires self configuration step'
            return str_in_logs(node, client_self_configuration_start_string)

        def self_config_default_in_logs(node):
            client_self_configuration_default_string = 'Could not self-configure S3 Client'
            return str_in_logs(node, client_self_configuration_default_string)

        def self_config_result_from_logs(node):
            client_self_configuration_complete_string = 'Client self configuration completed with result'
            for log in self.log_searcher._capture_log(
                    node, client_self_configuration_complete_string):
                m = re.search(
                    client_self_configuration_complete_string + r' (\{.*\})',
                    log.strip())
                if m:
                    return m.group(1)
            return None

        for node in self.redpanda.nodes:
            #Assert that self configuration started.
            assert self_config_start_in_logs(node)

            #If neither remote_read or remote_write are enabled, check for the "defaulting" output
            if not cloud_storage_enable_remote_read and not cloud_storage_enable_remote_write:
                assert self_config_default_in_logs(node)

            #Assert that self configuration returned a result.
            self_config_result = self_config_result_from_logs(node)

            #Currently, virtual_host will succeed in all cases with MinIO.
            self_config_expected_results = [
                '{s3_self_configuration_result: {s3_url_style: virtual_host}}',
                '{s3_self_configuration_result: {s3_url_style: path}}'
            ]

            assert self_config_result and self_config_result in self_config_expected_results
