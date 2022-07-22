# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import re
import time

from ducktape.utils.util import wait_until
from rptest.utils.rpenv import sample_license
from rptest.services.admin import Admin
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings
from rptest.services.cluster import cluster
from requests.exceptions import HTTPError
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions


class UpgradeToLicenseChecks(RedpandaTest):
    """
    Test that ensures the licensing work does not incorrectly print license
    enforcement errors during upgrade when a guarded feature is already
    enabled. Also tests that the license can only be uploaded once the cluster
    has completed upgrade to the latest version.
    """
    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, test_context):
        # Setting 'si_settings' enables a licensed feature, however at v22.1.4 there
        # are no license checks present. This test verifies behavior between versions
        # of redpanda that do and do not have the licensing feature built-in.
        super(UpgradeToLicenseChecks, self).__init__(test_context=test_context,
                                                     num_brokers=3,
                                                     si_settings=SISettings())
        self.installer = self.redpanda._installer
        self.admin = Admin(self.redpanda)

    def setUp(self):
        self.installer.install(self.redpanda.nodes, (22, 1, 4))
        super(UpgradeToLicenseChecks, self).setUp()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_basic_upgrade(self):
        # Modified environment variables apply to processes restarted from this point onwards
        self.redpanda.set_environment({
            '__REDPANDA_LICENSE_CHECK_INTERVAL_SEC':
            f'{UpgradeToLicenseChecks.LICENSE_CHECK_INTERVAL_SEC}'
        })

        license = sample_license()
        if license is None:
            self.logger.info(
                "Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
            return

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert 'v22.1.4' in unique_versions, unique_versions

        # These logs can't exist in v22.1.4 but double check anyway...
        assert self.redpanda.search_log("Enterprise feature(s).*") is False

        # Update one node to newest version
        self.installer.install([self.redpanda.nodes[0]],
                               RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        unique_versions = wait_for_num_versions(self.redpanda, 2)

        try:
            # Ensure a valid license cannot be uploaded in this cluster state
            self.admin.put_license(license)
            assert False
        except HTTPError as e:
            assert e.response.status_code == 400

        # Ensure the log is not written, if the fiber was enabled a log should
        # appear within one interval of the license check fiber
        time.sleep(UpgradeToLicenseChecks.LICENSE_CHECK_INTERVAL_SEC * 2)
        assert self.redpanda.search_log("Enterprise feature(s).*") is False

        # Install new version on all nodes
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)

        # Restart nodes 2 and 3
        self.redpanda.restart_nodes(
            [self.redpanda.nodes[1], self.redpanda.nodes[2]])
        _ = wait_for_num_versions(self.redpanda, 1)

        # Assert that the log was found
        wait_until(
            lambda: self.redpanda.search_log("Enterprise feature(s).*"),
            timeout_sec=(UpgradeToLicenseChecks.LICENSE_CHECK_INTERVAL_SEC * 4)
            * len(self.redpanda.nodes),
            backoff_sec=1,
            err_msg="Timeout waiting for enterprise nag log")

        # Install license
        assert self.admin.put_license(license).status_code == 200
