# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import logging
import time

import requests
from ducktape.tests.test import TestContext
from requests.adapters import HTTPAdapter

from rptest.context.cloud_provider import CloudProviderType
from rptest.utils.expiring_value import ExpiringValue

GLOBAL_GCP_PROJECT_ID_KEY = "gcp_project_id"

logger = logging.getLogger(__name__)


class GCPContext:
    """
    Context for GCP-related operations. At this time it provides enough
    information for VM Metadata authentication/authorization. This is also the
    only method currently supported by Redpanda for Tiered Storage, Datalake,
    and Cloud Topics.

    - https://cloud.google.com/docs/authentication
    - https://cloud.google.com/compute/docs/authentication
    - https://cloud.google.com/compute/docs/access/authenticate-workloads#applications
    """

    def __init__(self, *, project_id: str):
        self.project_id = project_id
        self._gcp_token_cache = ExpiringValue[str]()

    def fetch_iam_token(self) -> str:
        token = self._gcp_token_cache.value()
        if token is not None:
            return token

        logger.info("Getting gcp iam token")
        s = requests.Session()
        s.mount("http://169.254.169.254", HTTPAdapter(max_retries=5))
        res = s.request(
            "GET",
            "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"},
        )
        res.raise_for_status()
        logger.info(f"Got gcp iam token expiring in {res.json()['expires_in']} seconds")
        # GCP guarantees that tokens are valid for at least 5 minutes so it is
        # safe to subtract 60 seconds and still assume a valid token.
        self._gcp_token_cache.update(
            res.json()["access_token"],
            expire_at=time.time() + res.json()["expires_in"] - 60,
        )
        return res.json()["access_token"]

    @staticmethod
    def available(test_context: TestContext) -> bool:
        # Cloud provider is always set and if it is GCP then we always should
        # be able to provide a context.
        return CloudProviderType.from_context(test_context) == CloudProviderType.GCP

    @staticmethod
    def from_context(test_context: TestContext) -> "GCPContext":
        if not GCPContext.available(test_context):
            raise ValueError("GCP context is not available")

        project_id = test_context.globals.get(GLOBAL_GCP_PROJECT_ID_KEY)
        if not project_id:
            raise ValueError(
                f"Missing {GLOBAL_GCP_PROJECT_ID_KEY} global in the test context"
            )

        return GCPContext(project_id=project_id)
