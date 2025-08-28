# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from enum import Enum

from ducktape.tests.test import TestContext

GLOBAL_CLOUD_PROVIDER = "cloud_provider"


class CloudProviderType(str, Enum):
    DOCKER = "docker"
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"

    def __str__(self):
        return self.value

    @staticmethod
    def from_context(test_context: TestContext) -> "CloudProviderType":
        cloud_provider = test_context.globals.get(GLOBAL_CLOUD_PROVIDER)
        if not cloud_provider:
            return CloudProviderType.DOCKER

        return CloudProviderType(cloud_provider)
