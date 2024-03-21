# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError
import json

from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.services.admin import (Admin, RoleMemberList, RoleUpdate,
                                   RoleErrorCode, RoleError, RolesList,
                                   RoleMemberUpdateResponse, RoleMember)
from rptest.services.redpanda import SaslCredentials, SecurityConfig
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.admin_api_auth_test import create_user_and_wait
from rptest.tests.metrics_reporter_test import MetricsReporterServer
from rptest.util import expect_exception, expect_http_error, wait_until_result

ALICE = SaslCredentials("alice", "itsMeH0nest", "SCRAM-SHA-256")


def expect_role_error(status_code: RoleErrorCode):
    return expect_exception(
        HTTPError, lambda e: RoleError.from_http_error(e).code == status_code)


class RBACTestBase(RedpandaTest):
    password = "password"
    algorithm = "SCRAM-SHA-256"
    role_name0 = 'foo'
    role_name1 = 'bar'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.rpk = RpkTool(self.redpanda)
        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.superuser_admin = Admin(self.redpanda,
                                     auth=(self.superuser.username,
                                           self.superuser.password))
        self.user_admin = Admin(self.redpanda,
                                auth=(ALICE.username, ALICE.password))

    def setUp(self):
        super().setUp()
        create_user_and_wait(self.redpanda, self.superuser_admin, ALICE)

        self.redpanda.set_cluster_config({'admin_api_require_auth': True})


class RBACTest(RBACTestBase):
    @cluster(num_nodes=3)
    def test_superuser_access(self):
        # a superuser may access the RBAC API
        res = self.superuser_admin.list_roles()
        assert len(RolesList.from_response(res)) == 0, "Unexpected roles"

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.get_role(role=self.role_name0)

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.update_role(role=self.role_name0,
                                             update=RoleUpdate(
                                                 self.role_name1))

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.update_role(role=self.role_name0,
                                             update=RoleUpdate(
                                                 self.role_name1))

        res = self.superuser_admin.list_roles(filter='ba',
                                              principal=ALICE.username)
        assert len(RolesList.from_response(res)) == 0, "Unexpected roles"

        res = self.superuser_admin.list_user_roles()
        assert len(RolesList.from_response(res)) == 0, "Unexpected user roles"

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.delete_role(role=self.role_name1)

    @cluster(num_nodes=3)
    def test_regular_user_access(self):
        # a regular user may NOT access the RBAC API

        with expect_http_error(403):
            self.user_admin.list_roles()

        with expect_http_error(403):
            self.user_admin.create_role(role=self.role_name0)

        with expect_http_error(403):
            self.user_admin.get_role(role=self.role_name0)

        with expect_http_error(403):
            self.user_admin.update_role(role=self.role_name0,
                                        update=RoleUpdate(self.role_name1))

        with expect_http_error(403):
            self.user_admin.update_role_members(
                role=self.role_name1,
                add=[
                    RoleMember(RoleMember.PrincipalType.USER, ALICE.username)
                ])

        with expect_http_error(403):
            self.user_admin.list_role_members(role=self.role_name1)

        res = self.user_admin.list_user_roles()
        assert len(
            RolesList.from_response(res)) == 0, "Unexpected roles for user"

        with expect_http_error(403):
            self.user_admin.delete_role(role=self.role_name1)

    @cluster(num_nodes=3)
    def test_create_role(self):
        res = self.superuser_admin.create_role(role=self.role_name0)
        created_role = res.json()['role']
        assert created_role == self.role_name0, f"Incorrect create role response: {res.json()}"

        # Also verify idempotency
        res = self.superuser_admin.create_role(role=self.role_name0)
        created_role = res.json()['role']
        assert created_role == self.role_name0, f"Incorrect create role response: {res.json()}"

    @cluster(num_nodes=3)
    def test_invalid_create_role(self):
        with expect_http_error(400):
            self.superuser_admin._request("post", "security/roles")

        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request("post",
                                          "security/roles",
                                          data='["json list not object"]')

        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request("post",
                                          "security/roles",
                                          json=dict())

        # Two ordinals (corresponding to ',' and '=') are explicitly excluded from role names
        for ordinal in [0x2c, 0x3d]:
            invalid_rolename = f"john{chr(ordinal)}doe"

            with expect_http_error(400):
                self.superuser_admin.create_role(role=invalid_rolename)

    @cluster(num_nodes=3)
    def test_members_endpoint(self):
        alice = RoleMember(RoleMember.PrincipalType.USER, 'alice')
        bob = RoleMember(RoleMember.PrincipalType.USER, 'bob')

        self.logger.debug(
            "Test that update_role_members can create the role as a side effect"
        )
        res = self.superuser_admin.update_role_members(role=self.role_name0,
                                                       add=[alice],
                                                       create=True)
        assert res.status_code == 200, "Expected 200 (OK)"
        member_update = RoleMemberUpdateResponse.from_response(res)
        assert member_update.role == self.role_name0, f"Incorrect role name: {member_update.role}"
        assert member_update.created, "Expected created flag to be set"
        assert len(member_update.added
                   ) == 1, f"Incorrect 'added' result: {member_update.added}"
        assert len(
            member_update.removed
        ) == 0, f"Incorrect 'removed' result: {member_update.removed}"
        assert alice in member_update.added, f"Incorrect member added: {member_update.added[0]}"

        def try_get_members(role):
            try:
                res = self.superuser_admin.list_role_members(role=role)
                return True, res
            except:
                return False, None

        self.logger.debug("And check that we can query the role we created")
        res = wait_until_result(lambda: try_get_members(self.role_name0),
                                timeout_sec=5,
                                backoff_sec=1)
        assert res is not None, f"Failed to get members for newly created role"

        assert res.status_code == 200, "Expected 200 (OK)"
        members = RoleMemberList.from_response(res)
        assert len(members) == 1, f"Unexpected members list: {members}"
        assert alice in members, f"Missing expected member, got: {members}"

        self.logger.debug("Now add a new member to the role")
        res = self.superuser_admin.update_role_members(role=self.role_name0,
                                                       add=[bob],
                                                       create=False)

        assert res.status_code == 200, "Expected 200 (OK)"
        member_update = RoleMemberUpdateResponse.from_response(res)
        assert len(member_update.added
                   ) == 1, f"Incorrect 'added' result: {member_update.added}"
        assert len(
            member_update.removed
        ) == 0, f"Incorrect 'removed' result: {member_update.removed}"
        assert bob in member_update.added

        def until_members(role,
                          expected: list[RoleMember] = [],
                          excluded: list[RoleMember] = []):
            try:
                res = self.superuser_admin.list_role_members(role=role)
                assert res.status_code == 200, "Expected 200 (OK)"
                members = RoleMemberList.from_response(res)
                exp = all(m in members for m in expected)
                excl = not any(m in members for m in excluded)
                return exp and excl, members
            except:
                return False, None

        self.logger.debug(
            "And verify that the members list eventually reflects that change")
        members = wait_until_result(
            lambda: until_members(self.role_name0, expected=[alice, bob]),
            timeout_sec=5,
            backoff_sec=1)

        assert members is not None, "Failed to get members"
        for m in [bob, alice]:
            assert m in members, f"Missing member {m}, got: {members}"

        self.logger.debug("Remove a member from the role")
        res = self.superuser_admin.update_role_members(role=self.role_name0,
                                                       remove=[alice])
        assert res.status_code == 200, "Expected 200 (OK)"
        member_update = RoleMemberUpdateResponse.from_response(res)

        assert len(
            member_update.removed
        ) == 1, f"Incorrect 'removed' result: {member_update.removed}"
        assert len(member_update.added
                   ) == 0, f"Incorrect 'added' result: {member_update.added}"
        assert alice in member_update.removed, f"Expected {alice} to be removed, got {member_update.removed}"

        self.logger.debug(
            "And verify that the members list eventually reflects the removal")
        members = wait_until_result(lambda: until_members(
            self.role_name0, expected=[bob], excluded=[alice]),
                                    timeout_sec=5,
                                    backoff_sec=1)

        assert members is not None
        assert len(members) == 1, f"Unexpected member: {members}"
        assert alice not in members, f"Unexpected member {alice}, got: {members}"

        self.logger.debug(
            "Test update idempotency - no-op update should succeed")
        res = self.superuser_admin.update_role_members(role=self.role_name0,
                                                       add=[bob])
        assert res.status_code == 200, "Expected 200 (OK)"
        member_update = RoleMemberUpdateResponse.from_response(res)
        assert len(member_update.added
                   ) == 0, f"Unexpectedly added members: {member_update.added}"
        assert len(
            member_update.removed
        ) == 0, f"Unexpectedly removed members: {member_update.removed}"

        self.logger.debug(
            "Check that the create flag works even when add/remove lists are empty"
        )
        res = self.superuser_admin.update_role_members(role=self.role_name1,
                                                       create=True)
        assert res.status_code == 200, "Expected 200 (OK)"  # TODO(oren): should be 201??
        member_update = RoleMemberUpdateResponse.from_response(res)
        assert len(member_update.added
                   ) == 0, f"Unexpectedly added members: {member_update.added}"
        assert len(
            member_update.removed
        ) == 0, f"Unexpectedly removed members: {member_update.removed}"
        assert member_update.created, "Expected created flag to be set"

    @cluster(num_nodes=3)
    def test_members_endpoint_errors(self):
        alice = RoleMember(RoleMember.PrincipalType.USER, 'alice')
        bob = RoleMember(RoleMember.PrincipalType.USER, 'bob')

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.list_role_members(role=self.role_name0)

        self.logger.debug(
            "ROLE_NOT_FOUND whether create flag is defaulted or explicitly set false"
        )
        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.update_role_members(role=self.role_name0,
                                                     add=[alice])

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.update_role_members(role=self.role_name0,
                                                     add=[alice],
                                                     create=False)

        self.logger.debug(
            "MEMBER_LIST_CONFLICT even if the role doesn't exist")
        with expect_role_error(RoleErrorCode.MEMBER_LIST_CONFLICT):
            self.superuser_admin.update_role_members(role=self.role_name0,
                                                     add=[alice],
                                                     remove=[alice],
                                                     create=True)

        self.logger.debug("Check that errored update has no effect")
        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.list_role_members(role=self.role_name0)

        self.logger.debug("POST body must be a JSON object")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request(
                "post",
                f"security/roles/{self.role_name0}/members",
                data='["json list not an object"]')

        self.superuser_admin.update_role_members(role=self.role_name0,
                                                 create=True)

        def role_exists(role):
            try:
                self.superuser_admin.list_role_members(role=role)
                return True
            except:
                return False

        wait_until(lambda: role_exists(self.role_name0),
                   timeout_sec=5,
                   backoff_sec=1)

        self.logger.debug("Role members must be JSON objects")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request(
                "post",
                f"security/roles/{self.role_name0}/members",
                data=json.dumps({'add': ["foo"]}))

        self.logger.debug("Role members must have name field")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request(
                "post",
                f"security/roles/{self.role_name0}/members",
                data=json.dumps({'add': [{}]}))

        self.logger.debug("Role members must have principal_type field")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request(
                "post",
                f"security/roles/{self.role_name0}/members",
                data=json.dumps({'add': [{
                    'name': 'foo',
                }]}))

        self.logger.debug("principal_type field must be 'User'")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request(
                "post",
                f"security/roles/{self.role_name0}/members",
                data=json.dumps(
                    {'add': [{
                        'name': 'foo',
                        'principal_type': 'user',
                    }]}))

        self.logger.debug("A valid raw request")
        res = self.superuser_admin._request(
            "post",
            f"security/roles/{self.role_name0}/members",
            data=json.dumps(
                {'add': [{
                    'name': 'foo',
                    'principal_type': 'User',
                }]}))

        assert res.status_code == 200, f"Request unexpectedly failed with status {res.status_code}"


class RBACTelemetryTest(RBACTestBase):
    def __init__(self, test_ctx, **kwargs):
        self.metrics = MetricsReporterServer(test_ctx)
        super().__init__(test_ctx,
                         extra_rp_conf={**self.metrics.rp_conf()},
                         **kwargs)

    def setUp(self):
        self.metrics.start()
        super().setUp()

    @cluster(num_nodes=2)
    def test_telemetry(self):
        def wait_for_new_report():
            report_count = len(self.metrics.requests())
            wait_until(lambda: len(self.metrics.requests()) > report_count,
                       timeout_sec=20,
                       backoff_sec=1)
            self.logger.debug(f'New report: {self.metrics.reports()[-1]}')
            return self.metrics.reports()[-1]

        assert wait_for_new_report()['has_rbac'] is False

        self.superuser_admin.create_role(role=self.role_name0)

        wait_until(lambda: wait_for_new_report()['has_rbac'] is True,
                   timeout_sec=20,
                   backoff_sec=1)
        self.metrics.stop()


class RBACLicenseTest(RBACTestBase):
    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, test_ctx, **kwargs):
        super().__init__(test_ctx, **kwargs)
        self.redpanda.set_environment({
            '__REDPANDA_LICENSE_CHECK_INTERVAL_SEC':
            f'{self.LICENSE_CHECK_INTERVAL_SEC}'
        })

    def _has_license_nag(self):
        return self.redpanda.search_log_any("Enterprise feature(s).*")

    def _license_nag_is_set(self):
        return self.redpanda.search_log_all(
            f"Overriding default license log annoy interval to: {self.LICENSE_CHECK_INTERVAL_SEC}s"
        )

    @cluster(num_nodes=1)
    def test_license_nag(self):
        wait_until(self._license_nag_is_set,
                   timeout_sec=30,
                   err_msg="Failed to set license nag internal")

        self.logger.debug("Ensuring no license nag")
        time.sleep(self.LICENSE_CHECK_INTERVAL_SEC * 2)
        assert not self._has_license_nag()

        self.logger.debug("Adding a role")
        self.superuser_admin.create_role(role=self.role_name0)

        self.logger.debug("Waiting for license nag")
        wait_until(self._has_license_nag,
                   timeout_sec=self.LICENSE_CHECK_INTERVAL_SEC * 2,
                   err_msg="License nag failed to appear")
