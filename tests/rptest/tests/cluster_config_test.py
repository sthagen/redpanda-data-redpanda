# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import requests
import json

from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

BOOTSTRAP_CONFIG = {
    # A non-default value for checking bootstrap import works
    'enable_idempotence': True,
}


class ClusterConfigTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        rp_conf = BOOTSTRAP_CONFIG.copy()

        # Enable our feature flag
        rp_conf['enable_central_config'] = True

        super(ClusterConfigTest, self).__init__(*args,
                                                extra_rp_conf=rp_conf,
                                                **kwargs)

        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=3)
    def test_get_config(self):
        """
        Verify that the config GET endpoint serves valid json with some options in it.
        """
        admin = Admin(self.redpanda)
        config = admin.get_cluster_config()

        # Pick an arbitrary config property to verify that the result
        # contained some properties
        assert 'enable_transactions' in config

        node_config = admin.get_node_config()

        # Some arbitrary property to check syntax of result
        assert 'kafka_api' in node_config

    @cluster(num_nodes=3)
    def test_bootstrap(self):
        """
        Verify that config settings present in redpanda.cfg are imported on
        first startup.
        :return:
        """
        admin = Admin(self.redpanda)
        config = admin.get_cluster_config()
        for k, v in BOOTSTRAP_CONFIG.items():
            assert config[k] == v

        set_again = {'enable_idempotence': False}
        assert BOOTSTRAP_CONFIG['enable_idempotence'] != set_again[
            'enable_idempotence']

        self.redpanda.restart_nodes(self.redpanda.nodes, set_again)

        # Our attempt to set the value differently in the config file after first startup
        # should have failed: the original config value should still be set.
        config = admin.get_cluster_config()
        for k, v in BOOTSTRAP_CONFIG.items():
            assert config[k] == v

    def _wait_for_version_sync(self, version):
        wait_until(
            lambda: set([
                n['config_version']
                for n in self.admin.get_cluster_config_status()
            ]) == {version},
            timeout_sec=10,
            backoff_sec=0.5,
            err_msg=f"Config status versions did not converge on {version}")

    def _check_restart_clears(self):
        """
        After changing a setting with needs_restart=true, check that
        nodes clear the flag after being restarted.
        """
        status = self.admin.get_cluster_config_status()
        for n in status:
            assert n['restart'] is True

        first_node = self.redpanda.nodes[0]
        other_nodes = self.redpanda.nodes[1:]
        self.redpanda.restart_nodes(first_node)
        wait_until(lambda: self.admin.get_cluster_config_status()[0]['restart']
                   == False,
                   timeout_sec=10,
                   backoff_sec=0.5,
                   err_msg=f"Restart flag did not clear after restart")

        self.redpanda.restart_nodes(other_nodes)
        wait_until(lambda: set(
            [n['restart']
             for n in self.admin.get_cluster_config_status()]) == {False},
                   timeout_sec=10,
                   backoff_sec=0.5,
                   err_msg=f"Not all nodes cleared restart flag")

    @cluster(num_nodes=3)
    def test_restart(self):
        """
        Verify that a setting requiring restart is indicated as such in status,
        and that status is cleared after we restart the node.
        """
        # An arbitrary restart-requiring setting with a non-default value
        new_setting = ('kafka_qdc_idle_depth', 77)

        patch_result = self.admin.patch_cluster_config(
            upsert=dict([new_setting]))
        new_version = patch_result['config_version']
        self._wait_for_version_sync(new_version)

        assert self.admin.get_cluster_config()[
            new_setting[0]] == new_setting[1]
        # Update of cluster status is not synchronous
        self._check_restart_clears()

        # Test that a reset to default triggers the restart flag the same way as
        # an upsert does
        patch_result = self.admin.patch_cluster_config(remove=[new_setting[0]])
        new_version = patch_result['config_version']
        self._wait_for_version_sync(new_version)
        assert self.admin.get_cluster_config()[
            new_setting[0]] != new_setting[1]
        self._check_restart_clears()

    def _check_propagated_and_persistent(self, key, expect_value):
        """
        Verify that a configuration value has successfully propagated to all
        nodes, and that it persists after a restart.
        """
        def check_all():
            for node in self.redpanda.nodes:
                actual_value = self.admin.get_cluster_config(node)[key]
                if actual_value != expect_value:
                    self.logger.error(
                        f"Wrong value on node {node.account.hostname}: {key}={actual_value} (!={expect_value})"
                    )
                assert self.admin.get_cluster_config(node)[key] == expect_value

        check_all()
        self.redpanda.restart_nodes(self.redpanda.nodes)
        check_all()

    @cluster(num_nodes=3)
    def test_simple_live_change(self):
        # An arbitrary non-restart-requiring setting
        norestart_new_setting = ('log_message_timestamp_type', "LogAppendTime")
        assert self.admin.get_cluster_config()[
            norestart_new_setting[0]] == "CreateTime"  # Initially default
        patch_result = self.admin.patch_cluster_config(
            upsert=dict([norestart_new_setting]))
        new_version = patch_result['config_version']
        self._wait_for_version_sync(new_version)

        assert self.admin.get_cluster_config()[
            norestart_new_setting[0]] == norestart_new_setting[1]

        # Status should not indicate restart needed
        status = self.admin.get_cluster_config_status()
        for n in status:
            assert n['restart'] is False

        # Setting should be propagated and survive a restart
        self._check_propagated_and_persistent(norestart_new_setting[0],
                                              norestart_new_setting[1])

    @cluster(num_nodes=3)
    def test_invalid_settings(self):
        default_value = "CreateTime"
        invalid_setting = ('log_message_timestamp_type', "rhubarb")
        assert self.admin.get_cluster_config()[
            invalid_setting[0]] == default_value
        patch_result = self.admin.patch_cluster_config(
            upsert=dict([invalid_setting]))
        new_version = patch_result['config_version']
        self._wait_for_version_sync(new_version)

        assert self.admin.get_cluster_config()[
            invalid_setting[0]] == default_value

        # Status should not indicate restart needed
        status = self.admin.get_cluster_config_status()
        for n in status:
            assert n['restart'] is False
            assert n['invalid'] == [invalid_setting[0]]

        # List of invalid properties in node status should not clear on restart.
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # We have to sleep here because in the success case there is no status update
        # being sent: it's a no-op after node startup when they realize their config
        # status is the same as the one already reported.
        time.sleep(10)

        status = self.admin.get_cluster_config_status()
        for n in status:
            assert n['restart'] is False
            assert n['invalid'] == [invalid_setting[0]]

        # Reset the properties, check that it disappears from the list of invalid settings
        patch_result = self.admin.patch_cluster_config(
            remove=[invalid_setting[0]])
        self._wait_for_version_sync(patch_result['config_version'])
        assert self.admin.get_cluster_config()[
            invalid_setting[0]] == default_value

        status = self.admin.get_cluster_config_status()
        for n in status:
            assert n['restart'] is False
            assert n['invalid'] == []

        # TODO once API frontend does validation, this test will need a force
        # flag to the API to get the invalid value past the frontend and
        # to the nodes where it will show up in status.  That force flag
        # will also be important IRL if we want to enable e.g. pre-setting a config
        # for a future redpanda version before installing the new version.

        # TODO as well as specific invalid examples, do a pass across the whole
        # schema to check that
        pass

    @cluster(num_nodes=3)
    def test_bad_requests(self):
        """
        Verify that syntactically malformed configuration requests result
        in proper 400 responses (rather than 500s or crashes)
        """

        for content_type, body in [
            ('text/html', ""),  # Wrong type, empty
            ('text/html', "garbage"),  # Wrong type, nonempty
            ('application/json', ""),  # Empty
            ('application/json', "garbage"),  # Not JSON
            ('application/json', "{\"a\": 123}"),  # Wrong top level attributes
            ('application/json', "{\"upsert\": []}"),  # Wrong type of 'upsert'
        ]:
            try:
                self.logger.info(f"Checking {content_type}, {body}")
                self.admin._request("PUT",
                                    "cluster_config",
                                    node=self.redpanda.nodes[0],
                                    headers={'content-type': content_type},
                                    data=body)
            except requests.exceptions.HTTPError as e:
                assert e.response.status_code == 400
            else:
                # Should not succeed!
                assert False

    @cluster(num_nodes=3)
    def test_valid_settings(self):
        # TODO

        pass

    @cluster(num_nodes=3)
    def test_valid_settings(self):
        """
        Bulk exercise of all config settings & the schema endpoint:
        - for all properties in the schema, set them with a valid non-default value
        - check the new values are reflected in config GET
        - restart all nodes (prompt a reload from cache file)
        - check the new values are reflected in config GET

        This is not just checking the central config infrastructure: it's also
        validating that all the property types are outputting the same format
        as their input (e.g. they have proper rjson_serialize implementations)
        """
        schema_properties = self.admin.get_cluster_config_schema(
        )['properties']
        updates = {}
        properties_require_restart = False

        # Don't change these settings, they prevent the test from subsequently
        # using the cluster
        exclude_settings = {'enable_sasl', 'enable_admin_api'}

        initial_config = self.admin.get_cluster_config()

        for name, p in schema_properties.items():
            if name in exclude_settings:
                continue

            properties_require_restart |= p['needs_restart']

            initial_value = initial_config[name]
            if 'example' in p:
                valid_value = p['example']
            elif p['type'] == 'integer':
                if initial_value:
                    valid_value = initial_value * 2
                else:
                    valid_value = 100
            elif p['type'] == 'number':
                if initial_value:
                    valid_value = float(initial_value * 2)
                else:
                    valid_value = 1000.0
            elif p['type'] == 'string':
                valid_value = "rhubarb"
            elif p['type'] == 'boolean':
                valid_value = not initial_config[name]
            elif p['type'] == "array" and p['items']['type'] == 'string':
                valid_value = ["custard", "cream"]
            else:
                raise NotImplementedError(p['type'])

            updates[name] = valid_value

        patch_result = self.admin.patch_cluster_config(upsert=updates,
                                                       remove=[])
        self._wait_for_version_sync(patch_result['config_version'])

        def check_status(expect_restart):
            # Use one node's status, they should be symmetric
            status = self.admin.get_cluster_config_status()[0]

            self.logger.info(f"Status: {json.dumps(status, indent=2)}")

            assert status['invalid'] == []
            assert status['restart'] is expect_restart

        def check_values():
            read_back = self.admin.get_cluster_config()
            mismatch = []
            for k, expect in updates.items():
                actual = read_back.get(k, None)
                # String-ized comparison, because the example values are strings,
                # whereas by the time we read them back they're properly typed.
                if str(actual) != str(expect):
                    self.logger.error(
                        f"Config set failed ({k}) {actual}!={expect}")
                    mismatch.append((k, actual, expect))

            assert len(mismatch) == 0

        check_status(properties_require_restart)
        check_values()
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # We have to sleep here because in the success case there is no status update
        # being sent: it's a no-op after node startup when they realize their config
        # status is the same as the one already reported.
        time.sleep(10)

        # Check after restart that confuration persisted and status shows valid
        check_status(False)
        check_values()
