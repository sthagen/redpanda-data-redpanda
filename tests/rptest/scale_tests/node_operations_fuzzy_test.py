# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import re
import threading
import time
import requests

from ducktape.mark import matrix
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kcl import KCL
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.services.admin import Admin
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.redpanda import RedpandaService, CHAOS_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.end_to_end import EndToEndTest

DECOMMISSION = "decommission"
ADD = "add"
ADD_NO_WAIT = "add_no_wait"

ALLOWED_REPLICATION = [1, 3]

# Allow logs from previously allowed on old versions.
V22_1_CHAOS_ALLOW_LOGS = [
    re.compile("storage - Could not parse header"),
    re.compile("storage - Cannot continue parsing"),
]


class NodeOperationFuzzyTest(EndToEndTest):
    max_suspend_duration_seconds = 10
    min_inter_failure_time = 30
    max_inter_failure_time = 60

    def generate_random_workload(self, count, available_nodes):
        op_types = [ADD, ADD_NO_WAIT, DECOMMISSION]
        # current state
        active_nodes = list(available_nodes)
        decommissioned_nodes = []
        operations = []

        def decommission(id):
            active_nodes.remove(id)
            decommissioned_nodes.append(id)

        def add(id):
            active_nodes.append(id)
            decommissioned_nodes.remove(id)

        for _ in range(0, count):
            if len(active_nodes) <= 3:
                id = random.choice(decommissioned_nodes)
                operations.append((ADD, id))
                add(id)
            elif len(decommissioned_nodes) == 0:
                id = random.choice(active_nodes)
                operations.append((DECOMMISSION, id))
                decommission(id)
            else:
                op = random.choice(op_types)
                if op == DECOMMISSION:
                    id = random.choice(active_nodes)
                    operations.append((DECOMMISSION, id))
                    decommission(id)
                elif op == ADD or op == ADD_NO_WAIT:
                    id = random.choice(decommissioned_nodes)
                    operations.append((op, id))
                    add(id)

        return operations

    def _create_random_topics(self, count):
        max_partitions = 10

        topics = []
        for i in range(0, count):
            name = f"topic-{i}"
            spec = TopicSpec(
                name=name,
                partition_count=random.randint(1, max_partitions),
                replication_factor=random.choice(ALLOWED_REPLICATION))

            topics.append(spec)

        for spec in topics:
            DefaultClient(self.redpanda).create_topic(spec)

        return topics

    @cluster(num_nodes=7,
             log_allow_list=CHAOS_LOG_ALLOW_LIST + V22_1_CHAOS_ALLOW_LOGS)
    @matrix(enable_failures=[True, False], num_to_upgrade=[0, 3])
    def test_node_operations(self, enable_failures, num_to_upgrade):
        # allocate 5 nodes for the cluster
        extra_rp_conf = {
            "group_topic_partitions": 3,
            "default_topic_replications": 3,
        }
        if num_to_upgrade > 0:
            # Use the deprecated config to bootstrap older nodes.
            extra_rp_conf["enable_auto_rebalance_on_node_add"] = True
        else:
            extra_rp_conf["partition_autobalancing_mode"] = "node_add"
        self.redpanda = RedpandaService(self.test_context,
                                        5,
                                        extra_rp_conf=extra_rp_conf)
        if num_to_upgrade > 0:
            installer = self.redpanda._installer
            installer.install(self.redpanda.nodes, (22, 1, 4))
            self.redpanda.start()
            installer.install(self.redpanda.nodes[:num_to_upgrade],
                              RedpandaInstaller.HEAD)
            self.redpanda.restart_nodes(self.redpanda.nodes[:num_to_upgrade])
        else:
            self.redpanda.start()
        # create some topics
        topics = self._create_random_topics(10)
        self.redpanda.logger.info(f"using topics: {topics}")
        # select one of the topics to use in consumer/producer
        self.topic = random.choice(topics).name

        self.start_producer(1, throughput=100)
        self.start_consumer(1)
        self.await_startup()
        admin_fuzz = AdminOperationsFuzzer(self.redpanda,
                                           operations_interval=3)

        admin_fuzz.start()
        self.active_nodes = set(
            [self.redpanda.idx(n) for n in self.redpanda.nodes])
        # collect current mapping
        self.ids_mapping = {}
        for n in self.redpanda.nodes:
            self.ids_mapping[self.redpanda.idx(n)] = self.redpanda.idx(n)
        self.next_id = sorted(list(self.ids_mapping.keys()))[-1] + 1
        self.redpanda.logger.info(f"Initial ids mapping: {self.ids_mapping}")
        NODE_OP_TIMEOUT = 360

        def get_next_id():
            id = self.next_id
            self.next_id += 1
            return id

        def failure_injector_loop():
            with FailureInjector(self.redpanda) as f_injector:
                while enable_failures:
                    f_type = random.choice(FailureSpec.NETEM_FAILURE_TYPES +
                                           FailureSpec.FAILURE_TYPES)
                    length = 0
                    # allow suspending any node
                    if f_type == FailureSpec.FAILURE_SUSPEND:
                        length = random.randint(
                            1, NodeOperationFuzzyTest.
                            max_suspend_duration_seconds)
                        node = random.choice(self.redpanda.nodes)
                    else:
                        #kill/terminate only active nodes (not to influence the test outcome)
                        idx = random.choice(list(self.active_nodes))
                        node = self.redpanda.get_node(idx)

                    f_injector.inject_failure(
                        FailureSpec(node=node, type=f_type, length=length))

                    delay = random.randint(
                        NodeOperationFuzzyTest.min_inter_failure_time,
                        NodeOperationFuzzyTest.max_inter_failure_time)
                    self.redpanda.logger.info(
                        f"waiting {delay} seconds before next failure")
                    time.sleep(delay)

        if enable_failures:
            finjector_thread = threading.Thread(target=failure_injector_loop,
                                                args=())
            finjector_thread.daemon = True
            finjector_thread.start()

        def decommission(idx):
            node_id = self.ids_mapping[idx]
            self.logger.info(f"decommissioning node: {idx} with id: {node_id}")

            def decommissioned():
                try:
                    admin = Admin(self.redpanda)
                    # if broker is already draining, it is success

                    brokers = admin.get_brokers()
                    for b in brokers:
                        if b['node_id'] == node_id and b[
                                'membership_status'] == 'draining':
                            return True

                    r = admin.decommission_broker(id=node_id)
                    return r.status_code == 200
                except requests.exceptions.RetryError:
                    return False
                except requests.exceptions.ConnectionError:
                    return False
                except requests.exceptions.HTTPError:
                    return False

            wait_until(decommissioned,
                       timeout_sec=NODE_OP_TIMEOUT,
                       backoff_sec=2)
            admin = Admin(self.redpanda)

            def is_node_removed(idx_to_query, node_id):
                try:
                    brokers = admin.get_brokers(
                        self.redpanda.get_node(idx_to_query))
                    ids = map(lambda broker: broker['node_id'], brokers)
                    return not node_id in ids
                except:
                    return False

            def node_removed():
                node_removed_cnt = 0
                for idx in self.active_nodes:
                    if is_node_removed(idx, node_id):
                        node_removed_cnt += 1

                node_count = len(self.redpanda.nodes)
                majority = int(node_count / 2) + 1
                self.redpanda.logger.debug(
                    f"node {node_id} removed on {node_removed_cnt} nodes, majority: {majority}"
                )
                return node_removed_cnt >= majority

            wait_until(node_removed,
                       timeout_sec=NODE_OP_TIMEOUT,
                       backoff_sec=2)
            self.redpanda.stop_node(self.redpanda.get_node(idx))

        kafkacat = KafkaCat(self.redpanda)

        def replicas_per_node():
            node_replicas = {}
            md = kafkacat.metadata()
            self.redpanda.logger.info(f"metadata: {md}")
            for topic in md['topics']:
                for p in topic['partitions']:
                    for r in p['replicas']:
                        id = r['id']
                        if id not in node_replicas:
                            node_replicas[id] = 0
                        node_replicas[id] += 1

            return node_replicas

        def seed_servers_for(idx):
            seeds = map(
                lambda n: {
                    "address": n.account.hostname,
                    "port": 33145
                }, self.redpanda.nodes)

            return list(
                filter(
                    lambda n: n['address'] != self.redpanda.get_node(idx).
                    account.hostname, seeds))

        def add_node(idx, cleanup=True):
            id = get_next_id()
            self.logger.info(f"adding node: {idx} back with new id: {id}")
            self.ids_mapping[idx] = id
            self.redpanda.stop_node(self.redpanda.get_node(idx))
            if cleanup:
                self.redpanda.clean_node(self.redpanda.get_node(idx),
                                         preserve_logs=True,
                                         preserve_current_install=True)
            # we do not reuse previous node ids and override seed server list
            self.redpanda.start_node(
                self.redpanda.get_node(idx),
                timeout=NodeOperationFuzzyTest.min_inter_failure_time +
                NodeOperationFuzzyTest.max_suspend_duration_seconds + 30,
                override_cfg_params={
                    "node_id": id,
                    "seed_servers": seed_servers_for(idx)
                })

        def wait_for_new_replicas(idx):
            def has_new_replicas():
                id = self.ids_mapping[idx]
                per_node = replicas_per_node()
                self.logger.info(f"replicas per node: {per_node}")
                return id in per_node

            wait_until(has_new_replicas,
                       timeout_sec=NODE_OP_TIMEOUT,
                       backoff_sec=2)

        work = self.generate_random_workload(30,
                                             available_nodes=self.active_nodes)

        self.redpanda.logger.info(f"node operations to execute: {work}")
        for op in work:
            op_type = op[0]
            self.logger.info(
                f"executing - {op} - current ids: {self.ids_mapping}")
            if op_type == ADD:
                idx = op[1]
                self.active_nodes.add(idx)
                add_node(idx)
                wait_for_new_replicas(idx)
            if op_type == ADD_NO_WAIT:
                idx = op[1]
                self.active_nodes.add(idx)
                add_node(idx)
            if op_type == DECOMMISSION:
                idx = op[1]
                self.active_nodes.remove(idx)
                decommission(idx)

        enable_failures = False
        admin_fuzz.wait(20, 180)
        admin_fuzz.stop()
        self.run_validation(enable_idempotence=False,
                            producer_timeout_sec=60,
                            consumer_timeout_sec=180)
