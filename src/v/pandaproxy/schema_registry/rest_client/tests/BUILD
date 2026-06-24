load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "parse_test",
    timeout = "short",
    srcs = [
        "parse_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/pandaproxy/schema_registry/rest_client:parse",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "retry_policy_test",
    timeout = "short",
    srcs = [
        "retry_policy_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/pandaproxy/schema_registry/rest_client:retry_policy",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "client_test",
    timeout = "short",
    srcs = [
        "client_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/http",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/pandaproxy/schema_registry/rest_client:client",
        "//src/v/pandaproxy/schema_registry/rest_client:error",
        "//src/v/test_utils:gtest",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "client_integration_test",
    timeout = "short",
    srcs = [
        "client_integration_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cluster:client_quota_backend",
        "//src/v/cluster:cluster_utils",
        "//src/v/cluster:controller_log_limiter",
        "//src/v/cluster:data_migration_table",
        "//src/v/cluster:feature_backend",
        "//src/v/cluster:leader_balancer_strategy",
        "//src/v/cluster:node_status_backend",
        "//src/v/cluster:self_test",
        "//src/v/cluster:topic_metrics_watcher",
        "//src/v/http",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/pandaproxy/schema_registry/rest_client:client",
        "//src/v/pandaproxy/schema_registry/rest_client:error",
        "//src/v/pandaproxy/test:fixture",
        "//src/v/pandaproxy/test:utils",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:retry_chain_node",
        "@boost//:beast",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)
