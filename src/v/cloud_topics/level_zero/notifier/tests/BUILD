load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "level_zero_notifier_test",
    timeout = "short",
    srcs = [
        "level_zero_notifier_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_zero/notifier:level_zero_notifier",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_factory",
        "//src/v/cluster:state_machine_registry",
        "//src/v/model",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
