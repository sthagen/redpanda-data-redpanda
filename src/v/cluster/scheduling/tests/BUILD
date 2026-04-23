load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "leader_pinning_test",
    timeout = "short",
    srcs = [
        "leader_pinning_test.cc",
    ],
    deps = [
        "//src/v/cluster:leader_balancer_strategy",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@googletest//:gtest",
        "@seastar",
    ],
)
