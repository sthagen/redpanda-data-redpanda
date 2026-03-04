load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "read_merge_test",
    timeout = "short",
    srcs = [
        "read_merge_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/pipeline:event_filter",
        "//src/v/cloud_topics/level_zero/pipeline:pipeline_stage",
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:read_request",
        "//src/v/cloud_topics/level_zero/read_merge",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
        "@seastar",
    ],
)
