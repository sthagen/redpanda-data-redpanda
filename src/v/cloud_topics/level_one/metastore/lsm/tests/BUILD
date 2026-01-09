load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "lsm_update_test",
    timeout = "short",
    srcs = [
        "lsm_update_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/metastore/lsm:lsm_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:state",
        "//src/v/cloud_topics/level_one/metastore/lsm:write_batch_row",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
