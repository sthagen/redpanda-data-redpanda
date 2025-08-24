load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "reducer_test",
    timeout = "short",
    srcs = [
        "reducer_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_utils",
        "//src/v/cloud_topics/level_one/compaction:filter",
        "//src/v/cloud_topics/level_one/compaction:sink",
        "//src/v/compaction:reducer",
        "//src/v/compaction/tests:simple_reducer",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage/tests:batch_generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
