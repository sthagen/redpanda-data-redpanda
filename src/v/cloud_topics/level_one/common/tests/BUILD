load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "object_test",
    timeout = "short",
    srcs = [
        "object_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "object_utils_test",
    timeout = "short",
    srcs = [
        "object_utils_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_utils",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "file_arena_test",
    timeout = "short",
    srcs = [
        "file_arena_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:fake_io",
        "//src/v/cloud_topics/level_one/common:file_arena",
        "//src/v/config",
        "//src/v/ssx:semaphore",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
