load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "opfuzz",
    srcs = [
        "opfuzz.cc",
    ],
    hdrs = [
        "opfuzz.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/utils:directory_walker",
        "@boost//:algorithm",
    ],
    include_prefix = "storage/opfuzz",
    deps = [
        "//src/v/model",
        "//src/v/storage",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "opfuzz_test",
    timeout = "short",
    srcs = [
        "opfuzz_test.cc",
    ],
    cpu = 1,
    deps = [
        ":opfuzz",
        "//src/v/base",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage/tests:storage_test_fixture",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
    ],
)
