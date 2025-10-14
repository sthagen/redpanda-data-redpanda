load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/cloud_topics:__pkg__",
        "//src/v/cloud_topics/batch_cache:__pkg__",
        "//src/v/cloud_topics/frontend:__pkg__",
        "//src/v/cloud_topics/frontend/tests:__pkg__",
        "//src/v/cloud_topics/level_zero/batcher:__pkg__",
        "//src/v/cloud_topics/level_zero/batcher/tests:__pkg__",
        "//src/v/cloud_topics/level_zero/frontend_reader:__pkg__",
        "//src/v/cloud_topics/level_zero/pipeline:__pkg__",
        "//src/v/cloud_topics/level_zero/pipeline/tests:__pkg__",
        "//src/v/cloud_topics/level_zero/reader:__pkg__",
        "//src/v/cloud_topics/level_zero/reader/tests:__pkg__",
        "//src/v/cloud_topics/level_zero/stm:__pkg__",
        "//src/v/cloud_topics/level_zero/write_request_scheduler:__pkg__",
        "//src/v/cloud_topics/level_zero/write_request_scheduler/tests:__pkg__",
    ],
)

redpanda_cc_library(
    name = "extent_meta",
    hdrs = [
        "extent_meta.h",
    ],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "level_zero_probe",
    srcs = [
        "level_zero_probe.cc",
    ],
    hdrs = [
        "level_zero_probe.h",
    ],
    deps = [
        "//src/v/metrics",
        "//src/v/utils:hdr_hist",
        "//src/v/utils:log_hist",
        "@fmt",
        "@seastar",
    ],
)
