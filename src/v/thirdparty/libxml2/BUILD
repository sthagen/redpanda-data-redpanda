load("@rules_cc//cc:cc_library.bzl", "cc_library")

cc_library(
    name = "libxml2",
    hdrs = [
        "parser.h",
        "xmlwriter.h",
    ],
    include_prefix = "thirdparty/libxml2",
    visibility = ["//visibility:public"],
    deps = [
        "@libxml2",
    ],
)
