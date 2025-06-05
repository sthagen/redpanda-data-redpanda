load("@rules_cc//cc:cc_library.bzl", "cc_library")

cc_library(
    name = "ada",
    srcs = ["ada.cpp"],
    hdrs = [
        "ada.h",
        "ada_c.h",
    ],
    defines = ["ADA_INCLUDE_URL_PATTERN=0"],
    includes = ["."],
    visibility = [
        "//visibility:public",
    ],
)
