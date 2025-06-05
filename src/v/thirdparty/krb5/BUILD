load("@rules_cc//cc:cc_library.bzl", "cc_library")

cc_library(
    name = "krb5",
    hdrs = [
        "gssapi.h",
        "gssapi_ext.h",
        "krb5.h",
    ],
    include_prefix = "thirdparty/krb5",
    visibility = ["//visibility:public"],
    deps = [
        "@krb5",
    ],
)
