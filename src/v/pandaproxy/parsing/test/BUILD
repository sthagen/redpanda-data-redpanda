load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "from_chars",
    timeout = "short",
    srcs = [
        "from_chars.cc",
    ],
    deps = [
        "//src/v/pandaproxy:parsing",
        "//src/v/reflection:type_traits",
        "//src/v/strings:utf8",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:named_type",
        "@boost//:mpl",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "httpd",
    timeout = "short",
    srcs = [
        "httpd.cc",
    ],
    deps = [
        "//src/v/pandaproxy:json",
        "//src/v/pandaproxy:parsing",
        "//src/v/reflection:type_traits",
        "//src/v/strings:utf8",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@boost//:tuple",
        "@boost//:utility",
        "@seastar",
        "@seastar//:testing",
    ],
)
