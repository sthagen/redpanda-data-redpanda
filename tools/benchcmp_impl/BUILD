load("@rules_python//python:defs.bzl", "py_binary", "py_test")

py_binary(
    name = "benchcmp",
    srcs = ["benchcmp.py"],
    main = "benchcmp.py",
    visibility = ["//visibility:public"],
)

py_test(
    name = "benchcmp_test",
    size = "small",
    srcs = [
        "benchcmp.py",
        "benchcmp_test.py",
    ],
    imports = ["."],
    main = "benchcmp_test.py",
)
