load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "xz",
    autoreconf = True,
    autoreconf_options = ["-ivf"],
    configure_in_place = True,
    configure_options = [
        "--disable-shared",
        "--enable-static",
    ],
    lib_source = ":srcs",
    out_static_libs = ["liblzma.a"],
    visibility = [
        "//visibility:public",
    ],
)
