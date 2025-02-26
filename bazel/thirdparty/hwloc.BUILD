load("@bazel_skylib//rules:common_settings.bzl", "int_flag")
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

# Make this build faster by setting `build --@hwloc//:build_jobs=16` in user.bazelrc
# if you have the cores to spare.
int_flag(
    name = "build_jobs",
    build_setting_default = 8,
    make_variable = "BUILD_JOBS",
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "hwloc",
    args = ["-j$HWLOC_BUILD_JOBS"],
    autoreconf = True,
    autoreconf_options = ["-ivf"],
    configure_in_place = True,
    configure_options = [
        "--disable-libudev",

        # Disable graphics and the many kinds of display driver discovery
        "--disable-gl",
        "--disable-opencl",
        "--disable-nvml",
        "--disable-cuda",
        "--disable-rsmi",

        # Build a static library
        "--disable-shared",
        "--enable-static",
    ],
    env = {
        "HWLOC_BUILD_JOBS": "$(BUILD_JOBS)",
    },
    lib_source = ":srcs",
    out_binaries = [
        "hwloc-calc",
        "hwloc-distrib",
    ],
    out_static_libs = ["libhwloc.a"],
    toolchains = [":build_jobs"],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@libpciaccess",
    ],
)

filegroup(
    name = "hwloc_calc",
    srcs = [":hwloc"],
    output_group = "hwloc-calc",
    visibility = ["//visibility:public"],
)

filegroup(
    name = "hwloc_distrib",
    srcs = [":hwloc"],
    output_group = "hwloc-distrib",
    visibility = ["//visibility:public"],
)
