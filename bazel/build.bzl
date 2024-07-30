"""
This module contains functions for building Redpanda libraries and executables.
Prefer using the methods in this module (e.g. redpanda_cc_library) over native
Bazel functions (e.g. cc_library) because it provides a centralized place for
making behavior changes across the entire build.
"""

load(":internal.bzl", "redpanda_copts")

# buildifier: disable=function-docstring-args
def redpanda_cc_library(
        name,
        srcs = [],
        hdrs = [],
        defines = [],
        local_defines = [],
        strip_include_prefix = None,
        visibility = None,
        include_prefix = None,
        exclude_layering_check = False,
        copts = [],
        deps = []):
    """
    Define a Redpanda C++ library.
    """
    features = []
    if not exclude_layering_check:
        # TODO(bazel) Some dependencies brought in via rules_foreign_cc appear to not
        # have all their headers declared as outputs, which causes issues with
        # layering checks. So we allow layering check to be disabled in some
        # cases until this issue is addressed.
        # https://github.com/bazelbuild/rules_foreign_cc/issues/1221
        features.append("layering_check")

    native.cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        defines = defines,
        local_defines = local_defines,
        visibility = visibility,
        include_prefix = include_prefix,
        strip_include_prefix = strip_include_prefix,
        deps = deps,
        copts = redpanda_copts() + copts,
        features = features,
    )
