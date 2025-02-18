load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load(":packaging.bzl", "redpanda_package")

redpanda_package(
    name = "redpanda_tar",
    out = "redpanda.tar.gz",
    default_yaml_config = "//conf:redpanda.yaml",
    include_sysroot_libs = True,
    redpanda_binary = "//:redpanda",
    rpath_override = "$ORIGIN/../lib",
    rpk_binary = "//:rpk",
)

redpanda_package(
    name = "vtools_input",
    out = "bazel_out.tar.gz",
    include_sysroot_libs = True,
    redpanda_binary = "//:redpanda",
    rpath_override = "$ORIGIN/../lib",
)

redpanda_package(
    name = "ducktape",
    out = "redpanda_ducktape",
    include_sysroot_libs = True,
    redpanda_binary = "//:redpanda",
    rpath_override = "$ORIGIN/../lib",
    rpk_binary = "//:rpk",
)

# This is the ID for the nonroot user in the distroless containers.
NONROOT_USER = 65532

redpanda_package(
    name = "oci_image_contents",
    out = "redpanda_nonroot.tar.gz",
    default_yaml_config = "//conf:redpanda.yaml",
    owner = NONROOT_USER,
    redpanda_binary = "//:redpanda",
    rpath_override = "$ORIGIN/../lib",
    rpk_binary = "//:rpk",
)

# NOTE: this image is currently experimental, don't use this for production.
oci_image(
    name = "image",
    base = "@distroless_cc_debian12",
    cmd = [
        "redpanda",
        "start",
        "--overprovisioned",
    ],
    entrypoint = ["/opt/redpanda/bin/rpk"],
    exposed_ports = [
        "8081",
        "8082",
        "9092",
        "9644",
    ],
    labels = {"org.opencontainers.image.authors": "Redpanda Data <hi@redpanda.com>"},
    tars = [
        ":oci_image_contents",
    ],
    user = "nonroot",
    volumes = ["/var/lib/redpanda/data"],
)

oci_load(
    name = "image_load",
    image = ":image",
    repo_tags = ["redpandadata/redpanda-dev:latest"],
)

go_binary(
    name = "tool",
    embed = [":packaging_lib"],
    visibility = ["//visibility:private"],
)

go_library(
    name = "packaging_lib",
    srcs = ["tool.go"],
    importpath = "github.com/redpanda-data/redpanda/bazel/packaging",
    visibility = ["//visibility:private"],
)
