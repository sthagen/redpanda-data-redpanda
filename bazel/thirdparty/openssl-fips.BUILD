load("@bazel_skylib//rules:select_file.bzl", "select_file")
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "openssl-fips",
    args = [
        "-j$OPENSSL_BUILD_JOBS",
        # This will cause OpenSSL to ignore the prefix flag and install at the specified DESTDIR
        "DESTDIR=$BUILD_TMPDIR/openssl-fips",
    ],
    configure_command = "Configure",
    configure_options = [
        # OpenSSL will look for system certs in a path relative to the OPENSSLDIR macro
        # This macro is defined as <prefix>/<openssldir>
        # Most linux environments install system certs in /etc/ssl/certs, so we set
        # the OPENSSLDIR macro to /etc/ssl
        "enable-fips",
        "--prefix=/",
        "--openssldir=/etc/ssl",
        "--libdir=lib",
        "no-tests",
    ] + select({
        "@openssl//:debug_mode": ["--debug"],
        "@openssl//:release_mode": ["--release"],
        "//conditions:default": [],
    }),
    env = {
        "OPENSSL_BUILD_JOBS": "$(BUILD_JOBS)",
    },
    lib_source = ":srcs",
    out_shared_libs = [
        "ossl-modules/fips.so",
    ],
    targets = [
        "",
        "install_fips",
    ],
    toolchains = ["@openssl//:build_jobs"],
    visibility = [
        "//visibility:public",
    ],
)

filegroup(
    name = "gen_dir",
    srcs = [":openssl-fips"],
    output_group = "gen_dir",
)

select_file(
    name = "fipsmodule_so",
    srcs = ":openssl-fips",
    subpath = "lib/ossl-modules/fips.so",
    visibility = [
        "//visibility:public",
    ],
)

# We must use a genrule here because the output of `gen_dir`
# is a directory artifact, which you cannot pluck things out of
# using select_file.
genrule(
    name = "fipsmodule_cnf",
    srcs = [":gen_dir"],
    outs = ["fipsmodule.cnf"],
    cmd = "cp -L $(SRCS)/etc/ssl/fipsmodule.cnf $@ && sed -i '/activate = 1/d' $@",
    visibility = [
        "//visibility:public",
    ],
)
