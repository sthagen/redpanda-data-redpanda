load("@rules_shell//shell:sh_binary.bzl", "sh_binary")

# tools here are generally targeted by symlink or shim scripts in /tools

sh_binary(
    name = "debug",
    srcs = ["debug.sh"],
    env = {
        "BAZEL_DEBUG_ENV_TOKEN": "present",
    },
)
