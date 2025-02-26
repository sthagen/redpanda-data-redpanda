"""
A rule to create a redpanda tarball given inputs from the build system.
"""

load("@bazel_skylib//lib:collections.bzl", "collections")
load("@bazel_tools//tools/cpp:toolchain_utils.bzl", "find_cpp_toolchain")

def _is_versioned(file, starts_with):
    """ Return true if this file has a name like libfoo.so.N """
    parts = file.basename.rsplit(".", 3)
    if len(parts) != 3:
        return False
    if not parts[0].startswith(starts_with):
        return False
    if parts[1] != "so":
        return False
    for c in parts[2].elems():
        if not c.isdigit():
            return False
    return True

def _is_versioned_so(file):
    return _is_versioned(file, "lib")

def _is_dynamic_loader(file):
    return _is_versioned(file, "ld")

def _override_binary_rpath(ctx, path_override, original_binary):
    patched_binary = ctx.actions.declare_file("{}_patched".format(original_binary.path))

    ctx.actions.run(
        inputs = [original_binary],
        outputs = [patched_binary],
        executable = ctx.executable._patchelf,
        arguments = ["--set-rpath", path_override, original_binary.path, "--output", patched_binary.path],
        tools = [],
        mnemonic = "OverrideBinaryRPath",
    )
    return patched_binary

def _set_dynamic_loader(ctx, binary, loader, interpreter_path):
    """Uses provided dynamic loader as the interpreter for the binary"""
    patched_binary = ctx.actions.declare_file("{}_ld".format(binary.path))
    ctx.actions.run(
        inputs = [binary, loader],
        outputs = [patched_binary],
        executable = ctx.executable._patchelf,
        arguments = ["--set-interpreter", "{}/{}".format(interpreter_path, loader.basename), binary.path, "--output", patched_binary.path],
        tools = [],
        mnemonic = "SetDynamicLoader",
    )
    return patched_binary

def _prepare_package_content(ctx, dynamic_loader_path = "/opt/redpanda/lib"):
    # Collect all shared libraries from the sysroot that we used.
    shared_libraries = []
    dynamic_loader = None
    cc_toolchain = find_cpp_toolchain(ctx)
    if cc_toolchain.sysroot != None and ctx.attr.include_sysroot_libs:
        for cc_file in cc_toolchain.all_files.to_list():
            if cc_file.path.startswith(cc_toolchain.sysroot):
                if _is_versioned_so(cc_file):
                    shared_libraries.append(cc_file)
                elif _is_dynamic_loader(cc_file):
                    shared_libraries.append(cc_file)
                    dynamic_loader = cc_file

    if ctx.attr.include_sysroot_libs and dynamic_loader == None:
        fail("Dynamic loader not found in sysroot")

    # Collect all the shared libraries that we built as part of each binary.
    # NOTE: We don't need to do this for `rpk` because it's a static binary,
    # this is only needed for binaries with shared libraries.
    binaries = [
        ctx.attr.redpanda_binary,
        ctx.attr.rp_util,
        ctx.attr.iotune,
        ctx.attr.hwloc_calc,
        ctx.attr.hwloc_distrib,
        ctx.attr.openssl,
    ]
    binary_files = [
        ctx.file.redpanda_binary,
        ctx.file.rp_util,
        ctx.file.iotune,
        ctx.file.hwloc_calc,
        ctx.file.hwloc_distrib,
        ctx.file.openssl,
    ]
    for b in binaries:
        if b == None:
            continue
        runfiles = b[DefaultInfo].default_runfiles.files.to_list()
        for solib in runfiles:
            # Why the binary is marked as a runfile of itself? No idea...
            if solib in binary_files:
                continue
            shared_libraries.append(solib)

    if ctx.attr.rpath_override != "":
        for i in range(0, len(binary_files)):
            if binary_files[i] == None:
                continue
            binary_files[i] = _override_binary_rpath(
                ctx,
                ctx.attr.rpath_override,
                binary_files[i],
            )

    if ctx.attr.include_sysroot_libs:
        for i in range(0, len(binary_files)):
            if binary_files[i] == None:
                continue
            binary_files[i] = _set_dynamic_loader(
                ctx,
                binary_files[i],
                dynamic_loader,
                dynamic_loader_path,
            )

    [rp_binary, rp_util, iotune, hwloc_calc, hwloc_distrib, openssl] = binary_files

    return struct(
        redpanda_binary = rp_binary,
        rp_util = rp_util,
        iotune = iotune,
        hwloc_calc = hwloc_calc,
        hwloc_distrib = hwloc_distrib,
        openssl = openssl,
        rpk_binary = ctx.file.rpk_binary,
        shared_libraries = collections.uniq(shared_libraries),
    )

def _impl(ctx):
    use_dir = not ctx.attr.out.endswith(".tar.gz")
    out = ctx.actions.declare_directory(ctx.attr.out) if use_dir else ctx.actions.declare_file(ctx.attr.out)
    package_content = _prepare_package_content(ctx)

    fips_enabled = ctx.file.fips_module != None
    if fips_enabled != (ctx.file.fips_config != None):
        fail("`fips_module` and `fips_config` must both be specified in", ctx.attr.name)

    # Create the configuration file for the packaging tool
    cfg_file = ctx.actions.declare_file("%s.config.json" % ctx.attr.name)
    cfg = {
        "redpanda_binary": package_content.redpanda_binary.path,
        "rp_util": package_content.rp_util.path if package_content.rp_util else None,
        "iotune": package_content.iotune.path if package_content.iotune else None,
        "hwloc_calc": package_content.hwloc_calc.path if package_content.hwloc_calc else None,
        "hwloc_distrib": package_content.hwloc_distrib.path if package_content.hwloc_distrib else None,
        "openssl": package_content.openssl.path if package_content.openssl else None,
        "rpk": package_content.rpk_binary.path if package_content.rpk_binary else None,
        "shared_libraries": [solib.path for solib in package_content.shared_libraries],
        "default_yaml_config": ctx.file.default_yaml_config.path if ctx.file.default_yaml_config else None,
        "fips": {"module": ctx.file.fips_module.path, "config": ctx.file.fips_config.path} if fips_enabled else None,
        "owner": ctx.attr.owner,
        "directory_mode": use_dir,
    }
    ctx.actions.write(cfg_file, content = json.encode_indent(cfg))

    inputs = [cfg_file, package_content.redpanda_binary] + package_content.shared_libraries

    optional_inputs = [
        package_content.rp_util,
        package_content.iotune,
        package_content.hwloc_calc,
        package_content.hwloc_distrib,
        package_content.openssl,
        package_content.rpk_binary,
        ctx.file.default_yaml_config,
    ]

    for input in optional_inputs:
        if input != None:
            inputs.append(input)

    if fips_enabled:
        inputs.append(ctx.file.fips_module)
        inputs.append(ctx.file.fips_config)

    # run the packaging tool
    ctx.actions.run(
        outputs = [out],
        inputs = inputs,
        tools = [ctx.executable._tool],
        executable = ctx.executable._tool,
        arguments = [
            "-config",
            cfg_file.path,
            "-output",
            out.path,
        ],
        mnemonic = "BuildingRedpandaPackage",
        use_default_shell_env = False,
    )
    return [DefaultInfo(files = depset([out]))]

redpanda_package = rule(
    implementation = _impl,
    attrs = {
        "redpanda_binary": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "rp_util": attr.label(
            allow_single_file = True,
        ),
        "iotune": attr.label(
            allow_single_file = True,
        ),
        "hwloc_calc": attr.label(
            allow_single_file = True,
        ),
        "hwloc_distrib": attr.label(
            allow_single_file = True,
        ),
        "openssl": attr.label(
            allow_single_file = True,
        ),
        "default_yaml_config": attr.label(
            allow_single_file = True,
        ),
        "fips_module": attr.label(
            allow_single_file = True,
        ),
        "fips_config": attr.label(
            allow_single_file = True,
        ),
        "rpk_binary": attr.label(
            allow_single_file = True,
        ),
        "owner": attr.int(),
        "out": attr.string(
            mandatory = True,
        ),
        "include_sysroot_libs": attr.bool(),
        "rpath_override": attr.string(mandatory = False),
        "_tool": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("//bazel/packaging:tool"),
        ),
        "_cc_toolchain": attr.label(
            default = Label("@bazel_tools//tools/cpp:current_cc_toolchain"),
        ),
        "_patchelf": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("@patchelf"),
        ),
    },
    toolchains = ["@bazel_tools//tools/cpp:toolchain_type"],
)
