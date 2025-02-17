"""
A rule to create a redpanda tarball given inputs from the build system.
"""

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

    # Collect all the shared libraries that we built as part of Redpanda.
    rp_runfiles = ctx.attr.redpanda_binary[DefaultInfo].default_runfiles.files.to_list()
    for solib in rp_runfiles:
        # Why the redpanda binary is marked as a runfile of itself? No idea...
        if solib == ctx.file.redpanda_binary:
            continue
        shared_libraries.append(solib)
    rp_binary = ctx.file.redpanda_binary

    if ctx.attr.rpath_override != "":
        #TODO: add overriding iotune and rp_util rpaths after we add them to the package
        rp_binary = _override_binary_rpath(ctx, ctx.attr.rpath_override, rp_binary)

    # TODO: set dynamic loader for other native binaries in the package
    if ctx.attr.include_sysroot_libs:
        rp_binary = _set_dynamic_loader(ctx, rp_binary, dynamic_loader, dynamic_loader_path)

    return struct(
        redpanda_binary = rp_binary,
        rpk_binary = ctx.file.rpk_binary,
        shared_libraries = shared_libraries,
    )

def _impl(ctx):
    package_content = _prepare_package_content(ctx)

    # Create the configuration file for the packaging tool
    cfg_file = ctx.actions.declare_file("%s.config.json" % ctx.attr.name)
    cfg = {
        "redpanda_binary": package_content.redpanda_binary.path,
        "rpk": package_content.rpk_binary.path if package_content.rpk_binary else None,
        "shared_libraries": [solib.path for solib in package_content.shared_libraries],
        "default_yaml_config": ctx.file.default_yaml_config.path if ctx.file.default_yaml_config else None,
        "owner": ctx.attr.owner,
    }
    ctx.actions.write(cfg_file, content = json.encode_indent(cfg))

    inputs = [cfg_file, package_content.redpanda_binary] + package_content.shared_libraries

    if package_content.rpk_binary:
        inputs.append(package_content.rpk_binary)
    if ctx.file.default_yaml_config:
        inputs.append(ctx.file.default_yaml_config)

    # run the packaging tool
    ctx.actions.run(
        outputs = [ctx.outputs.out],
        inputs = inputs,
        tools = [ctx.executable._tool],
        executable = ctx.executable._tool,
        arguments = [
            "-config",
            cfg_file.path,
            "-output",
            ctx.outputs.out.path,
        ],
        mnemonic = "BuildingRedpandaPackage",
        use_default_shell_env = False,
    )
    return [DefaultInfo(files = depset([ctx.outputs.out]))]

redpanda_package = rule(
    implementation = _impl,
    attrs = {
        "redpanda_binary": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "default_yaml_config": attr.label(
            allow_single_file = True,
        ),
        "rpk_binary": attr.label(
            allow_single_file = True,
        ),
        "owner": attr.int(),
        "out": attr.output(
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
