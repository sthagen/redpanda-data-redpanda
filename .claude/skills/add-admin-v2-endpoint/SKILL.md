---
name: add-admin-v2-endpoint
description: Use when adding or modifying a Redpanda admin v2 ConnectRPC endpoint — authoring the protobuf under proto/redpanda/core/admin/v2/, wiring it into the Bazel build, implementing the C++ service handler under src/v/redpanda/admin/services/, registering it on the admin server, and regenerating Python bindings for ducktape integration tests. Covers both new RPCs on an existing service and brand-new services.
---

# Add an Admin v2 ConnectRPC Endpoint

This skill walks through every layer touched by a new admin v2 RPC: the `.proto`, the Bazel build, the C++ service implementation, registration on the admin server, and the Python bindings consumed by ducktape tests.

For generic protobuf authoring conventions (field options, message design, well-known types), the canonical reference is `proto/redpanda/README.md`. This skill assumes you've read that and focuses on what's specific to admin v2.

## Reference example

Commit `04d3160c3a` (`cluster: add manual upgrade finalization with admin v2 RPC`) is the cleanest minimal example — a new service with a single RPC. Skim these when unsure:

- `proto/redpanda/core/admin/v2/features.proto` — the proto
- `proto/redpanda/core/admin/v2/BUILD` — the BUILD entries (look at the `features_proto` / `features_redpanda_proto` pair)
- `src/v/redpanda/admin/services/features.h` and `features.cc` — the C++ impl
- `src/v/redpanda/admin/services/BUILD` — the `features` cc_library target
- `src/v/redpanda/application_admin.cc` — the `add_service` registration

`git show 04d3160c3a` will print the full diff.

## Procedure

### 1. Author the protobuf

Create or extend a `.proto` under `proto/redpanda/core/admin/v2/`. One file per service domain; the filename matches the dominant service (`features.proto` → `FeaturesService`).

Required header for a new file:

```proto
// (BSL copyright header — copy from features.proto:1-13)
syntax = "proto3";

package redpanda.core.admin.v2;

import "proto/redpanda/core/pbgen/options.proto";
import "proto/redpanda/core/pbgen/rpc.proto";

option (pbgen.cpp_namespace) = "proto::admin";
```

Notes:

- `package` is always `redpanda.core.admin.v2` for admin v2.
- `option (pbgen.cpp_namespace) = "proto::admin";` is mandatory and always exactly this value for admin v2 — the short form (`pbgen.cpp_namespace`, not the FQN `redpanda.core.pbgen.cpp_namespace`) is what's used in the tree.
- Import `rpc.proto` only when the file defines a `service`.

Service / RPC block:

```proto
service MyService {
    // One short paragraph explaining what the RPC does and any
    // async/poll behavior the caller needs to know.
    rpc MyMethod(MyMethodRequest) returns (MyMethodResponse) {
        option (pbgen.rpc) = {
            authz: SUPERUSER
        };
    }
}

message MyMethodRequest {}
message MyMethodResponse {}
```

- Every RPC must carry `option (pbgen.rpc) = { authz: ... }`. Default to `SUPERUSER` for admin v2 — anything else (`USER`, `PUBLIC`) needs core-team approval (see `proto/redpanda/core/pbgen/rpc.proto`).
- Optional `http_route: "/v2/..."` adds an alias HTTP path. The auto-generated `/<package>.<Service>/<Method>` route always works without it.
- Naming: messages are `<RpcName>Request` / `<RpcName>Response`, one pair per RPC. Empty messages are fine.

Field-level options worth knowing (full reference in `proto/redpanda/core/pbgen/options.proto`):

- `[(pbgen.iobuf) = true]` on `string` fields that may exceed 128 KiB — generates `iobuf` instead of `ss::sstring`. `bytes` is already iobuf-backed.
- `[(pbgen.ptr) = true]` on message fields for indirection (recursive types or when you need to distinguish unset).
- `[debug_redact = true]` on sensitive fields — suppresses them from `fmt::formatter` output (passwords, tokens, etc.).

Well-known types (`google.protobuf.Duration`, `Timestamp`, `FieldMask`) are supported; importing them requires the corresponding BUILD deps in step 2.

### 2. Wire up the Bazel build

Each `.proto` needs **two stacked targets** in `proto/redpanda/core/admin/v2/BUILD`. Append (alphabetical order in the file):

```python
proto_library(
    name = "myservice_proto",
    srcs = ["myservice.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
        # Add per-import:
        # "@protobuf//:timestamp_proto",
        # "@protobuf//:duration_proto",
        # "@protobuf//:field_mask_proto",
        # "@googleapis//google/api:field_info_proto",
        # "@googleapis//google/api:field_behavior_proto",
        # "@googleapis//google/api:resource_proto",
        # Cross-proto imports: e.g. ":kafka_connections_proto"
    ],
)

redpanda_proto_library(
    name = "myservice_redpanda_proto",
    protos = [":myservice_proto"],
    visibility = ["//visibility:public"],
    deps = [
        # Add when the proto uses Timestamp or Duration:
        # "@abseil-cpp//absl/time:time",
        # Add when the proto uses FieldMask:
        # "//src/v/serde/protobuf:field_mask",
        # For cross-proto deps the *_redpanda_proto sibling goes here:
        # ":kafka_connections_redpanda_proto"
    ],
)
```

Rules:

- `proto_library.deps` mirrors `import` statements in the `.proto` file.
- `redpanda_proto_library.deps` carries C++ deps the *generated* code needs (e.g. abseil for timestamps), plus any sibling `_redpanda_proto` targets for cross-proto imports.
- The pbgen plugin lives at `bazel/pbgen/`; it generates `<name>.proto.h` and `<name>.proto.cc` with the message types, formatters, and service stubs.

### 3. Implement the C++ service

If extending an existing service (just adding a new RPC), open the matching `src/v/redpanda/admin/services/<name>.{h,cc}` and add the new handler. Skip to step 4 after.

For a brand-new service, create `src/v/redpanda/admin/services/<name>.h`:

```cpp
/*
 * Copyright 2026 Redpanda Data, Inc.
 * (BSL header — copy from features.h:1-10)
 */
#pragma once

#include "cluster/fwd.h"
#include "proto/redpanda/core/admin/v2/<name>.proto.h"
#include "redpanda/admin/proxy/client.h"

namespace admin {

class <name>_service_impl : public proto::admin::<name>_service {
public:
    <name>_service_impl(
      admin::proxy::client,
      /* collaborators — e.g. cluster::controller*, ss::sharded<X>& */);

    ss::future<proto::admin::<rpc>_response> <rpc>(
      serde::pb::rpc::context, proto::admin::<rpc>_request) override;

private:
    admin::proxy::client _proxy_client;
    /* members for collaborators */
};

} // namespace admin
```

Naming conventions (CamelCase → snake_case translation):

- Service base class: `proto::admin::<service>_service` (e.g. `FeaturesService` → `features_service`)
- Generated client (for inter-node proxying): `proto::admin::<service>_service_client`
- Request / response types: `proto::admin::<rpc_name>_request` / `_response`
- Handler method name: snake_case of the RPC name (e.g. `FinalizeUpgrade` → `finalize_upgrade`)
- Implementation class: `admin::<service>_service_impl`, in namespace `admin` (NOT `proto::admin`)

Handler signature is fixed: `ss::future<...response>` return type, `serde::pb::rpc::context` first arg, request second, `override`.

Then `src/v/redpanda/admin/services/<name>.cc`:

```cpp
#include "redpanda/admin/services/<name>.h"
#include "redpanda/admin/services/utils.h"
#include "serde/protobuf/rpc.h"
#include <seastar/core/coroutine.hh>

namespace admin {
namespace {
ss::logger <name>log{"admin_api_server/<name>_service"};
} // namespace

<name>_service_impl::<name>_service_impl(/* args */) : _proxy_client(...) {}

ss::future<proto::admin::<rpc>_response>
<name>_service_impl::<rpc>(
  serde::pb::rpc::context ctx, proto::admin::<rpc>_request req) {
    vlog(<name>log.trace, "<rpc>: {}", req);
    // ... validate, optionally redirect-to-leader, do work ...
    co_return proto::admin::<rpc>_response{};
}

} // namespace admin
```

Patterns to know — read the existing service files for live examples:

- **Validation failures throw typed exceptions** declared in `src/v/serde/protobuf/rpc.h:241-287`: `invalid_argument_exception`, `failed_precondition_exception`, `not_found_exception`, `already_exists_exception`, `permission_denied_exception`, `unavailable_exception`, `unauthenticated_exception`, etc. Each maps to a ConnectRPC/gRPC status code. Example: `features.cc:46`.
- **Leader redirect**: `admin::utils::redirect_to_leader(...)` from `services/utils.h` returns an optional `node_id`; if set, proxy the call to that node using the generated `_service_client`. Pattern in `features.cc:51-64`.
- **Proxying to peers**: `_proxy_client.make_client_for_node<proto::admin::<service>_service_client>(node)` for one target, or `.make_clients_for_other_nodes<...>()` for scatter-gather. Defined in `redpanda/admin/proxy/client.h`.
- **License gating**: `admin::utils::check_license(_feature_table.local())` if the RPC is enterprise-gated. See `cluster.cc:51`.
- **Avoid proxy loops** in scatter-gather handlers: gate with `proxy::is_proxied(ctx)` (example in `cluster.cc:56`).

Add the BUILD entry to `src/v/redpanda/admin/services/BUILD`:

```python
redpanda_cc_library(
    name = "<name>",
    srcs = ["<name>.cc"],
    hdrs = ["<name>.h"],
    implementation_deps = [
        ":utils",
        # .cc-only collaborators
    ],
    deps = [
        "//proto/redpanda/core/admin/v2:<name>_redpanda_proto",
        "//src/v/cluster:fwd",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
        # types that appear in <name>.h
    ],
)
```

Rule: types that appear in the `.h` belong in `deps`; collaborators only used in the `.cc` belong in `implementation_deps`.

### 4. Register the service

Two edits in `src/v/redpanda/`:

**4a. `src/v/redpanda/BUILD`** — add the new services library to the top-level `redpanda` library's `deps` list (kept sorted):

```python
"//src/v/redpanda/admin/services:<name>",
```

**4b. `src/v/redpanda/application_admin.cc`** — two changes:

1. Include the header (alphabetical with the other service includes):
   ```cpp
   #include "redpanda/admin/services/<name>.h"
   ```
2. Inside `_admin.invoke_on_all([this, node_id](admin_server& s) { ... })`, append an `add_service` call. `create_client()` is the local lambda defined at the top of the block; pass other collaborators via `controller.get()` or `std::ref(...)`:
   ```cpp
   s.add_service(
     std::make_unique<admin::<name>_service_impl>(
       create_client(), /* collaborators */));
   ```

That's it for registration. `add_service` walks the service's routes and registers an HTTP POST handler per route at `route.path` with the auth wrapper keyed on `route.authz_level`.

### 5. Build and format

```bash
bazel build //proto/redpanda/core/admin/v2:<name>_redpanda_proto
bazel build //src/v/redpanda/admin/services:<name>
bazel build //src/v/redpanda:application
bazel run //tools:clang_format
```

Fix compilation errors before moving on. The pbgen plugin's error messages are usually self-explanatory; common causes are missing BUILD deps from step 2 or imports in the `.proto` without matching `proto_library.deps`.

### 6. Regenerate ducktape Python bindings

Ducktape tests consume admin v2 via committed-to-repo Python bindings under `tests/rptest/clients/admin/proto/`. They are regenerated by a script — CI (`.github/workflows/check-ducktape-protos.yml`) fails the PR if they're stale, so this step is mandatory whenever a `.proto` under `proto/redpanda/core/admin/**` changes.

Prerequisite: `uv` installed (https://docs.astral.sh/uv/getting-started/installation/). The script aborts otherwise.

```bash
bazel fetch //proto/...          # one-time; populates the googleapis external dir
tools/regenerate_ducktape_protos.sh
```

The script wipes `tests/rptest/clients/admin/proto/**/*.py{,i}` and regenerates from the proto glob using `grpc_tools.protoc` + `mypy-protobuf` + `connect-python`, then runs `protoletariat` to rewrite imports to relative form. The glob picks up new `.proto` files automatically — **no manifest to update**.

Commit the generated files. `.gitignore` does not exclude them.

### 7. Add a ducktape client accessor (only for new services)

If you created a brand-new service, expose it from the `Admin` wrapper at `tests/rptest/clients/admin/v2.py`. Two edits:

1. Add imports near the top (line ~9), alongside the existing service imports:
   ```python
   from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
       <name>_pb2,
       <name>_pb2_connect,
       # ... existing
   )
   ```
   And add a convenience re-export (`<name>_pb = <name>_pb2`) in the re-export block (around line ~52).

2. Add an accessor method on `Admin` matching the existing pattern (line ~133):
   ```python
   def <name>(self, **kwargs: Any) -> <name>_pb2_connect.<Name>ServiceClient:
       return self._make_service(<name>_pb2_connect.<Name>ServiceClient, **kwargs)
   ```

Existing services (`broker`, `cluster`, `features`, `security`, `shadow_link`, `datalake`, `debug`, `breakglass`, `metastore`, `l0`, `internal_shadow_link`) show the shape. If you only added a new RPC to an existing service, no edit to `v2.py` is needed — `_make_service` returns the generated client which exposes every RPC as a method.

A ducktape test uses the new RPC like this:

```python
from rptest.clients.admin.v2 import Admin as AdminV2, <name>_pb

admin_v2 = AdminV2(self.redpanda,
                   auth=(self.superuser.username, self.superuser.password))
req = <name>_pb.<Rpc>Request(...)
resp = admin_v2.<name>().<rpc>(req)
```

## Verification

After all edits:

```bash
bazel build //...
bazel run //tools:clang_format
tools/regenerate_ducktape_protos.sh   # idempotent — confirms generated tree matches
git status                            # should show all expected files staged
```

Manual smoke (optional, but the canonical way to confirm wiring):

1. `bazel build //src/v/redpanda:application` or a debug-binary target.
2. Start a local cluster; invoke the RPC via the ducktape `AdminV2` client, or `curl` against `/redpanda.core.admin.v2.<Service>/<Method>` with a protobuf-encoded body.

## Out of scope

**Go bindings for `rpk` consumption.** Admin v2 Go bindings are not generated in this repo — they're pulled from the Buf Schema Registry as a Go module (`buf.build/gen/go/redpandadata/core/...`). CI publishes to BSR automatically on merge to `dev` or a `v*` branch (see `.github/workflows/buf.yml`). Consuming a new RPC from `rpk` is a follow-up PR after the proto change has merged and the BSR push has run — out of scope for this skill.

If the user mentions needing `rpk` to consume the new RPC, surface this as a separate follow-up task rather than attempting it in the same change.

## Rules and pitfalls

- **Auth level defaults to `SUPERUSER`.** Don't use `USER` or `PUBLIC` without explicit approval — the proto README is clear on this.
- **The proto's `option (pbgen.cpp_namespace)` is always `"proto::admin"` for admin v2.** Don't invent a per-service namespace.
- **`add_service` registration order matters only for readability.** The existing block in `application_admin.cc` is not strictly sorted; match its grouping (regular services, then `if (cloud_topics_app)` services, then internal/security at the end).
- **`bazel fetch //proto/...` is a prerequisite for `regenerate_ducktape_protos.sh`** — the script needs the googleapis external dir to exist. The script's error message is clear, but the failure mode (script aborts with "googleapis directory not found") is easy to misdiagnose if you skip it.
- **CI enforces generated-file freshness.** `check-ducktape-protos` will fail if you forget step 6. The fix is always: re-run the script and commit the diff.
- **Don't add comments that restate the code.** RPC docstrings in the `.proto` are valuable (they become the public contract); duplicating them in the C++ handler isn't.
- **The handler signature `ss::future<R> name(serde::pb::rpc::context, Request)` is fixed** — context first, request second, both by value, return future. Deviating produces inscrutable pbgen-template errors.
- **Validation errors must throw the typed exceptions from `src/v/serde/protobuf/rpc.h`.** Returning a default response, returning an `errc`, or throwing `std::runtime_error` will not surface a proper ConnectRPC status to the caller.
