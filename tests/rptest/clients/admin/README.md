## Redpanda Admin API Client

Python ConnectRPC client for the admin v2 API, used by ducktape tests. The proto bindings are generated and committed; regenerate them after any change under `proto/redpanda/core/admin/**`:

```
./tools/regenerate_ducktape_protos.sh
```

For the end-to-end process of adding a new admin v2 endpoint (proto authoring, C++ service impl, Bazel wiring, and the regen step above), use the `add-admin-v2-endpoint` skill.
