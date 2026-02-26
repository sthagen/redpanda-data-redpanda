# Iceberg test data

Data generated for use in tests.

```
RP_HOME=$(git rev-parse --show-toplevel)
uv run $RP_HOME/src/v/iceberg/tests/gen_test_iceberg_manifest.py -o $RP_HOME/src/v/iceberg/tests/testdata/nested_manifest.avro -n 100
```
