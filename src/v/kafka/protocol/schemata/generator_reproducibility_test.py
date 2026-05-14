# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
"""Hermeticity regression test for the kafka schemata codegen.

The generator emits C++ source from JSON schemas. Earlier versions iterated
Python sets when emitting #include lines, so the byte-level output varied with
PYTHONHASHSEED (which CPython picks fresh per interpreter). Bazel keys its
action cache on input content hashes, so non-deterministic codegen invalidates
the cache for every downstream compile even when the result is
preprocessor-equivalent.

This test runs the generator with several PYTHONHASHSEED values and fails if
any output byte differs.
"""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_GENERATOR = _HERE / "generator.py"

_SEEDS = ("1", "4294967295")
_EXTS = ("h", "cc")

# Schemata chosen to exercise the codegen paths most likely to expose set-order
# bugs: ones whose fields pull in multiple `extra_headers` entries.
_SCHEMATA = (
    "create_topics_request",
    "create_topics_response",
    "fetch_request",
    "fetch_response",
    "metadata_response",
    "describe_configs_response",
)


def _run_generator(schema: str, seed: str, outdir: Path) -> dict[str, bytes]:
    """Run the generator once and return {ext: bytes} for the produced files."""
    outdir.mkdir()
    subprocess.run(
        [
            sys.executable,
            str(_GENERATOR),
            str(_HERE / f"{schema}.json"),
            *(str(outdir / f"{schema}.{ext}") for ext in _EXTS),
        ],
        check=True,
        env={**os.environ, "PYTHONHASHSEED": seed},
    )
    return {ext: (outdir / f"{schema}.{ext}").read_bytes() for ext in _EXTS}


class GeneratorReproducibilityTest(unittest.TestCase):
    def test_codegen_is_hash_seed_independent(self) -> None:
        for schema in _SCHEMATA:
            with self.subTest(schema=schema), tempfile.TemporaryDirectory() as tmp:
                outputs = [
                    _run_generator(schema, s, Path(tmp) / f"seed{s}") for s in _SEEDS
                ]
                for ext in _EXTS:
                    self.assertEqual(
                        {o[ext] for o in outputs},
                        {outputs[0][ext]},
                        f"{schema}.{ext} varies across PYTHONHASHSEED values",
                    )


if __name__ == "__main__":
    unittest.main()
