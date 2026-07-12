#!/usr/bin/env python3
"""Pick a random ducktape test from --collect-only output.

Reads ducktape's --collect-only output from stdin, parses each
test (including each @matrix parameterization) into a ducktape
selector, and picks one using the Antithesis SDK random_choice.
In AT this integrates with the branching model so each branch
explores a different test; outside AT it falls back to stdlib
random.

Ducktape selector format for parameterized tests:
    file::Class.method@{"key": "value", ...}
"""

import re
import sys


def parse_collected_tests(lines):
    """Yield ducktape test selectors from --collect-only output lines."""
    for line in lines:
        m = re.search(
            r"cls=([^,]+), function=([^,]+), injected_args=(\{[^}]+\}|None),"
            r".*file=([^,]+),",
            line,
        )
        if not m:
            continue
        cls, func, args_str, fpath = m.groups()
        fpath = fpath.removeprefix("/root/")
        selector = f"{fpath}::{cls}.{func}"

        if args_str.strip() != "None":
            # Convert Python repr to JSON that ducktape can match.
            # Enum reprs like <Foo.BAR: 1> use the integer value.
            args_json = args_str
            args_json = re.sub(r"<[^:>]+:\s*([^>]+)>", r"\1", args_json)
            args_json = args_json.replace("'", '"')
            args_json = re.sub(r"\bTrue\b", "true", args_json)
            args_json = re.sub(r"\bFalse\b", "false", args_json)
            selector += f"@{args_json}"

        yield selector


try:
    from antithesis.random import random_choice
except ImportError:
    import random

    random_choice = random.choice

tests = list(parse_collected_tests(sys.stdin))
if not tests:
    print("ERROR: No tests parsed from input", file=sys.stderr)
    sys.exit(1)

print(random_choice(tests))
