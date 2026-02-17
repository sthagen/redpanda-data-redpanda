#!/usr/bin/env python3
"""Split log files by shard number.

Handles multiline logs by attributing continuation lines to the last seen shard.

Usage: python split_by_shard.py <logfile> [output_dir]
"""

import os
import re
import sys

SHARD_RE = re.compile(r"\[shard (\d+):")


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <logfile> [output_dir]", file=sys.stderr)
        sys.exit(1)

    logfile = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "."

    os.makedirs(output_dir, exist_ok=True)

    files = {}
    current_shard = None

    with open(logfile) as f:
        for line in f:
            m = SHARD_RE.search(line)
            if m:
                current_shard = m.group(1)
            if current_shard is None:
                continue
            if current_shard not in files:
                path = os.path.join(output_dir, f"shard-{current_shard}.log")
                files[current_shard] = open(path, "w")
            files[current_shard].write(line)

    for fh in files.values():
        fh.close()

    for shard in sorted(files, key=int):
        print(f"shard {shard} -> {files[shard].name}")


if __name__ == "__main__":
    main()
