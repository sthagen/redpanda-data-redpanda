#!/usr/bin/env python3
"""
Convert LSM trace logs to Chrome trace JSON format.

Usage:
    ./logs_to_trace.py < logfile.log > trace.json
    ./logs_to_trace.py logfile.log > trace.json

Open the output in chrome://tracing or https://ui.perfetto.dev/
"""

import json
import re
import sys
from datetime import datetime


# Log format: LEVEL TIMESTAMP [shard N:CONTEXT] LOGGER - FILE:LINE - MESSAGE
LOG_PATTERN = re.compile(
    r"^(?P<level>\w+)\s+"
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+"
    r"\[shard (?P<shard>\d+):\w+\]\s+"
    r"lsm\s+-\s+"
    r"(?P<file>\w+\.cc):(?P<line>\d+)\s+-\s+"
    r"(?P<message>.+)$"
)


def parse_timestamp(ts_str: str) -> int:
    """Parse seastar log timestamp to microseconds since epoch."""
    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
    return int(dt.timestamp() * 1_000_000)


def parse_attrs(message: str) -> dict[str, str]:
    """Parse key=value attributes from message, supporting quoted values."""
    attrs = {}
    # Match key=value or key="quoted value"
    for match in re.finditer(r'(\w+)=(?:"([^"]*)"|(\S+))', message):
        k = match.group(1)
        v = match.group(2) if match.group(2) is not None else match.group(3)
        try:
            attrs[k] = int(v)
        except ValueError:
            attrs[k] = v
    return attrs


def main():
    # Read input
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            lines = f.readlines()
    else:
        lines = sys.stdin.readlines()

    events = []

    for line in lines:
        match = LOG_PATTERN.match(line.strip())
        if not match:
            continue

        msg = match.group("message")
        parts = msg.split()
        if not parts:
            continue

        op = parts[0]

        # Determine phase: B=begin, E=end, i=instant
        if op.endswith("_start"):
            name = op[:-6]
            ph = "B"
        elif op.endswith("_end"):
            name = op[:-4]
            ph = "E"
        else:
            # Instant event for point-in-time operations
            name = op
            ph = "i"

        event = {
            "name": name,
            "cat": "lsm",
            "ph": ph,
            "ts": parse_timestamp(match.group("timestamp")),
            "pid": 1,
            "tid": int(match.group("shard")),
            "args": parse_attrs(msg),
        }

        # Instant events need a scope (g=global, p=process, t=thread)
        if ph == "i":
            event["s"] = "t"  # Thread-scoped instant event

        events.append(event)

    print(json.dumps({"traceEvents": events}, indent=2))


if __name__ == "__main__":
    main()
