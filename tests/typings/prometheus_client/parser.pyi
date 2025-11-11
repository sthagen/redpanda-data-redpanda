import re
from collections.abc import Generator
from typing import TextIO

from .metrics_core import Metric

def text_string_to_metric_families(text: str) -> Generator[Metric, None, None]:
    """Parse Prometheus text format from a unicode string.

    See text_fd_to_metric_families.
    """
    ...

ESCAPE_SEQUENCES: dict[str, str]

def replace_escape_sequence(match: re.Match[str]) -> str: ...

HELP_ESCAPING_RE: re.Pattern[str]
ESCAPING_RE: re.Pattern[str]

def text_fd_to_metric_families(fd: TextIO) -> Generator[Metric, None, None]:
    """Parse Prometheus text format from a file descriptor.

    This is a laxer parser than the main Go parser,
    so successful parsing does not imply that the parsed
    text meets the specification.

    Yields Metric's.
    """
    ...
