import re
from typing import Any

from .samples import Exemplar, Sample, Timestamp

METRIC_TYPES: tuple[str, ...]
METRIC_NAME_RE: re.Pattern[str]
METRIC_LABEL_NAME_RE: re.Pattern[str]
RESERVED_METRIC_LABEL_NAME_RE: re.Pattern[str]

class Metric:
    """A single metric family and its samples.

    This is intended only for internal use by the instrumentation client.

    Custom collectors should use GaugeMetricFamily, CounterMetricFamily
    and SummaryMetricFamily instead.
    """

    name: str
    documentation: str
    unit: str
    type: str
    samples: list[Sample]

    def __init__(
        self, name: str, documentation: str, typ: str, unit: str = ""
    ) -> None: ...
    def add_sample(
        self,
        name: str,
        labels: dict[str, str],
        value: int | float,
        timestamp: float | Timestamp | None = None,
        exemplar: Exemplar | None = None,
    ) -> None:
        """Add a sample to the metric.

        Internal-only, do not use."""
        ...
    def __eq__(self, other: object) -> bool: ...
    def __repr__(self) -> str: ...

class UnknownMetricFamily(Metric):
    """A single unknown metric and its samples.
    For use by custom collectors.
    """

    _labelnames: tuple[str, ...]

    def __init__(
        self,
        name: str,
        documentation: str,
        value: int | float | None = None,
        labels: list[str] | None = None,
        unit: str = "",
    ) -> None: ...
    def add_metric(
        self,
        labels: list[str],
        value: int | float,
        timestamp: float | Timestamp | None = None,
    ) -> None:
        """Add a metric to the metric family.
        Args:
        labels: A list of label values
        value: The value of the metric.
        """
        ...

# For backward compatibility.
UntypedMetricFamily = UnknownMetricFamily

class CounterMetricFamily(Metric):
    """A single counter and its samples.

    For use by custom collectors.
    """

    _labelnames: tuple[str, ...]

    def __init__(
        self,
        name: str,
        documentation: str,
        value: int | float | None = None,
        labels: list[str] | None = None,
        created: float | None = None,
        unit: str = "",
    ) -> None: ...
    def add_metric(
        self,
        labels: list[str],
        value: int | float,
        created: float | None = None,
        timestamp: float | Timestamp | None = None,
    ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          value: The value of the metric
          created: Optional unix timestamp the child was created at.
        """
        ...

class GaugeMetricFamily(Metric):
    """A single gauge and its samples.

    For use by custom collectors.
    """

    _labelnames: tuple[str, ...]

    def __init__(
        self,
        name: str,
        documentation: str,
        value: int | float | None = None,
        labels: list[str] | None = None,
        unit: str = "",
    ) -> None: ...
    def add_metric(
        self,
        labels: list[str],
        value: int | float,
        timestamp: float | Timestamp | None = None,
    ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          value: A float
        """
        ...

class SummaryMetricFamily(Metric):
    """A single summary and its samples.

    For use by custom collectors.
    """

    _labelnames: tuple[str, ...]

    def __init__(
        self,
        name: str,
        documentation: str,
        count_value: int | float | None = None,
        sum_value: int | float | None = None,
        labels: list[str] | None = None,
        unit: str = "",
    ) -> None: ...
    def add_metric(
        self,
        labels: list[str],
        count_value: int | float,
        sum_value: int | float,
        timestamp: float | Timestamp | None = None,
    ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          count_value: The count value of the metric.
          sum_value: The sum value of the metric.
        """
        ...

class HistogramMetricFamily(Metric):
    """A single histogram and its samples.

    For use by custom collectors.
    """

    _labelnames: tuple[str, ...]

    def __init__(
        self,
        name: str,
        documentation: str,
        buckets: list[list[Any]] | None = None,
        sum_value: int | float | None = None,
        labels: list[str] | None = None,
        unit: str = "",
    ) -> None: ...
    def add_metric(
        self,
        labels: list[str],
        buckets: list[list[Any]],
        sum_value: int | float,
        timestamp: float | Timestamp | None = None,
    ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          buckets: A list of lists.
              Each inner list can be a pair of bucket name and value,
              or a triple of bucket name, value, and exemplar.
              The buckets must be sorted, and +Inf present.
          sum_value: The sum value of the metric.
        """
        ...

class GaugeHistogramMetricFamily(Metric):
    """A single gauge histogram and its samples.

    For use by custom collectors.
    """

    _labelnames: tuple[str, ...]

    def __init__(
        self,
        name: str,
        documentation: str,
        buckets: list[tuple[str, int | float]] | None = None,
        gsum_value: int | float | None = None,
        labels: list[str] | None = None,
        unit: str = "",
    ) -> None: ...
    def add_metric(
        self,
        labels: list[str],
        buckets: list[tuple[str, int | float]],
        gsum_value: int | float,
        timestamp: float | Timestamp | None = None,
    ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          buckets: A list of pairs of bucket names and values.
              The buckets must be sorted, and +Inf present.
          gsum_value: The sum value of the metric.
        """
        ...

class InfoMetricFamily(Metric):
    """A single info and its samples.

    For use by custom collectors.
    """

    _labelnames: tuple[str, ...]

    def __init__(
        self,
        name: str,
        documentation: str,
        value: dict[str, str] | None = None,
        labels: list[str] | None = None,
    ) -> None: ...
    def add_metric(
        self,
        labels: list[str],
        value: dict[str, str],
        timestamp: float | Timestamp | None = None,
    ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          value: A dict of labels
        """
        ...

class StateSetMetricFamily(Metric):
    """A single stateset and its samples.

    For use by custom collectors.
    """

    _labelnames: tuple[str, ...]

    def __init__(
        self,
        name: str,
        documentation: str,
        value: dict[str, bool] | None = None,
        labels: list[str] | None = None,
    ) -> None: ...
    def add_metric(
        self,
        labels: list[str],
        value: dict[str, bool],
        timestamp: float | Timestamp | None = None,
    ) -> None:
        """Add a metric to the metric family.

        Args:
          labels: A list of label values
          value: A dict of string state names to booleans
        """
        ...
