# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Thin wrapper around the Antithesis Python SDK. When the SDK is not
# installed (e.g. running tests locally via tools/dt), the assertion
# functions become no-ops so tests still execute and validate via
# regular Python asserts.

from __future__ import annotations

import logging
import os
import threading
import time
import functools
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, Mapping, cast

from ducktape.tests.test import TestContext

from rptest.services.redpanda import RedpandaService
from rptest.tests.redpanda_test import RedpandaTest

_at_log = logging.getLogger(__name__)

try:
    # The SDK is intentionally not a test dependency, so only its type stub is
    # present at check time; silence the "stub without source" warning.
    from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
        always as always,
        always_or_unreachable as always_or_unreachable,
        sometimes as sometimes,
        reachable as reachable,
        unreachable as unreachable,
    )
# Local fallback: turn safety assertions into Python asserts so the test
# still fails on a violation. sometimes/reachable have no single-run
# meaning (they're satisfied across the run set), so they stay no-ops.
except ImportError:
    _at_log.info("Antithesis SDK not available — using assert fallbacks")

    def always(condition: bool, message: str, details: Mapping[str, Any]) -> None:
        assert condition, f"{message} {details}"

    def always_or_unreachable(
        condition: bool, message: str, details: Mapping[str, Any]
    ) -> None:
        assert condition, f"{message} {details}"

    def sometimes(condition: bool, message: str, details: Mapping[str, Any]) -> None:
        pass

    def reachable(message: str, details: Mapping[str, Any]) -> None:
        pass

    def unreachable(message: str, details: Mapping[str, Any]) -> None:
        raise AssertionError(f"{message} {details}")


def retry_call(
    fn: Callable[..., Any],
    attempts: int,
    sleep_sec: float,
    label: str,
    logger: logging.Logger = _at_log,
) -> Callable[..., Any]:
    """Wrap fn with a bounded retry loop. Re-raises on the final failure;
    sleeps between attempts when sleep_sec > 0. Used to harden cluster
    bootstrap calls that flake transiently under AT fault injection."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        for attempt in range(attempts):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                if attempt == attempts - 1:
                    raise
                logger.warning(f"{label} attempt {attempt + 1} failed: {e}, retrying")
                if sleep_sec:
                    time.sleep(sleep_sec)
        raise RuntimeError("retry_call: unreachable")

    return wrapper


class AntithesisFaultManager:
    """Process-level singleton wrapping the Antithesis container-fault
    marker-file API. A marker file at
    $ANTITHESIS_CONTAINER_FAULTS/<category>/<container> pauses faults
    of that category for that container; deleting it resumes. The AT
    helper watches the directory with inotify so pause/resume take
    effect immediately. No-op outside the Antithesis environment.

    All access is via class methods to avoid divergent per-instance
    container lists fighting over the same shared filesystem state.
    Use AntithesisFaultManager.paused(...) as a context manager, or call
    pause()/resume() explicitly from lifecycle hooks that don't pair
    in a single scope.
    """

    NETWORK = "exclude-network"
    CPU = "exclude-cpu"
    STATUS = "exclude-status"
    ALL = "exclude-all"

    _LOCK = threading.Lock()

    @classmethod
    def enabled(cls) -> bool:
        return os.environ.get("ANTITHESIS_CONTAINER_FAULTS") is not None

    @classmethod
    def _excl_dir(cls, category: str) -> str | None:
        base = os.environ.get("ANTITHESIS_CONTAINER_FAULTS")
        if base is None:
            return None
        d = os.path.join(base, category)
        # The AT-side helper sets up the category directories at startup
        # and watches them with inotify. If the directory is missing the
        # helper isn't running, so silently touching markers would do
        # nothing — fail loudly instead of suppressing.
        if not os.path.isdir(d):
            raise RuntimeError(
                f"AntithesisFaultManager: {d} not present; AT helper not initialized"
            )
        return d

    @classmethod
    def pause(cls, containers: Iterable[str], category: str = NETWORK) -> None:
        d = cls._excl_dir(category)
        if d is None:
            return
        with cls._LOCK:
            for name in containers:
                path = Path(d) / name
                try:
                    path.touch()
                except Exception as e:
                    _at_log.warning(f"create {path} failed: {e}")

    @classmethod
    def resume(cls, containers: Iterable[str], category: str = NETWORK) -> None:
        d = cls._excl_dir(category)
        if d is None:
            return
        with cls._LOCK:
            for name in containers:
                path = os.path.join(d, name)
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
                except Exception as e:
                    _at_log.warning(f"remove {path} failed: {e}")

    @classmethod
    @contextmanager
    def paused(
        cls, containers: Iterable[str], category: str = NETWORK
    ) -> Iterator[None]:
        cls.pause(containers, category)
        try:
            yield
        finally:
            cls.resume(containers, category)


class AntithesisTimeoutMixin:
    """Mixin for Antithesis ducktape tests. Increases timeouts for
    fault injection and sets storage_min_free_bytes for the constrained
    AT disk environment (10GB shared across all nodes).

    Must be listed before RedpandaTest in the inheritance chain:

        class MyTest(AntithesisTimeoutMixin, RedpandaTest):
            ...
    """

    ANTITHESIS_STORAGE_MIN_FREE_BYTES = 10000000  # 10MB

    # These must be provided by the mixin's environment (i.e. RedpandaTest,
    # which must come after this mixin in the inheritance chain). logger is a
    # property on ducktape's Test, so it must be declared as one here too to
    # avoid an incompatible-override against that base.
    test_context: TestContext
    redpanda: RedpandaService

    if TYPE_CHECKING:

        @property
        def logger(self) -> logging.Logger: ...

    def __init__(
        self, *args: Any, extra_rp_conf: dict[str, Any] | None = None, **kwargs: Any
    ) -> None:
        if extra_rp_conf is None:
            extra_rp_conf = {}
        extra_rp_conf.setdefault(
            "storage_min_free_bytes", self.ANTITHESIS_STORAGE_MIN_FREE_BYTES
        )
        cast(RedpandaTest, super()).__init__(
            *args, extra_rp_conf=extra_rp_conf, **kwargs
        )

    def _container_names(self) -> tuple[str, ...]:
        """Hostnames of every container hosting a registered ducktape
        service in this test. Includes the redpanda brokers and any
        client services (e.g. kgo-verifier producer/consumer). Infra
        containers (minio-s3, ducktape-runner) are excluded — they
        aren't ducktape services and already have faults disabled."""
        names: set[str] = set()
        for svc in self.test_context.services:
            for node in svc.nodes:
                names.add(node.account.hostname)
        return tuple(sorted(names))

    def setUp(self) -> None:
        # Increase membership timeout for AT fault injection
        orig_wait = self.redpanda.wait_for_membership

        def _wait_with_timeout(first_start: bool, timeout_sec: int = 180) -> None:
            orig_wait(first_start, timeout_sec=timeout_sec)

        self.redpanda.wait_for_membership = _wait_with_timeout

        # Retry create_user — admin API can be slow under faults.
        orig_create = self.redpanda._admin.create_user

        def _create_user(*args: Any, **kwargs: Any) -> Any:
            kwargs.setdefault("await_exists", True)
            return orig_create(*args, **kwargs)

        self.redpanda._admin.create_user = retry_call(
            _create_user,
            attempts=3,
            sleep_sec=0,
            label="create_user",
            logger=self.logger,
        )

        # Retry validate_metastore — under AT faults the metastore can
        # report UNAVAILABLE long after network faults pause (cloud-topics
        # scheduler/leader recovery is in-process, not network). The
        # function's own retry budget (5×1s) isn't always enough.
        self.redpanda.validate_metastore = retry_call(
            self.redpanda.validate_metastore,
            attempts=6,
            sleep_sec=5,
            label="validate_metastore",
            logger=self.logger,
        )

        # Pause network faults for the duration of setUp so cluster
        # bootstrap (membership, create_user, topic create) runs without
        # interference. Resumed on exit so faults are active during the
        # test method body.
        with AntithesisFaultManager.paused(self._container_names()):
            cast(RedpandaTest, super()).setUp()

    def post_test_checks(self, test_name: str, test_passed: bool) -> None:
        """Called by @cluster in its finally block."""
        always_or_unreachable(
            test_passed, f"{test_name} passes", {"test_name": test_name}
        )

        # Resume faults so the next test starts with faults active.
        AntithesisFaultManager.resume(self._container_names())

    def pre_test_checks(self) -> None:
        """Called by @cluster before post-test checks. Pauses network
        faults and waits for the cluster to be ready so teardown runs
        on a healthy cluster. Markers are removed in post_test_checks.
        No-op outside the AT environment."""
        if not AntithesisFaultManager.enabled():
            return

        AntithesisFaultManager.pause(self._container_names())

        try:
            self.redpanda.wait_for_membership(first_start=False, timeout_sec=30)
        except Exception as e:
            _at_log.warning(f"Membership wait failed: {e}")

        # Wait until the controller has a leader and every started
        # broker's admin API answers. These are the dependencies of
        # the unwrapped post-test calls (validate_controller_log,
        # scrub paths).
        started = self.redpanda.started_nodes()
        admin = self.redpanda._admin

        def _ready() -> bool:
            try:
                if (
                    admin.get_partition_leader(
                        namespace="redpanda", topic="controller", partition=0
                    )
                    < 0
                ):
                    return False
                for node in started:
                    admin.get_brokers(node=node)
                return True
            except Exception:
                return False

        try:
            self.redpanda.wait_until(
                _ready,
                timeout_sec=60,
                backoff_sec=2,
                err_msg="cluster not ready for post-test checks",
            )
        except Exception as e:
            _at_log.warning(f"Readiness wait failed: {e}")

    def create_topic(
        self,
        name: str,
        partitions: int = 3,
        replicas: int = 3,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Create a topic with retries for AT fault injection.
        Tolerates TOPIC_ALREADY_EXISTS since multiple test methods in
        the same class may attempt to create the same topic."""
        from rptest.clients.rpk import RpkTool, RpkException

        rpk = RpkTool(self.redpanda)

        def _try_create() -> bool:
            try:
                rpk.create_topic(
                    topic=name,
                    partitions=partitions,
                    replicas=replicas,
                    config=config or {},
                )
            except RpkException as e:
                if "TOPIC_ALREADY_EXISTS" in str(e):
                    return True
                raise
            return True

        self.redpanda.wait_until(
            _try_create,
            timeout_sec=120,
            backoff_sec=5,
            err_msg=f"Failed to create topic {name}",
            retry_on_exc=True,
        )


def monitor_leadership(
    admin: Any,
    topic: str,
    partition_count: int,
    rounds: int,
    interval_sec: int,
    logger: logging.Logger,
) -> tuple[int, int, int, set[int]]:
    """Poll leadership state.

    Uses the bulk get_partitions(topic) API — one HTTP call per round
    instead of one per partition.

    Returns (leadership_changes_seen, rounds_with_all_leaders,
    total_rounds, ever_had_leader). Antithesis assertions are emitted
    by the caller so the property is registered only with the test
    that actually exercises the polling loop.
    """
    prev_leaders: dict[int, int | None] = {}
    ever_had_leader: set[int] = set()
    leadership_changes_seen = 0
    rounds_with_all_leaders = 0

    for _ in range(rounds):
        try:
            all_partitions: list[dict[str, Any]] = admin.get_partitions(topic)
        except Exception:
            all_partitions = []

        partition_map: dict[int, int] = {}
        for info in all_partitions:
            p_id: int = info.get("partition_id", info.get("id", -1))
            partition_map[p_id] = info.get("leader_id", -1)

        partitions_with_leader = 0
        for p_id in range(partition_count):
            leader_id = partition_map.get(p_id, -1)
            has_leader = leader_id >= 0
            if has_leader:
                partitions_with_leader += 1
                ever_had_leader.add(p_id)

            prev = prev_leaders.get(p_id)
            if prev is not None and has_leader and leader_id != prev:
                leadership_changes_seen += 1

            prev_leaders[p_id] = leader_id if has_leader else None

        if partitions_with_leader == partition_count:
            rounds_with_all_leaders += 1

        time.sleep(interval_sec)

    return leadership_changes_seen, rounds_with_all_leaders, rounds, ever_had_leader
