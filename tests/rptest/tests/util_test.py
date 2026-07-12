# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from rptest.util import ConditionNotHeldError, check_consistently, expect_exception


class CheckConsistentlyTest(Test):
    @cluster(num_nodes=0)
    def test_holds_across_window(self) -> None:
        # A condition that stays true returns without raising, polled repeatedly.
        calls = 0

        def cond() -> bool:
            nonlocal calls
            calls += 1
            return True

        check_consistently(cond, duration_sec=0.1, interval_sec=0.005)
        assert calls >= 2

    @cluster(num_nodes=0)
    def test_slow_poll_forces_a_check_past_the_deadline(self) -> None:
        calls = 0

        def cond() -> bool:
            nonlocal calls
            calls += 1
            if calls == 2:
                # Begins inside the window but returns well after it.
                time.sleep(0.5)
            return True

        check_consistently(cond, duration_sec=0.1, interval_sec=0)
        # The slow second poll started before the deadline, so success requires
        # a third poll -- one that begins after the deadline.
        assert calls == 3

    @cluster(num_nodes=0)
    def test_raises_when_condition_becomes_false(self) -> None:
        # False from the start: fails on the first poll, before sleeping.
        calls = 0

        def false_from_start() -> bool:
            nonlocal calls
            calls += 1
            return False

        with expect_exception(ConditionNotHeldError, lambda _: True):
            check_consistently(false_from_start, duration_sec=5, interval_sec=0.01)
        assert calls == 1

        # Flips to false mid-window: raises as soon as it does.
        calls = 0

        def flips() -> bool:
            nonlocal calls
            calls += 1
            return calls < 3

        with expect_exception(ConditionNotHeldError, lambda _: True):
            check_consistently(flips, duration_sec=5, interval_sec=0.001)
        assert calls == 3

    @cluster(num_nodes=0)
    def test_check_failure_is_still_an_assertion_error(self) -> None:
        with expect_exception(AssertionError, lambda _: True):
            check_consistently(lambda: False, duration_sec=5, interval_sec=0.01)
