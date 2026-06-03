# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import unittest

import benchcmp as b


def metrics(median, mad, inst, allocs=0, tasks=0, runs=5):
    return b.CaseMetrics(
        median=median,
        mad=mad,
        allocs=allocs,
        tasks=tasks,
        inst=inst,
        cycles=0,
        runs=runs,
    )


class ParseTest(unittest.TestCase):
    def test_parse_flat_doubles(self):
        doc = {
            "results": {
                "g.case": {
                    "median": 412.0,
                    "mad": 3.0,
                    "min": 400,
                    "max": 500,
                    "allocs": 2,
                    "tasks": 1,
                    "inst": 1.21e6,
                    "cycles": 9e5,
                    "runs": 5,
                    "total_iterations": 5000,
                }
            }
        }
        out = b.parse_results(doc)
        self.assertEqual(out["g.case"].median, 412.0)
        self.assertEqual(out["g.case"].inst, 1.21e6)
        self.assertEqual(out["g.case"].runs, 5)

    def test_missing_fields_default_zero(self):
        out = b.parse_results({"results": {"g.c": {"median": 1.0}}})
        self.assertEqual(out["g.c"].inst, 0.0)
        self.assertEqual(out["g.c"].mad, 0.0)


class VerdictTest(unittest.TestCase):
    def test_inst_improved(self):
        d = b.diff_case(metrics(1000, 5, 1000), metrics(900, 5, 900), "inst")
        self.assertEqual(d.verdict, "improved")
        self.assertTrue(d.inst_ok and d.inst_sig)

    def test_inst_regressed(self):
        d = b.diff_case(metrics(900, 5, 900), metrics(1000, 5, 1000), "inst")
        self.assertEqual(d.verdict, "regressed")

    def test_inst_small_change_is_noise(self):
        d = b.diff_case(metrics(1000, 5, 1000), metrics(1005, 5, 1001), "inst")
        self.assertFalse(d.inst_sig)
        self.assertEqual(d.verdict, "noise")

    def test_inst_flat_ignores_wall_swing(self):
        # identical instruction count but wall time doubled: with metric=inst this
        # is noise (machine noise), not a regression.
        d = b.diff_case(metrics(1000, 5, 1000), metrics(2000, 5, 1000), "inst")
        self.assertTrue(d.rt_sig)
        self.assertEqual(d.verdict, "noise")

    def test_inst_unavailable_is_noise(self):
        # diff_case yields noise when inst is absent; the CLI errors before this.
        d = b.diff_case(metrics(1000, 5, 0), metrics(900, 5, 0), "inst")
        self.assertFalse(d.inst_ok)
        self.assertEqual(d.verdict, "noise")

    def test_runtime_metric(self):
        # same inst, wall-clock doubled: with metric=runtime this is a regression.
        d = b.diff_case(metrics(1000, 5, 1000), metrics(2000, 5, 1000), "runtime")
        self.assertEqual(d.verdict, "regressed")

    def test_runtime_within_mad_is_noise(self):
        # 20% slower but the MADs are huge, so it is not separable
        d = b.diff_case(metrics(1000, 400, 1000), metrics(1200, 400, 1000), "runtime")
        self.assertFalse(d.rt_sig)
        self.assertEqual(d.verdict, "noise")

    def test_allocs_metric(self):
        less = b.diff_case(
            metrics(1, 1, 1, allocs=10), metrics(1, 1, 1, allocs=8), "allocs"
        )
        self.assertEqual(less.verdict, "improved")
        more = b.diff_case(
            metrics(1, 1, 1, allocs=8), metrics(1, 1, 1, allocs=12), "allocs"
        )
        self.assertEqual(more.verdict, "regressed")
        same = b.diff_case(
            metrics(1, 1, 1, allocs=8), metrics(1, 1, 1, allocs=8), "allocs"
        )
        self.assertEqual(same.verdict, "noise")

    def test_tasks_metric(self):
        d = b.diff_case(metrics(1, 1, 1, tasks=3), metrics(1, 1, 1, tasks=5), "tasks")
        self.assertEqual(d.verdict, "regressed")


class FormatTest(unittest.TestCase):
    def setUp(self):
        b._COLOR = False  # deterministic, no ANSI escapes in assertions

    def test_fmt_runtime_units(self):
        self.assertEqual(b.fmt_runtime(412, 3), "412±3ns")
        self.assertEqual(b.fmt_runtime(85_100, 40), "85.10±0.04µs")
        self.assertEqual(b.fmt_runtime(2_410_000, 180_000), "2.41±0.18ms")

    def test_fmt_count(self):
        self.assertEqual(b.fmt_count(1_210_000), "1.21M")
        self.assertEqual(b.fmt_count(512), "512")
        self.assertEqual(b.fmt_count(8.88e9), "8.88G")

    def test_delta_markup_directions(self):
        self.assertEqual(b.delta_markup(-9.1, True), "-9.1% ▼")
        self.assertEqual(b.delta_markup(+14.2, True), "+14.2% ▲")
        self.assertEqual(b.delta_markup(+0.2, False), "+0.2% =")
        # a non-gating delta still shows direction but is never green/red
        self.assertEqual(b.delta_markup(-9.1, True, gating=False), "-9.1% ▼")

    def test_delta_markup_color(self):
        b._COLOR = True
        try:
            self.assertIn(b._CODES["green"], b.delta_markup(-9.1, True))
            self.assertIn(b._CODES["red"], b.delta_markup(+14.2, True))
            # non-gating is dimmed, not colored by direction
            self.assertNotIn(
                b._CODES["green"], b.delta_markup(-9.1, True, gating=False)
            )
        finally:
            b._COLOR = False

    def test_count_cell(self):
        self.assertEqual(b.count_cell(2, 2), "2 =")
        self.assertEqual(b.count_cell(8, 12), "12 +4 ▲")  # more allocs = regression
        self.assertEqual(b.count_cell(12, 8), "8 -4 ▼")

    def test_count_cell_non_gating_is_dim(self):
        b._COLOR = True
        try:
            self.assertNotIn(b._CODES["red"], b.count_cell(8, 12, gating=False))
            self.assertIn(b._CODES["red"], b.count_cell(8, 12, gating=True))
        finally:
            b._COLOR = False

    def test_visible_len_ignores_ansi(self):
        b._COLOR = True
        try:
            self.assertEqual(b._visible_len(b.paint("abc", "green")), 3)
        finally:
            b._COLOR = False


if __name__ == "__main__":
    unittest.main()
