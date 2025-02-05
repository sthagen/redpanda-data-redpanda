#!/bin/bash

# single_test_cov.sh
# ==================
#
# This script runs a single unit test with coverage profiling enabled
# and processes the output into an html report.
#
# It is useful for developers working on an individual test who would
# like to directly measure the coverage of the class they are testing.
#
# Usage (in your redpanda directory):
#
#  single_test_cov.sh //src/v/path/to/test
set -e

bazel coverage $*
COVERAGE_REPORT="$(bazel info output_path)/_coverage/_coverage_report.dat"
genhtml --branch-coverage --output genhtml "$(bazel info output_path)/_coverage/_coverage_report.dat"
