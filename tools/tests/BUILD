load("@rules_shell//shell:sh_test.bzl", "sh_test")

sh_test(
    name = "run_cov_test",
    size = "small",
    timeout = "short",
    srcs = ["run_cov_test.sh"],
    args = [
        "$(rootpath //tools:run-cov)",
        "$(rootpath //tools/tests/testdata:coverage_fixture.dat)",
        "$(rootpath //tools/tests/testdata:sample.diff)",
    ],
    data = [
        "//tools:run-cov",
        "//tools/tests/testdata:coverage_fixture.dat",
        "//tools/tests/testdata:sample.diff",
    ],
)
