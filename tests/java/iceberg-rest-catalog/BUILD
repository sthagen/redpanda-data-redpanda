genrule(
    name = "iceberg_rest_catalog_jar",
    srcs = glob(
        ["**"],
        exclude = [
            "BUILD",
            "build/**",
            ".gradle/**",
            "README.md",
        ],
    ),
    outs = ["iceberg-rest-catalog-all.jar"],
    cmd = """
        OUT_PATH=$$(realpath $(OUTS))
        SRC_DIR=$$(dirname $(location gradlew))
        cd $$SRC_DIR
        export GRADLE_USER_HOME="$$(pwd)/.gradle-home"
        ./gradlew --no-daemon shadowJar
        cp build/libs/iceberg-rest-catalog-all.jar $$OUT_PATH
    """,
    tags = ["requires-network"],
    visibility = ["//visibility:public"],
)
