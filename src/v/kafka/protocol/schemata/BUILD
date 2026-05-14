load("@rules_python//python:defs.bzl", "py_binary", "py_test")
load("//src/v/kafka/protocol/schemata:generator.bzl", "generate_kafka_messages")

py_binary(
    name = "generator",
    srcs = ["generator.py"],
    main = "generator.py",
    visibility = ["//visibility:public"],
    deps = [
        "@python_deps//jinja2",
        "@python_deps//jsonschema",
    ],
)

py_test(
    name = "generator_reproducibility_test",
    size = "small",
    srcs = ["generator_reproducibility_test.py"],
    data = [
        "create_topics_request.json",
        "create_topics_response.json",
        "describe_configs_response.json",
        "fetch_request.json",
        "fetch_response.json",
        "generator.py",
        "metadata_response.json",
    ],
    deps = [
        "@python_deps//jinja2",
        "@python_deps//jsonschema",
    ],
)

generate_kafka_messages()
