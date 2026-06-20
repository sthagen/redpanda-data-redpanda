// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/types.h"
#include "redpanda/tests/fixture.h"
#include "schema/registry.h"

#include <gtest/gtest.h>

#include <limits>
#include <optional>

namespace pps = pandaproxy::schema_registry;

class internal_writes_fixture
  : public redpanda_thread_fixture
  , public ::testing::Test {
public:
    internal_writes_fixture() {
        wait_for_controller_leadership().get();
        registry = schema::registry::make_default(app.schema_registry().get());
    }

    pps::context_schema_id import(pps::stored_schema schema) {
        return registry->import_schema(std::move(schema)).get();
    }

    std::error_code import_error_code(pps::stored_schema schema) {
        try {
            registry->import_schema(std::move(schema)).get();
        } catch (const pps::exception& e) {
            return e.code();
        }
        ADD_FAILURE() << "import_schema unexpectedly succeeded";
        return {};
    }

    std::error_code get_subject_schema_error(
      pps::context_subject sub, std::optional<pps::schema_version> version) {
        try {
            registry->get_subject_schema(std::move(sub), version).get();
        } catch (const pps::exception& e) {
            return e.code();
        }
        ADD_FAILURE() << "get_subject_schema unexpectedly succeeded";
        return {};
    }

    std::unique_ptr<schema::registry> registry;
};

TEST_F(internal_writes_fixture, imports_id_and_version) {
    auto subject = pps::context_subject::unqualified("internal-import");
    auto schema_def = pps::schema_definition(
      R"({"type":"record","name":"r1","fields":[{"name":"f1","type":"string"}]})",
      pps::schema_type::avro);

    ASSERT_TRUE(registry->write_mode(subject, pps::mode::read_only).get());

    auto ctx_id = registry
                    ->import_schema(
                      pps::stored_schema{
                        .schema = {subject, schema_def.share()},
                        .version = pps::schema_version{7},
                        .id = pps::schema_id{42},
                        .deleted = pps::is_deleted::no})
                    .get();
    ASSERT_TRUE(ctx_id.ctx == pps::default_context);
    ASSERT_EQ(ctx_id.id, pps::schema_id{42});

    auto stored
      = registry->get_subject_schema(subject, pps::schema_version{7}).get();
    ASSERT_EQ(stored.version, pps::schema_version{7});
    ASSERT_EQ(stored.id, pps::schema_id{42});
    ASSERT_TRUE(stored.deleted == pps::is_deleted::no);

    auto duplicate = registry
                       ->import_schema(
                         pps::stored_schema{
                           .schema = {subject, schema_def.share()},
                           .version = pps::schema_version{7},
                           .id = pps::schema_id{42},
                           .deleted = pps::is_deleted::no})
                       .get();
    ASSERT_EQ(duplicate.id, pps::schema_id{42});
}

TEST_F(internal_writes_fixture, import_rejects_conflicts) {
    auto subject = pps::context_subject::unqualified("internal-conflict");
    auto schema_def = pps::schema_definition(
      R"("int")", pps::schema_type::avro);
    auto other_def = pps::schema_definition(
      R"("string")", pps::schema_type::avro);

    registry
      ->import_schema(
        pps::stored_schema{
          .schema = {subject, schema_def.share()},
          .version = pps::schema_version{1},
          .id = pps::schema_id{9},
          .deleted = pps::is_deleted::no})
      .get();

    ASSERT_EQ(
      import_error_code(
        pps::stored_schema{
          .schema = {subject, schema_def.share()},
          .version = pps::schema_version{1},
          .id = pps::schema_id{10},
          .deleted = pps::is_deleted::no}),
      pps::error_code::subject_version_schema_id_already_exists);

    ASSERT_EQ(
      import_error_code(pps::stored_schema{
        .schema
        = {pps::context_subject::unqualified("internal-conflict-other"),
           other_def.share()},
        .version = pps::schema_version{1},
        .id = pps::schema_id{9},
        .deleted = pps::is_deleted::no}),
      pps::error_code::subject_version_operation_not_permitted);
}

TEST_F(internal_writes_fixture, import_rejects_invalid_ids) {
    auto subject = pps::context_subject::unqualified("internal-invalid-id");
    auto schema_def = pps::schema_definition(
      R"("int")", pps::schema_type::avro);

    // Ids come from an external Schema Registry: non-positive ids must be
    // rejected, as must INT32_MAX, whose successor is not representable.
    for (auto id :
         {pps::schema_id{-1},
          pps::schema_id{0},
          pps::schema_id{std::numeric_limits<pps::schema_id::type>::max()}}) {
        ASSERT_EQ(
          import_error_code(
            pps::stored_schema{
              .schema = {subject, schema_def.share()},
              .version = pps::schema_version{1},
              .id = id,
              .deleted = pps::is_deleted::no}),
          pps::error_code::schema_invalid);
    }

    // Versions are equally external input: valid versions are
    // 1..INT32_MAX-1, since later writes to the subject compute version + 1.
    for (auto version :
         {pps::schema_version{0},
          pps::schema_version{-1},
          pps::schema_version{
            std::numeric_limits<pps::schema_version::type>::max()}}) {
        ASSERT_EQ(
          import_error_code(
            pps::stored_schema{
              .schema = {subject, schema_def.share()},
              .version = version,
              .id = pps::schema_id{1},
              .deleted = pps::is_deleted::no}),
          pps::error_code::schema_version_invalid);
    }

    try {
        registry->get_subject_schema(subject, std::nullopt).get();
        FAIL() << "expected subject_not_found";
    } catch (const pps::exception& e) {
        ASSERT_EQ(e.code(), pps::error_code::subject_not_found);
    }

    // Boundary acceptance: the largest valid id and version are accepted.
    constexpr auto max_id = pps::schema_id{
      std::numeric_limits<pps::schema_id::type>::max() - 1};
    constexpr auto max_version = pps::schema_version{
      std::numeric_limits<pps::schema_version::type>::max() - 1};
    auto boundary_subject = pps::context_subject::unqualified(
      "internal-boundary-id");
    auto ctx_id = registry
                    ->import_schema(
                      pps::stored_schema{
                        .schema = {boundary_subject, schema_def.share()},
                        .version = max_version,
                        .id = max_id,
                        .deleted = pps::is_deleted::no})
                    .get();
    ASSERT_EQ(ctx_id.id, max_id);
    auto stored
      = registry->get_subject_schema(boundary_subject, max_version).get();
    ASSERT_EQ(stored.id, max_id);
}

TEST_F(internal_writes_fixture, deletes_bypass_readonly) {
    auto subject = pps::context_subject::unqualified("internal-delete");
    auto schema_def = pps::schema_definition(
      R"("int")", pps::schema_type::avro);

    registry
      ->import_schema(
        pps::stored_schema{
          .schema = {subject, schema_def.share()},
          .version = pps::schema_version{1},
          .id = pps::schema_id{77},
          .deleted = pps::is_deleted::no})
      .get();
    registry->write_mode(subject, pps::mode::read_only).get();

    ASSERT_TRUE(
      registry->soft_delete_schema(subject, pps::schema_version{1}).get());
    auto deleted = registry
                     ->permanent_delete_schema(subject, pps::schema_version{1})
                     .get();
    ASSERT_EQ(deleted.size(), 1);
    ASSERT_EQ(deleted.front(), pps::schema_version{1});

    ASSERT_THROW(
      registry->get_subject_schema(subject, pps::schema_version{1}).get(),
      pps::exception);
}

TEST_F(internal_writes_fixture, imports_preserve_deleted_state) {
    auto subject = pps::context_subject::unqualified("internal-deleted-state");
    auto schema_def = pps::schema_definition(
      R"("int")", pps::schema_type::avro);
    auto other_def = pps::schema_definition(
      R"("string")", pps::schema_type::avro);

    // Import a version that was already soft-deleted at the source.
    import(
      pps::stored_schema{
        .schema = {subject, schema_def.share()},
        .version = pps::schema_version{1},
        .id = pps::schema_id{5},
        .deleted = pps::is_deleted::yes});

    // Soft-deleted versions are invisible to ordinary reads: the subject's only
    // version is deleted, so the subject itself reads as not found.
    ASSERT_EQ(
      get_subject_schema_error(subject, pps::schema_version{1}),
      pps::error_code::subject_not_found);

    // ...but the id slot stays occupied: reusing the same subject/version/id
    // with a different definition must conflict, even while soft-deleted.
    ASSERT_EQ(
      import_error_code(
        pps::stored_schema{
          .schema = {subject, other_def.share()},
          .version = pps::schema_version{1},
          .id = pps::schema_id{5},
          .deleted = pps::is_deleted::yes}),
      pps::error_code::subject_version_operation_not_permitted);

    // Source un-deletes the version: re-importing with deleted::no makes it
    // readable again under the same id.
    import(
      pps::stored_schema{
        .schema = {subject, schema_def.share()},
        .version = pps::schema_version{1},
        .id = pps::schema_id{5},
        .deleted = pps::is_deleted::no});
    auto stored
      = registry->get_subject_schema(subject, pps::schema_version{1}).get();
    ASSERT_EQ(stored.id, pps::schema_id{5});
    ASSERT_TRUE(stored.deleted == pps::is_deleted::no);

    // Source soft-deletes again: the version disappears from ordinary reads.
    import(
      pps::stored_schema{
        .schema = {subject, schema_def.share()},
        .version = pps::schema_version{1},
        .id = pps::schema_id{5},
        .deleted = pps::is_deleted::yes});
    ASSERT_EQ(
      get_subject_schema_error(subject, pps::schema_version{1}),
      pps::error_code::subject_not_found);
}
