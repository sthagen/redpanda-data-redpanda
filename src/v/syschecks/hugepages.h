/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

namespace syschecks {

/// Promote file-backed executable mappings (code segments) to transparent huge
/// pages.
void promote_code_to_hugepages();

/// Undo the effect of promote_code_to_hugepages(). Marks executable VMAs with
/// MADV_NOHUGEPAGE
void demote_code_from_hugepages();

} // namespace syschecks
