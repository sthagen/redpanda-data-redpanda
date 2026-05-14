// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "syschecks/hugepages.h"

#include "base/vlog.h"
#include "syschecks/syschecks.h"

#include <sys/mman.h>

#include <cstddef>
#include <link.h>

// MADV_COLLAPSE was added in Linux 6.1. Define it for older headers.
#ifndef MADV_COLLAPSE
#define MADV_COLLAPSE 25
#endif

namespace syschecks {

namespace {

/// Invoke fn(addr, len) for each non-writable PT_LOAD segment across
/// all loaded ELF objects (main binary + shared libraries). This covers
/// .text (PF_R|PF_X) and .rodata (PF_R) segments.
template<typename Fn>
void for_each_ro_segment(Fn fn) {
    dl_iterate_phdr(
      [](struct dl_phdr_info* info, size_t /*size*/, void* data) -> int {
          auto& callback = *static_cast<Fn*>(data);
          for (int i = 0; i < info->dlpi_phnum; ++i) {
              const auto& phdr = info->dlpi_phdr[i];
              if (phdr.p_type != PT_LOAD) {
                  continue;
              }
              // Skip writable segments (.data, .bss).
              if (phdr.p_flags & PF_W) {
                  continue;
              }
              auto addr = info->dlpi_addr + phdr.p_vaddr;
              auto len = phdr.p_memsz;
              if (len == 0) {
                  continue;
              }
              callback(reinterpret_cast<void*>(addr), static_cast<size_t>(len));
          }
          return 0; // continue iteration
      },
      &fn);
}

} // namespace

void promote_code_to_hugepages() {
    vlog(checklog.info, "Starting hugepage promotioin of code segments");

    size_t total_bytes = 0;
    size_t marked_bytes = 0;
    size_t collapsed_bytes = 0;

    for_each_ro_segment([&](void* addr, size_t len) {
        total_bytes += len;

        // Mark the VMA for huge pages. In "madvise" THP mode (the common
        // default), khugepaged only scans VMAs with VM_HUGEPAGE set, so this
        // is required for ongoing huge page maintenance — not just a hint.
        if (::madvise(addr, len, MADV_HUGEPAGE) == 0) {
            marked_bytes += len;
        }

        // Fault in all pages so MADV_COLLAPSE has something to work with.
        // At startup most pages are still demand-paged.
        // In theory this is not needed with MADV_COLLAPSE but the docs leave a
        // cop out so we are just explicit in any case.
        // Incompatible with ASAN, disable if on
#if !__has_feature(address_sanitizer)
        auto* base = static_cast<volatile const char*>(addr);
        for (size_t off = 0; off < len; off += 4096) {
            [[maybe_unused]] char c = base[off];
        }
#endif

        // Synchronously collapse 4 KB pages into 2 MB huge pages
        // (Linux 6.1+). Without this, khugepaged promotes pages in the
        // background over the next few seconds; MADV_COLLAPSE makes it
        // immediate (best effort).
        if (::madvise(addr, len, MADV_COLLAPSE) == 0) {
            collapsed_bytes += len;
        }
    });

    if (total_bytes > 0) {
        vlog(
          checklog.info,
          "hugepages: {}/{} MiB marked, {}/{} MiB collapsed",
          marked_bytes / (1024 * 1024),
          total_bytes / (1024 * 1024),
          collapsed_bytes / (1024 * 1024),
          total_bytes / (1024 * 1024));
    }
}

void demote_code_from_hugepages() {
    size_t total_bytes = 0;
    size_t demoted_bytes = 0;

    for_each_ro_segment([&](void* addr, size_t len) {
        total_bytes += len;

        // Prevent khugepaged from re-promoting these pages.
        if (::madvise(addr, len, MADV_NOHUGEPAGE) != 0) {
            return;
        }

        // MADV_NOHUGEPAGE only prevents future promotions — existing PMD
        // entries for file-backed pages are not split. MADV_DONTNEED drops
        // the page table entries. They will be re-faulted at 4 KB granularity
        // (since MADV_NOHUGEPAGE is set).
        if (::madvise(addr, len, MADV_DONTNEED) == 0) {
            demoted_bytes += len;
        }
    });

    if (total_bytes > 0) {
        vlog(
          checklog.info,
          "hugepages: demoted {}/{} MiB from huge pages",
          demoted_bytes / (1024 * 1024),
          total_bytes / (1024 * 1024));
    }
}

} // namespace syschecks
