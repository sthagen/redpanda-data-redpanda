The `io` module implements a page-based write-back cache.

### `io_queue`

Per-file I/O request submission queue and scheduler.

### `scheduler`

High-level scheduling across I/O queues.

### `persistence`

Abstract storage interface with disk and memory backends.

### `pager`

Abstraction of an append-only file.

### `page_set`

The set of pages backing a file that are memory resident. The page set is the
primary owner of pages in memory, and serves as the index that maps a file
offset to a page. Pages in a file's page set may or may not be undergoing active
I/O such as write-back.
