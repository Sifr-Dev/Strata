# Engineering log

Product development started 2026-11-20. Initial implementation evidence is appended only after commands execute.

## 2026-11-22 — benchmark-driven concurrency and durability work

### Problem

The first end-to-end workload matrix (SQLite comparator included, see the
workspace report `strata-v0.1.0.md`) showed per-write cost growing with
dataset size: 400 µs for a single buffered overwrite and 45 ms for a
100-mutation batch at 20,000 entries, roughly 20× slower than SQLite. The
cause was the copy-on-write publication model: every `write()` copied the
whole `TreeMap` before publishing a new immutable `StorageState`.

### Decision

`write()` now mutates the live map under the writer lock. A `shared` flag is
set when a snapshot or scan view is handed out; the next mutation copies the
map once and continues in place afterwards. Point lookups take the writer lock
instead of reading the published snapshot. This preserves the documented
stable-snapshot and atomic-batch semantics while making per-write cost
O(log n) again. Alternatives rejected:

- copy-on-write per batch only: still O(n) whenever any view is live, which is
  the steady state for read-heavy users;
- an immutable persistent map: a large rewrite of the data structure for a
  benchmark-first milestone;
- parallel writers: incompatible with the single-writer durability contract.

### Measurement

| Workload (20k entries, 128 B values, XFS/LUKS, JDK 27) | before | after |
|---|---:|---:|
| FLUSH_ONLY sequential overwrite | 2,423 ops/s | 401,206 ops/s |
| FLUSH_ONLY 100-row batch | 22 batches/s | 10,204 batches/s |
| reopen + verify 20,000 entries | 4,063 ms | 94.9 ms |
| scan 100 | 159,248/s | 131,308/s |
| SYNC sequential overwrite | 565 ops/s | 842 ops/s |

Scans trade a copy-free shared view for a copy on the next write; steady-state
scan throughput is within measurement noise of the copy-per-scan variant
(131k vs 159k/s in a random workload where every scan is preceded by a write),
while write throughput improves by two orders of magnitude.

### WAL preallocation

The WAL is extended to a 1 MiB window with a zero tail. A zero first length
field terminates recovery, so the filler is a documented part of format
version 1 (see `docs/FORMAT.md`). The intended benefit was avoiding allocation
inside the fsync path; measurements show `fdatasync` on this host costs
0.8–1.2 ms with or without preallocation, so the change is retained for its
jitter reduction and explicit end-of-log marker, not for a claimed speedup.

### Comparator honesty

`INSERT OR REPLACE` with a byte-identical row does not write in SQLite; the
first comparator run therefore measured a "rewrite" workload at ~10 µs/op
against a 1.3 ms fsync-bound fill. The harness now derives a new value per
write. Both engines then sit at the same fsync floor for SYNC writes.
