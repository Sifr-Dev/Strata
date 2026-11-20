# ADR 0001: immutable full-state snapshots and one writer

Status: accepted for 0.1 development.

## Context

A small embedded ordered store needs correct snapshots, recovery and concurrent reads before complex storage optimization. JDK 25 is the compatibility baseline. Preview APIs are excluded.

## Decision

Serialize writers, copy public arrays, and publish immutable sorted state after a forced WAL batch. Readers and snapshots acquire one immutable state reference. Compaction writes a complete sorted segment. Explicit resource ownership and a file lease bound the process model.

## Consequences

Point reads and snapshots are simple and stable, but writes copy the live map and compaction rewrites all live values. Memory is bounded by live-byte/key/value limits, not by an off-heap block cache. This is intentionally not presented as a finished tiered LSM. Multi-chunk state and segment indexes are future changes motivated by profiles, not assumed improvements.

## Recovery

One WAL frame represents one atomic batch. Incomplete trailing frames are discarded; interior corruption is fatal. On every open the recovered state is checkpointed and a new WAL is created, preventing appends after damaged tails. Atomic file replacement and directory force are required for checkpoint publication.
