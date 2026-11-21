# Strata

Strata is an embedded, ordered byte-key/value store for Java 25 and newer. It is an early **0.x** product, not a general-purpose SQL database.

## Current architecture

- Single-process file lease; one writer lock serializes mutations.
- Atomic batch write-ahead log with bounded little-endian frames and CRC-32 checksums.
- Copy-on-write immutable sorted state published to concurrent readers.
- Stable snapshots and lexicographic bounded scans return detached arrays.
- Explicit compaction to a checksummed sorted segment, followed by manifest replacement and a new WAL.
- `SYNC` forces each write batch before publication; `FLUSH_ONLY` acknowledges operating-system-buffered writes and may lose acknowledged mutations on power failure.

**Important maturity limit:** this milestone uses a full sorted map and full-state compaction, not a tiered LSM, MVCC transaction system or block cache. It is designed to establish a correct durable base before benchmark-driven evolution. CRC checks detect accidental corruption; they do not authenticate files. A partial final WAL frame is discarded, while malformed interior data is fatal. Atomic replacement and directory-force support are required; crash/power-loss semantics remain dependent on the filesystem and device.

## Build and verify

JDK 25+ and Maven 3.10+:

```sh
mvn clean test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk mvn clean test
```

## Library example

```java
try (Strata db = Strata.open(Path.of("data"))) {
    db.put("hello".getBytes(UTF_8), "world".getBytes(UTF_8));
    Optional<byte[]> value = db.get("hello".getBytes(UTF_8));
}
```

Keys are non-empty unsigned byte strings. Inputs and outputs are copied. Empty scan ranges and negative limits are rejected. Close snapshots, scans and the store; do not share one store between processes.

## CLI

```sh
mvn package
java -jar core/target/strata-0.1.0-SNAPSHOT.jar ./data put greeting hello
java -jar core/target/strata-0.1.0-SNAPSHOT.jar ./data get greeting
java -jar core/target/strata-0.1.0-SNAPSHOT.jar ./data compact
```

`scan FROM TO` emits hexadecimal entries for the half-open range. The CLI accepts UTF-8 text for put/get/delete; binary applications should use the library.

## Performance policy

No comparative performance claim is established yet. Benchmarks must record JDK, GC, heap, hardware, filesystem, durability, dataset/cache state, and raw output. They must distinguish in-memory lookup from durable writes and never imply universal superiority over SQLite or RocksDB.

## License

MIT; see `LICENSE`.
