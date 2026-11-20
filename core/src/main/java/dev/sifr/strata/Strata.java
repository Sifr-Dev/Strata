package dev.sifr.strata;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/** A single-writer, concurrent-reader ordered persistent key-value store. */
public interface Strata extends AutoCloseable {
    static Strata open(Path directory) throws IOException {
        return StrataEngine.open(StrataOptions.builder(directory).build());
    }

    static Strata open(StrataOptions options) throws IOException {
        return StrataEngine.open(options);
    }

    /** Returns a detached value, never a view into reusable internal buffers. */
    Optional<byte[]> get(byte[] key) throws IOException;

    void put(byte[] key, byte[] value) throws IOException;

    void delete(byte[] key) throws IOException;

    /** Atomically applies a batch in caller-provided iteration order. */
    void write(Iterable<Mutation> mutations) throws IOException;

    /** Returns a read-only point-in-time view; caller must close it. */
    Snapshot snapshot();

    /** Returns a bounded, lexicographically ordered iterator over detached entries. */
    Scan scan(byte[] fromInclusive, byte[] toExclusive, long limit) throws IOException;

    /** Flushes live entries to a sorted segment and resets the active log. */
    void compact() throws IOException;

    @Override void close() throws IOException;
}
