package dev.sifr.strata;

import java.io.IOException;
import java.util.Optional;

/** A stable point-in-time view; mutations never alter an acquired snapshot. */
public interface Snapshot extends AutoCloseable {
    Optional<byte[]> get(byte[] key) throws IOException;
    Scan scan(byte[] fromInclusive, byte[] toExclusive, long limit) throws IOException;
    @Override void close();
}
