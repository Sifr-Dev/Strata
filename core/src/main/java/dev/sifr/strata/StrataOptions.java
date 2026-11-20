package dev.sifr.strata;

import java.nio.file.Path;
import java.util.Objects;

/** Immutable engine limits; no system properties affect on-disk semantics. */
public record StrataOptions(Path directory, Durability durability, long maxLiveBytes,
                            long maxSegmentBytes, int maxKeyBytes, int maxValueBytes,
                            boolean verifyOnOpen) {
    public enum Durability { SYNC, FLUSH_ONLY }

    public StrataOptions {
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(durability, "durability");
        if (maxLiveBytes < maxKeyBytes + (long) maxValueBytes || maxSegmentBytes < 1024 || maxKeyBytes < 1 || maxValueBytes < 1) {
            throw new IllegalArgumentException("storage limits must be positive; segments must hold at least 1024 bytes");
        }
    }

    public static Builder builder(Path directory) { return new Builder(directory); }

    public static final class Builder {
        private final Path directory;
        private Durability durability = Durability.SYNC;
        private long maxLiveBytes = 16L * 1024 * 1024;
        private long maxSegmentBytes = 64L * 1024 * 1024;
        private int maxKeyBytes = 4096;
        private int maxValueBytes = 4 * 1024 * 1024;
        private boolean verifyOnOpen = true;

        private Builder(Path directory) { this.directory = directory; }
        public Builder durability(Durability value) { durability = value; return this; }
        public Builder maxLiveBytes(long value) { maxLiveBytes = value; return this; }
        public Builder maxSegmentBytes(long value) { maxSegmentBytes = value; return this; }
        public Builder maxKeyBytes(int value) { maxKeyBytes = value; return this; }
        public Builder maxValueBytes(int value) { maxValueBytes = value; return this; }
        public Builder verifyOnOpen(boolean value) { verifyOnOpen = value; return this; }
        public StrataOptions build() { return new StrataOptions(directory, durability, maxLiveBytes, maxSegmentBytes, maxKeyBytes, maxValueBytes, verifyOnOpen); }
    }
}
