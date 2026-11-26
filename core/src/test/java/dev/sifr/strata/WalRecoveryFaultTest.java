package dev.sifr.strata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Fault injection into the write-ahead log.
 *
 * <p>A crash can interrupt the append of a frame in the middle. Because the log
 * is preallocated with zero filler, the interrupted frame can be followed by
 * durable zeros, which the old reader mistook for interior corruption and
 * refused to open. These tests pin the documented contract: an interrupted
 * final append is discarded, damage inside written data stays fatal.
 */
class WalRecoveryFaultTest {
    @TempDir Path temp;
    static byte[] b(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    static String s(byte[] value) { return new String(value, StandardCharsets.UTF_8); }

    /** Offset of the first zero length field, i.e. the logical end of the log. */
    static int logicalEnd(byte[] wal) {
        int offset = Format.WAL_MAGIC.length;
        while (offset + 4 <= wal.length) {
            int length = ByteBuffer.wrap(wal, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
            if (length == 0) return offset;
            offset += 8 + length;
        }
        return offset;
    }

    /** Builds a store holding one acknowledged, durable batch, and returns a pristine copy of it. */
    private Path seeded(String name) throws Exception {
        Path source = temp.resolve("seed-" + name);
        try (Strata db = Strata.open(source)) {
            db.write(List.of(Mutation.put(b("durable-1"), b("one")), Mutation.put(b("durable-2"), b("two"))));
        }
        Path copy = temp.resolve("copy-" + name);
        copyStore(source, copy);
        return copy;
    }

    /** Copies every durable file; the LOCK file is per-process and is not data. */
    static void copyStore(Path from, Path to) throws Exception {
        Files.createDirectories(to);
        try (var entries = Files.list(from)) {
            for (Path file : entries.toList()) {
                if (file.getFileName().toString().equals("LOCK")) continue;
                Files.copy(file, to.resolve(file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    /**
     * Every possible cut point of a subsequent batch, injected over the
     * preallocated zero tail exactly as a power failure would leave it. The
     * interrupted batch must never appear, the earlier batch must always
     * survive, and the store must reopen and accept new writes at every cut.
     */
    @Test void everyTornAppendOverZeroTailReopensWithPriorBatchIntact() throws Exception {
        Path seed = seeded("torn");
        byte[] durableWal = Files.readAllBytes(seed.resolve("wal-1.log"));
        int durableEnd = logicalEnd(durableWal);
        byte[] torn = Format.frame(List.of(
                Mutation.put(b("torn-1"), b("first")),
                Mutation.put(b("torn-2"), b("second"))));

        for (int cut = 0; cut <= torn.length; cut++) {
            Path candidate = temp.resolve("torn-" + cut);
            copyStore(seed, candidate);

            // Write exactly `cut` bytes of the next frame at the logical end and
            // leave the rest of the preallocated window as durable zero filler.
            byte[] wal = Files.readAllBytes(candidate.resolve("wal-1.log"));
            System.arraycopy(torn, 0, wal, durableEnd, cut);
            Files.write(candidate.resolve("wal-1.log"), wal, StandardOpenOption.TRUNCATE_EXISTING);

            try (Strata db = Strata.open(candidate)) {
                assertEquals("one", s(db.get(b("durable-1")).orElseThrow()), "prior batch lost at cut " + cut);
                assertEquals("two", s(db.get(b("durable-2")).orElseThrow()), "prior batch lost at cut " + cut);
                if (cut == torn.length) {
                    // The whole frame is present, so this is a complete batch.
                    assertEquals("first", s(db.get(b("torn-1")).orElseThrow()), "complete batch lost at cut " + cut);
                    assertEquals("second", s(db.get(b("torn-2")).orElseThrow()), "complete batch lost at cut " + cut);
                } else {
                    assertTrue(db.get(b("torn-1")).isEmpty(), "torn batch became visible at cut " + cut);
                    assertTrue(db.get(b("torn-2")).isEmpty(), "torn batch became visible at cut " + cut);
                }
                db.put(b("after-recovery"), b("yes"));
            }
            try (Strata db = Strata.open(candidate)) {
                assertEquals("yes", s(db.get(b("after-recovery")).orElseThrow()), "post-recovery write lost at cut " + cut);
                assertEquals("one", s(db.get(b("durable-1")).orElseThrow()), "checkpoint lost prior batch at cut " + cut);
            }
        }
    }

    /** A torn length field must not be read as a huge or negative frame size. */
    @Test void tornLengthFieldRecoversWhenOnlyFillerFollows() throws Exception {
        Path seed = seeded("length");
        byte[] wal = Files.readAllBytes(seed.resolve("wal-1.log"));
        int end = logicalEnd(wal);
        // A garbage nonzero length with all-zero filler behind it: a half-written
        // header for a batch that was never durable.
        ByteBuffer.wrap(wal, end, 4).order(ByteOrder.LITTLE_ENDIAN).putInt(0x7FFF_FF00);
        Files.write(seed.resolve("wal-1.log"), wal, StandardOpenOption.TRUNCATE_EXISTING);

        try (Strata db = Strata.open(seed)) {
            assertEquals("one", s(db.get(b("durable-1")).orElseThrow()));
            db.put(b("after"), b("yes"));
        }
    }

    /**
     * Damage inside written data must stay fatal. A batch that was fully written
     * and then corrupted must never be silently dropped, because that would lose
     * an acknowledged mutation without any signal.
     */
    @Test void corruptedDurableFrameStaysFatal() throws Exception {
        Path seed = seeded("crc");
        byte[] wal = Files.readAllBytes(seed.resolve("wal-1.log"));
        int end = logicalEnd(wal);
        assertTrue(end > Format.WAL_MAGIC.length, "expected a written frame");
        byte[] damaged = Arrays.copyOf(wal, wal.length);
        damaged[end - 1] ^= 0x01; // flip a bit inside the durable frame's checksum
        Files.write(seed.resolve("wal-1.log"), damaged, StandardOpenOption.TRUNCATE_EXISTING);

        assertThrows(java.io.IOException.class, () -> Strata.open(seed));
    }

    /** A corrupted payload byte inside written data is equally fatal. */
    @Test void corruptedDurablePayloadStaysFatal() throws Exception {
        Path seed = seeded("payload");
        byte[] wal = Files.readAllBytes(seed.resolve("wal-1.log"));
        int bodyStart = Format.WAL_MAGIC.length + 4;
        byte[] damaged = Arrays.copyOf(wal, wal.length);
        damaged[bodyStart + 6] ^= 0x01;
        Files.write(seed.resolve("wal-1.log"), damaged, StandardOpenOption.TRUNCATE_EXISTING);

        assertThrows(java.io.IOException.class, () -> Strata.open(seed));
    }

    /**
     * Truncation and zero-filler tearing must agree: a batch is either fully
     * visible with both mutations or absent with neither.
     */
    @Test void interruptedBatchIsAtomicUnderBothTearShapes() throws Exception {
        Path seed = seeded("atomic");
        byte[] wal = Files.readAllBytes(seed.resolve("wal-1.log"));
        int end = logicalEnd(wal);
        byte[] torn = Format.frame(List.of(
                Mutation.put(b("pair-1"), b("a")),
                Mutation.put(b("pair-2"), b("b"))));

        for (int cut = 0; cut < torn.length; cut++) {
            // Shape 1: the file is physically truncated at the cut.
            Path truncated = temp.resolve("trunc-" + cut);
            copyStore(seed, truncated);
            byte[] truncatedWal = new byte[end + cut];
            System.arraycopy(wal, 0, truncatedWal, 0, end);
            System.arraycopy(torn, 0, truncatedWal, end, cut);
            Files.write(truncated.resolve("wal-1.log"), truncatedWal, StandardOpenOption.TRUNCATE_EXISTING);

            // Shape 2: the cut bytes land over the preallocated zero tail.
            Path zeroTail = temp.resolve("zero-" + cut);
            copyStore(seed, zeroTail);
            byte[] zeroWal = Arrays.copyOf(wal, wal.length);
            System.arraycopy(torn, 0, zeroWal, end, cut);
            Files.write(zeroTail.resolve("wal-1.log"), zeroWal, StandardOpenOption.TRUNCATE_EXISTING);

            for (Path candidate : List.of(truncated, zeroTail)) {
                try (Strata db = Strata.open(candidate)) {
                    boolean first = db.get(b("pair-1")).isPresent();
                    boolean second = db.get(b("pair-2")).isPresent();
                    assertEquals(first, second, "split batch visibility at cut " + cut + " in " + candidate);
                    assertFalse(first, "incomplete batch visible at cut " + cut + " in " + candidate);
                }
            }
        }
    }
}
