package dev.sifr.strata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class StrataEngineTest {
    @TempDir Path temp;
    static byte[] b(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    static String s(byte[] value) { return new String(value, StandardCharsets.UTF_8); }

    @Test void persistsOrderedEntriesAndDeletes() throws Exception {
        try (Strata db = Strata.open(temp)) {
            db.put(b("c"), b("3")); db.put(b("a"), b("1")); db.put(b("b"), b("2"));
            db.delete(b("b"));
            assertEquals("1", s(db.get(b("a")).orElseThrow()));
            assertTrue(db.get(b("b")).isEmpty());
            try (Scan scan = db.scan(b("a"), b("d"), 10)) {
                assertTrue(scan.next()); assertArrayEquals(b("a"), scan.entry().key());
                assertTrue(scan.next()); assertArrayEquals(b("c"), scan.entry().key());
                assertFalse(scan.next());
            }
        }
        try (Strata db = Strata.open(temp)) { assertEquals("3", s(db.get(b("c")).orElseThrow())); }
    }

    @Test void snapshotIsStableAndInputsAreOwned() throws Exception {
        try (Strata db = Strata.open(temp)) {
            byte[] key = b("key"), value = b("before");
            db.put(key, value); key[0] = 'X'; value[0] = 'X';
            assertEquals("before", s(db.get(b("key")).orElseThrow()));
            Snapshot snapshot = db.snapshot();
            db.put(b("key"), b("after"));
            assertEquals("before", s(snapshot.get(b("key")).orElseThrow()));
            snapshot.close();
            assertThrows(IllegalStateException.class, () -> snapshot.get(b("key")));
            assertEquals("after", s(db.get(b("key")).orElseThrow()));
        }
    }

    @Test void compactionPreservesDataAcrossReopen() throws Exception {
        try (Strata db = Strata.open(temp)) {
            for (int i = 0; i < 100; i++) db.put(b(String.format("%03d", i)), b("value-" + i));
            db.compact();
        }
        try (Strata db = Strata.open(temp)) {
            assertEquals("value-42", s(db.get(b("042")).orElseThrow()));
            db.put(b("101"), b("after-checkpoint"));
        }
        try (Strata db = Strata.open(temp)) { assertEquals("after-checkpoint", s(db.get(b("101")).orElseThrow())); }
    }

    @Test void truncatedTailDoesNotLosePreviousFrames() throws Exception {
        try (Strata db = Strata.open(temp)) { db.put(b("stable"), b("yes")); db.put(b("tail"), b("remove")); }
        Path wal = temp.resolve("wal-1.log");
        byte[] original = Files.readAllBytes(wal);
        Files.write(wal, Arrays.copyOf(original, original.length - 2), StandardOpenOption.TRUNCATE_EXISTING);
        try (Strata db = Strata.open(temp)) {
            assertEquals("yes", s(db.get(b("stable")).orElseThrow()));
            assertTrue(db.get(b("tail")).isEmpty());
        }
    }

    @Test void everyTruncatedTailOffsetRecoversOnlyCompleteBatches() throws Exception {
        // Build the two batches in a separate directory so each truncation starts from identical bytes.
        Path originalDir = temp.resolve("original");
        try (Strata db = Strata.open(originalDir)) {
            db.write(List.of(Mutation.put(b("stable"), b("yes")), Mutation.put(b("also-stable"), b("yes"))));
            db.put(b("tail"), b("remove"));
        }
        byte[] wal = Files.readAllBytes(originalDir.resolve("wal-1.log"));
        int firstBatchEnd = wal.length - Format.frame(Mutation.put(b("tail"), b("remove"))).length;
        for (int length = 0; length < wal.length; length++) {
            Path candidate = temp.resolve("truncated-" + length);
            Files.createDirectories(candidate);
            Path emptySegment = candidate.resolve("segment-1.sst");
            Format.writeSegment(emptySegment, new TreeMap<>());
            Format.writeManifest(candidate.resolve("MANIFEST"), 0, 1, Format.segmentChecksum(emptySegment));
            Files.write(candidate.resolve("wal-1.log"), Arrays.copyOf(wal, length));
            try (Strata db = Strata.open(candidate)) {
                if (length == wal.length) {
                    assertEquals("yes", s(db.get(b("stable")).orElseThrow()));
                    assertEquals("remove", s(db.get(b("tail")).orElseThrow()));
                } else {
                    boolean firstBatchComplete = length >= firstBatchEnd;
                    assertEquals(firstBatchComplete, db.get(b("stable")).isPresent(), "complete batch visibility at " + length);
                    assertEquals(firstBatchComplete, db.get(b("also-stable")).isPresent(), "atomic first batch at " + length);
                    assertTrue(db.get(b("tail")).isEmpty());
                }
                db.put(b("after-recovery"), b("valid"));
            }
            try (Strata db = Strata.open(candidate)) {
                assertEquals("valid", s(db.get(b("after-recovery")).orElseThrow()));
            }
        }
    }

    @Test void atomicBatchSurvivesTruncationWithoutPartialVisibility() throws Exception {
        try (Strata db = Strata.open(temp)) {
            db.write(List.of(Mutation.put(b("first"), b("one")), Mutation.put(b("second"), b("two"))));
        }
        Path wal = temp.resolve("wal-1.log");
        byte[] original = Files.readAllBytes(wal);
        Files.write(wal, Arrays.copyOf(original, original.length - 1), StandardOpenOption.TRUNCATE_EXISTING);
        try (Strata db = Strata.open(temp)) {
            assertTrue(db.get(b("first")).isEmpty());
            assertTrue(db.get(b("second")).isEmpty());
        }
    }

    @Test void uncommittedGenerationCannotReplaceCommittedManifest() throws Exception {
        try (Strata db = Strata.open(temp)) { db.put(b("committed"), b("old")); }
        TreeMap<ByteKey, byte[]> uncommitted = new TreeMap<>();
        uncommitted.put(new ByteKey(b("uncommitted")), b("new"));
        Format.writeSegment(temp.resolve("segment-999.sst"), uncommitted);
        Files.write(temp.resolve("wal-999.log"), Format.WAL_MAGIC);
        try (Strata db = Strata.open(temp)) {
            assertEquals("old", s(db.get(b("committed")).orElseThrow()));
            assertTrue(db.get(b("uncommitted")).isEmpty());
        }
    }

    @Test void manifestRejectsSegmentFromAnotherGeneration() throws Exception {
        try (Strata db = Strata.open(temp)) { db.put(b("committed"), b("old")); }
        var manifest = Format.readManifest(temp.resolve("MANIFEST"));
        Files.move(temp.resolve("segment-1.sst"), temp.resolve("segment-2.sst"));
        TreeMap<ByteKey, byte[]> replacement = new TreeMap<>();
        replacement.put(new ByteKey(b("replacement")), b("new"));
        Format.writeSegment(temp.resolve("segment-1.sst"), replacement);
        assertThrows(java.io.IOException.class, () -> Strata.open(temp));
        // Restore the committed generation after proving identity validation.
        Files.delete(temp.resolve("segment-1.sst"));
        Files.move(temp.resolve("segment-2.sst"), temp.resolve("segment-1.sst"));
    }

    @Test void secondProcessCannotAcquireStoreLease() throws Exception {
        try (Strata db = Strata.open(temp)) {
            assertThrows(java.io.IOException.class, () -> Strata.open(temp));
        }
    }

    @Test void concurrentReadersObserveCompleteValues() throws Exception {
        try (Strata db = Strata.open(temp)) {
            db.put(b("key"), b("initial"));
            ExecutorService pool = Executors.newFixedThreadPool(8);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < 4; t++) futures.add(pool.submit(() -> {
                for (int i = 0; i < 100; i++) assertNotNull(db.get(b("key")).orElseThrow());
                return null;
            }));
            for (int i = 0; i < 100; i++) db.put(b("key"), b("value-" + i));
            for (Future<?> future : futures) future.get(10, TimeUnit.SECONDS);
            pool.close();
        }
    }

    @Test void rejectsInvalidLimitsAndOperations() throws Exception {
        try (Strata db = Strata.open(temp)) {
            assertThrows(IllegalArgumentException.class, () -> db.put(new byte[0], b("v")));
            assertThrows(IllegalArgumentException.class, () -> db.scan(b("z"), b("a"), 1));
            assertThrows(NullPointerException.class, () -> db.put(b("key"), null));
        }
    }
}
