package dev.sifr.strata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class RandomizedModelTest {
    @TempDir Path temp;
    @Test void deterministicOperationModelMatchesReferenceMap() throws Exception {
        Random random = new Random(0x5eed);
        TreeMap<ByteKey, byte[]> model = new TreeMap<>();
        try (Strata db = Strata.open(temp)) {
            for (int step = 0; step < 1500; step++) {
                byte[] key = new byte[] {(byte) random.nextInt(64)};
                switch (random.nextInt(10)) {
                    case 0, 1 -> { db.delete(key); model.remove(new ByteKey(key)); }
                    case 2 -> {
                        List<Mutation> batch = new ArrayList<>();
                        for (int j = 0; j < 5; j++) {
                            byte[] k = {(byte) random.nextInt(64)};
                            byte[] v = {(byte) step, (byte) j};
                            batch.add(Mutation.put(k, v)); model.put(new ByteKey(k), v);
                        }
                        db.write(batch);
                    }
                    case 3 -> db.compact();
                    default -> {
                        byte[] value = {(byte) step, (byte) random.nextInt()};
                        db.put(key, value); model.put(new ByteKey(key), value);
                    }
                }
                if (step % 25 == 0) {
                    for (var entry : model.entrySet()) {
                        assertArrayEquals(entry.getValue(), db.get(entry.getKey().bytes).orElseThrow(), "step " + step);
                    }
                    try (Scan scan = db.scan(new byte[]{0}, new byte[]{64}, 100)) {
                        var expected = model.entrySet().iterator();
                        while (scan.next()) {
                            assertTrue(expected.hasNext());
                            var next = expected.next();
                            assertArrayEquals(next.getKey().bytes, scan.entry().key());
                            assertArrayEquals(next.getValue(), scan.entry().value());
                        }
                        assertFalse(expected.hasNext());
                    }
                }
            }
        }
        try (Strata db = Strata.open(temp)) {
            for (var entry : model.entrySet()) assertArrayEquals(entry.getValue(), db.get(entry.getKey().bytes).orElseThrow());
        }
    }
}
