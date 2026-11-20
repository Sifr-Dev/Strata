package dev.sifr.strata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class FormatTest {
    @TempDir Path temp;
    @Test void logRoundTripsBinaryMutations() throws Exception {
        Path wal = temp.resolve("wal.log");
        Files.write(wal, Format.WAL_MAGIC);
        byte[] key = {0, (byte) 255, 1};
        Mutation put = Mutation.put(key, new byte[0]);
        Files.write(wal, Format.frame(put), StandardOpenOption.APPEND);
        Files.write(wal, Format.frame(Mutation.delete(key)), StandardOpenOption.APPEND);
        assertEquals(List.of(put, Mutation.delete(key)), Format.readLog(wal));
    }
    @Test void segmentUsesDocumentedLittleEndianEncoding() throws Exception {
        Path segment = temp.resolve("segment.sst");
        TreeMap<ByteKey, byte[]> values = new TreeMap<>();
        values.put(new ByteKey(new byte[]{1}), new byte[]{2});
        Format.writeSegment(segment, values);
        byte[] bytes = Files.readAllBytes(segment);
        assertArrayEquals(new byte[]{1,0,0,0,0,0,0,0}, Arrays.copyOfRange(bytes, 8, 16));
        assertArrayEquals(new byte[]{1,0,0,0,1,0,0,0}, Arrays.copyOfRange(bytes, 16, 24));
        var decoded = Format.readSegment(segment, StrataOptions.builder(temp).build());
        assertEquals(values.navigableKeySet(), decoded.navigableKeySet());
        for (var key : values.navigableKeySet()) assertArrayEquals(values.get(key), decoded.get(key));
    }
    @Test void segmentRejectsCorruption() throws Exception {
        Path segment = temp.resolve("segment.sst");
        TreeMap<ByteKey, byte[]> values = new TreeMap<>();
        values.put(new ByteKey(new byte[]{1}), new byte[]{2});
        Format.writeSegment(segment, values);
        byte[] bytes = Files.readAllBytes(segment); bytes[12] ^= 1;
        Files.write(segment, bytes);
        StrataOptions options = StrataOptions.builder(temp).build();
        assertThrows(java.io.IOException.class, () -> Format.readSegment(segment, options));
    }
}
