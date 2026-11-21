package dev.sifr.strata;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32;

/** Versioned, little-endian records. Checksums detect corruption, not malicious writers. */
final class Format {
    static final int VERSION = 1;
    static final byte[] WAL_MAGIC = "STRWAL1\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] SEGMENT_MAGIC = "STRSEG1\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] MANIFEST_MAGIC = "STRMAN1\n".getBytes(StandardCharsets.US_ASCII);
    static final int MAX_FRAME_BYTES = 8 * 1024 * 1024 + 128;

    private Format() {}

    static void validate(byte[] key, byte[] value, StrataOptions options) {
        Objects.requireNonNull(key, "key");
        if (key.length == 0) throw new IllegalArgumentException("keys must not be empty");
        if (key.length > options.maxKeyBytes()) throw new IllegalArgumentException("key exceeds configured byte limit");
        if (value != null && value.length > options.maxValueBytes()) throw new IllegalArgumentException("value exceeds configured byte limit");
    }

    static byte[] frame(Mutation mutation) { return frame(List.of(mutation)); }

    /** One length-prefixed, checksummed frame is the atomic recovery unit. */
    static byte[] frame(List<Mutation> batch) {
        if (batch.isEmpty()) throw new IllegalArgumentException("empty WAL batch");
        long payloadLength = 4;
        for (Mutation mutation : batch) {
            payloadLength += 1L + 4 + mutation.key().length + (mutation.type() == Mutation.Type.PUT ? 4 + mutation.value().length : 0);
        }
        if (payloadLength > MAX_FRAME_BYTES) throw new IllegalArgumentException("WAL batch exceeds configured frame bound");
        int length = Math.toIntExact(payloadLength);
        ByteBuffer out = ByteBuffer.allocate(8 + length).order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(length).putInt(batch.size());
        for (Mutation mutation : batch) {
            out.put(mutation.type() == Mutation.Type.PUT ? (byte) 1 : (byte) 2);
            out.putInt(mutation.key().length).put(mutation.key());
            if (mutation.type() == Mutation.Type.PUT) out.putInt(mutation.value().length).put(mutation.value());
        }
        CRC32 crc = new CRC32(); crc.update(out.array(), 4, length);
        out.putInt((int) crc.getValue());
        return out.array();
    }

    static List<Mutation> readLog(Path path) throws IOException {
        if (!Files.exists(path)) return List.of();
        if (Files.size(path) < WAL_MAGIC.length) return List.of(); // incomplete header contains no acknowledged frame
        try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
            expectMagic(in, WAL_MAGIC);
            List<Mutation> result = new ArrayList<>();
            while (true) {
                int first = in.read();
                if (first < 0) break;
                byte[] rest = in.readNBytes(3);
                if (rest.length < 3) break; // A partial final frame header is recoverable.
                byte[] header = { (byte) first, rest[0], rest[1], rest[2] };
                ByteBuffer b = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
                int length = b.getInt();
                if (length < 4 || length > MAX_FRAME_BYTES) throw new IOException("invalid WAL frame length");
                byte[] body = in.readNBytes(length);
                if (body.length < length) break;
                byte[] crcBytes = in.readNBytes(4);
                if (crcBytes.length < 4) break;
                CRC32 crc = new CRC32(); crc.update(body);
                int stored = ByteBuffer.wrap(crcBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
                if (stored != (int) crc.getValue()) {
                    if (in.read() >= 0) throw new IOException("WAL checksum mismatch before end of log");
                    break;
                }
                ByteBuffer record = ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN);
                int count = record.getInt();
                if (count < 1 || count > (length - 4) / 5) throw new IOException("invalid WAL batch count");
                List<Mutation> batch = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    if (record.remaining() < 5) throw new IOException("truncated WAL mutation");
                    int type = Byte.toUnsignedInt(record.get());
                    int keyLength = record.getInt();
                    if (keyLength < 1 || keyLength > record.remaining()) throw new IOException("invalid WAL key length");
                    byte[] key = new byte[keyLength]; record.get(key);
                    if (type == 1) {
                        if (record.remaining() < 4) throw new IOException("truncated WAL value length");
                        int valueLength = record.getInt();
                        if (valueLength < 0 || valueLength > record.remaining()) throw new IOException("invalid WAL value length");
                        byte[] value = new byte[valueLength]; record.get(value);
                        batch.add(Mutation.put(key, value));
                    } else if (type == 2) batch.add(Mutation.delete(key));
                    else throw new IOException("invalid WAL mutation type");
                }
                if (record.hasRemaining()) throw new IOException("trailing WAL mutation bytes");
                result.addAll(batch);
            }
            return List.copyOf(result);
        }
    }

    static NavigableMap<ByteKey, byte[]> readSegment(Path path, StrataOptions options) throws IOException {
        if (!Files.exists(path)) return new TreeMap<>();
        long maximumBytes = Math.addExact(Math.multiplyExact(options.maxLiveBytes(), 9), 16);
        if (Files.size(path) > maximumBytes) throw new IOException("segment exceeds configured physical bounds");
        try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
            expectMagic(in, SEGMENT_MAGIC);
            byte[] payload = in.readAllBytes();
            if (payload.length < 4) throw new IOException("truncated segment checksum");
            int split = payload.length - 4;
            CRC32 crc = new CRC32(); crc.update(payload, 0, split);
            int stored = ByteBuffer.wrap(payload, split, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
            if (stored != (int) crc.getValue()) throw new IOException("segment checksum mismatch: " + path.getFileName());
            ByteBuffer b = ByteBuffer.wrap(payload, 0, split).order(ByteOrder.LITTLE_ENDIAN);
            long count = b.getLong();
            if (count < 0 || count > options.maxLiveBytes()) throw new IOException("invalid segment entry count");
            TreeMap<ByteKey, byte[]> result = new TreeMap<>();
            for (long i = 0; i < count; i++) {
                if (b.remaining() < 8) throw new IOException("truncated segment entry");
                int keyLength = b.getInt(), valueLength = b.getInt();
                if (keyLength < 1 || valueLength < 0 || keyLength > b.remaining()) throw new IOException("invalid segment entry length");
                byte[] key = new byte[keyLength]; b.get(key);
                if (valueLength > b.remaining()) throw new IOException("truncated segment value");
                byte[] value = new byte[valueLength]; b.get(value);
                if (result.put(ByteKey.owned(key), value) != null) throw new IOException("duplicate segment key");
            }
            if (b.hasRemaining()) throw new IOException("trailing segment bytes");
            return result;
        }
    }

    static long writeSegment(Path path, NavigableMap<ByteKey, byte[]> values) throws IOException {
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
            out.write(SEGMENT_MAGIC);
            CRC32 crc = new CRC32();
            java.nio.ByteBuffer payload = java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            payload.putLong(values.size()).flip();
            out.write(payload.array()); crc.update(payload.array());
            for (var entry : values.entrySet()) {
                java.nio.ByteBuffer lengths = java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN);
                lengths.putInt(entry.getKey().bytes.length).putInt(entry.getValue().length).flip();
                out.write(lengths.array()); crc.update(lengths.array());
                out.write(entry.getKey().bytes); crc.update(entry.getKey().bytes);
                out.write(entry.getValue()); crc.update(entry.getValue());
            }
            out.write(java.nio.ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt((int) crc.getValue()).array());
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) { channel.force(true); }
        byte[] bytes = Files.readAllBytes(path);
        int offset = bytes.length - 4;
        return Integer.toUnsignedLong(ByteBuffer.wrap(bytes, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt());
    }

    static long segmentChecksum(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        if (bytes.length < 4) throw new IOException("truncated segment checksum");
        return Integer.toUnsignedLong(ByteBuffer.wrap(bytes, bytes.length - 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt());
    }

    record Checkpoint(long sequence, long generation, long segmentChecksum) {}

    static void writeManifest(Path path, long sequence, long generation, long segmentChecksum) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer payload = ByteBuffer.allocate(8 + 24 + 4).order(ByteOrder.LITTLE_ENDIAN);
            payload.put(MANIFEST_MAGIC).putLong(sequence).putLong(generation).putLong(segmentChecksum);
            CRC32 crc = new CRC32(); crc.update(payload.array(), 8, 24);
            payload.putInt((int) crc.getValue()).flip();
            writeFully(channel, payload);
            channel.force(true);
        }
    }

    static Checkpoint readManifest(Path path) throws IOException {
        if (!Files.exists(path)) return null;
        byte[] bytes = Files.readAllBytes(path);
        if (bytes.length != 36 || !Arrays.equals(Arrays.copyOf(bytes, 8), MANIFEST_MAGIC)) throw new IOException("invalid manifest");
        ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN); b.position(8);
        long sequence = b.getLong(), generation = b.getLong(), checksum = b.getLong();
        CRC32 crc = new CRC32(); crc.update(bytes, 8, 24);
        if (b.getInt() != (int) crc.getValue() || sequence < 0 || generation < 0 || checksum < 0) throw new IOException("manifest checksum/version mismatch");
        return new Checkpoint(sequence, generation, checksum);
    }

    static void forceDirectory(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) { channel.force(true); }
    }

    static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) channel.write(buffer);
    }

    private static void expectMagic(InputStream in, byte[] magic) throws IOException {
        if (!Arrays.equals(in.readNBytes(magic.length), magic)) throw new IOException("unsupported or truncated storage header");
    }


}
