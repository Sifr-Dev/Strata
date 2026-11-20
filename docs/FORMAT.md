# Storage format version 1

All integers are unsigned-length fields encoded little-endian unless stated otherwise. Signed Java accessors interpret only lengths bounded before allocation.

## WAL

`STRWAL1\n` magic, followed by frames:

- `u32 payloadLength`
- `u32 mutationCount`
- mutations: `u8 type` (1 put, 2 delete), `u32 keyLength`, key, and for put `u32 valueLength`, value
- `u32 crc32(payloadLength field + payload)`

One frame is one atomic batch. Complete frames replay in order. A partial final header/payload/checksum or checksum mismatch at the physical end discards that frame; subsequent data or malformed interior data is fatal. The WAL is checkpointed and recreated on open, so a damaged tail is never extended.

## Segment

`STRSEG1\n`, `u64 count`, then each entry's `u32 keyLength`, `u32 valueLength`, key and value. A final `u32 CRC-32` covers all bytes after the magic. Entries are strictly unique and ordered by unsigned key bytes.

## Manifest

`STRMAN1\n`, `u64 sequence`, `u64 generation`, `u32 CRC-32` of the two numeric fields. Segment and manifest are written to temporary files, forced and atomically replaced before directory synchronization.

Version 1 readers reject other magic and malformed lengths. No automatic migration exists in this 0.x milestone.
