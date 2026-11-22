# Storage format version 1

All integers are unsigned-length fields encoded little-endian unless stated otherwise. Signed Java accessors interpret only lengths bounded before allocation.

## WAL

`STRWAL1\n` magic, followed by frames:

- `u32 payloadLength`
- `u32 mutationCount`
- mutations: `u8 type` (1 put, 2 delete), `u32 keyLength`, key, and for put `u32 valueLength`, value
- `u32 crc32(payloadLength field + payload)`

One frame is one atomic batch. Complete frames replay in order. A partial final header/payload/checksum or checksum mismatch at the physical end discards that frame; subsequent data or malformed interior data is fatal. The WAL is checkpointed and recreated on open, so a damaged tail is never extended.

The WAL is preallocated in fixed windows (default 1 MiB). The region between the last written frame and the physical end of the file is zero filler; a frame header whose length field is zero terminates recovery, and any nonzero length that fails validation remains fatal. The logical end of the log is therefore the offset of the first zero length field, not the file size.

## Segment

`STRSEG1\n`, `u64 count`, then each entry's `u32 keyLength`, `u32 valueLength`, key and value. A final `u32 CRC-32` covers all bytes after the magic. Entries are strictly unique and ordered by unsigned key bytes.

## Manifest

`STRMAN1\n`, `u64 sequence`, `u64 generation`, `u64 segmentChecksum`, and `u32 CRC-32` of the three numeric fields. The manifest selects `segment-G.sst` and `wal-G.log`, where `G` is its generation. Segment and WAL contents are written and forced under temporary names before the manifest replacement and directory synchronization. One manifest replacement therefore selects one consistent generation; a segment or WAL left by an uncommitted generation is ignored.

The committed segment's CRC-32 identity is checked against the manifest during recovery. Old generation files are retained for diagnostics in this early milestone, so repeated compaction consumes additional disk space; an explicit reclamation policy is future work. The engine must not run out of disk headroom silently.

Version 1 readers reject other magic and malformed lengths. No automatic migration exists in this 0.x milestone.
