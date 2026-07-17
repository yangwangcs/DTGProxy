# DTGProxy Raft Command V1

Raft Entry data is a stable wire protocol, not a serialization of Rust memory. Every integer is
big-endian. Every variable byte string is prefixed by an unsigned 32-bit length. CRC-32/ISO-HDLC
covers every byte except the checksum itself.

## Envelope

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | magic `DTRC` |
| 4 | 2 | version, currently `1` |
| 6 | 1 | body tag: `1` ApplyPrepared, `2` ClosedTimestampTick |
| 7 | 1 | flags, must be zero |
| 8 | 4 | shard ID |
| 12 | 8 | placement epoch |
| 20 | 16 | client request ID |
| 36 | 4 | body byte length |
| 40 | N | body |
| 40+N | 4 | CRC-32 checksum |

The complete command is limited to 16 MiB. A decoder verifies the declared exact length and
checksum before allocating mutation data or interpreting a body.

## ApplyPrepared body

| Bytes | Field |
|---:|---|
| 8 | commit physical microseconds, signed two's-complement |
| 4 | commit logical counter |
| 4 | prepared-batch shard ID; must equal the envelope shard |
| 16 | transaction ID |
| 4 | mutation count, at most 65,536 |
| variable | canonical mutation sequence |

Each mutation contains `sequence:u32`, `operation:u8`, `keyspace:u8`, and a length-delimited key.
Put (`operation=1`) additionally contains a length-delimited value; Delete (`operation=2`) does
not. Sequences must be exactly `0..count` in wire order. Keys are limited to 64 KiB, values to 8
MiB, and all eight Phase 1 Keyspace tags remain unchanged.

ApplyPrepared carries the already validated and rewritten logical mutations. A Replica assigns
the Raft Entry index when applying the batch. Apply must not reread graph state, choose timestamps,
or regenerate mutations.

## ClosedTimestampTick body

The body is exactly one transaction timestamp: signed 64-bit physical microseconds followed by an
unsigned 32-bit logical counter. The state machine accepts only monotonic ticks and derives a
servable timestamp from durable closed, resolved, and Adapter-applied watermarks.

## Compatibility rules

- Unknown versions, flags, body tags, mutation tags, and Keyspace tags fail closed.
- Trailing bytes and noncanonical or duplicate mutation sequences fail closed.
- V1 bytes are immutable. A future format uses a new envelope version or body tag; it does not
  reinterpret an existing V1 byte sequence.
- The golden ClosedTimestampTick vector is maintained by `crates/raft-command/tests/codec.rs`.
