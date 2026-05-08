# WAL + TOAST Binary Format Specification

WAL version 5 + TOAST version 1 — Tuber persistence layer.

## Overview

When persistence is enabled (`-b <dir>`), tuber maintains two on-disk stores
side by side:

- **WAL** (`binlog.NNNNNN`) — metadata records (FullJob carrying a `BodyId`
  reference, StateChange for delete/release/bury/kick/timeout). Small, churny,
  append-only. Replays on startup to rebuild the in-memory job map.
- **TOAST** (`toast/body.NNNNNN`) — append-only body segments holding raw job
  payload bytes, addressed by `BodyId`. The in-memory `BodyId → BodyLocation`
  index is reconstructed on startup by scanning segment headers.

```
wal-dir/
  lock              # flock — prevents concurrent access
  binlog.000001     # oldest WAL segment
  binlog.000002
  binlog.000003     # current writable WAL segment
  toast/
    body.000000     # oldest TOAST segment
    body.000001     # current writable TOAST segment
```

All multi-byte integers in both stores are **little-endian**.

The two stores are kept consistent by the **TOAST-then-WAL fsync ordering**:
every WAL fsync is preceded by a TOAST fsync. A crash mid-sync therefore
leaves orphan bodies (TOAST has bytes, WAL doesn't reference them) — wasted
space, never dangling references. Orphans are reclaimed on the next replay.

---

# Part 1 — WAL format

## File Header

```
 0                   1                   2
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  'T'  'W'  'A'  'L'  |     version (u32)     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      flags (u32)      |
+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Offset | Size | Field     | Value                                      |
|--------|------|-----------|--------------------------------------------|
| 0      | 4    | magic     | `TWAL` (`0x5457414C`)                      |
| 4      | 4    | version   | `5` (u32 LE) — current; v3 and v4 still readable |
| 8      | 4    | flags     | `0` — reserved for future use              |

**Total: 12 bytes**

The `flags` field is reserved for forward compatibility. Currently written as
zero and ignored on read.

## Record Types

Two record types follow the header. Both share a common envelope:

```
+-----------+------------------+------------------+---------+----------+
| type (u8) |   job_id (u64)   | payload_len (u32)|  payload | crc (u32)|
+-----------+------------------+------------------+---------+----------+
     1              8                  4            variable      4
```

The CRC32 (`crc32fast`) covers everything from the type byte through the end
of the payload — the CRC itself is excluded.

### 0x01 — FullJob (v5)

Written on `put`. Contains the complete job metadata plus a `BodyId`
reference into the TOAST store. The body bytes themselves live in TOAST,
not in this record.

```
+------+----------+-------------+-----------------------------------+-----+
| 0x01 | job_id   | payload_len |            payload                | crc |
|  1B  |   8B     |     4B      |          variable                 | 4B  |
+------+----------+-------------+-----------------------------------+-----+
```

**Payload layout (v5)** — field order matches `serialize_full_job`:

```
+------------------------------------------------------------------+
| priority (u32)                                                    |  4
+------------------------------------------------------------------+
| delay_nanos (u64)                                                 |  8
+------------------------------------------------------------------+
| ttr_nanos (u64)                                                   |  8
+------------------------------------------------------------------+
| created_at_epoch_secs (u64)                                       |  8
+------------------------------------------------------------------+
| state (u8)                                                        |  1
+------------------------------------------------------------------+
| reserve_ct (u32)                                                  |  4
+------------------------------------------------------------------+
| timeout_ct (u32)                                                  |  4
+------------------------------------------------------------------+
| release_ct (u32)                                                  |  4
+------------------------------------------------------------------+
| bury_ct (u32)                                                     |  4
+------------------------------------------------------------------+
| kick_ct (u32)                                                     |  4
+------------------------------------------------------------------+
| tube_name_len (u16) | tube_name (bytes)                          |  2+N
+------------------------------------------------------------------+
| idempotency_key          (option_string)                          |  2+N
+------------------------------------------------------------------+
| idempotency_ttl (u32)                                             |  4
+------------------------------------------------------------------+
| group                    (option_string)                          |  2+N
+------------------------------------------------------------------+
| after_group              (option_string)                          |  2+N
+------------------------------------------------------------------+
| concurrency_key          (option_string)                          |  2+N
+------------------------------------------------------------------+
| concurrency_limit (u32)                                           |  4
+------------------------------------------------------------------+
| body_id (u64)                                                     |  8     <-- v5
+------------------------------------------------------------------+
```

The trailing `body_id` is what changed in v5: it replaced the v3/v4 inline
body fields (`body_len (u32)` + raw bytes). The body itself is read from
TOAST at reserve/peek time using this id.

### 0x02 — StateChange (v4+)

Written on `delete`, `bury`, `release`, `kick`, `timeout`. Fixed-size
payload (22 bytes in v4+; v3 was 21 bytes — see version history).

```
+------+----------+-------------+----------------------+-----+
| 0x02 | job_id   | payload_len |       payload        | crc |
|  1B  |   8B     |  4B (= 22) |        22B           | 4B  |
+------+----------+-------------+----------------------+-----+
                                 |                      |
                                 v                      |
                   +-------+-----+-----------+--------+----+
                   | state | pri | delay_ns  | expiry | rsn |
                   |  1B   | 4B  |    8B     |   8B   | 1B  |
                   +-------+-----+-----------+--------+-----+
```

| Offset* | Size | Field              | Notes                                   |
|---------|------|--------------------|-----------------------------------------|
| 0       | 1    | state              | New state, or `0xFF` for deleted        |
| 1       | 4    | new_priority       | Updated priority (u32)                  |
| 5       | 8    | new_delay_nanos    | Updated delay in nanoseconds (u64)      |
| 13      | 8    | expiry_epoch_secs  | Idempotency tombstone expiry (u64), 0 = none |
| 21      | 1    | reason             | StateChangeReason (see below) — v4+ only |

*Offsets relative to payload start.

**Total record size: 39 bytes** (1 + 8 + 4 + 22 + 4)

The `reason` byte was added in v4 so replay can reconstruct per-job
counters (`reserve_ct`, `release_ct`, `bury_ct`, `kick_ct`, `timeout_ct`).
v3 records replay with `reason = None` and don't increment counters.

## Encoding Details

### State Encoding

| Value  | State      |
|--------|------------|
| `0x00` | Ready      |
| `0x01` | Reserved   |
| `0x02` | Delayed    |
| `0x03` | Buried     |
| `0xFF` | Deleted    |

### StateChangeReason Encoding (v4+)

| Value | Reason   | Counter incremented on replay |
|-------|----------|------------------------------|
| `0`   | None     | (none)                       |
| `1`   | Reserve  | `reserve_ct`                 |
| `2`   | Release  | `release_ct`                 |
| `3`   | Bury     | `bury_ct`                    |
| `4`   | Kick     | `kick_ct`                    |
| `5`   | Timeout  | `timeout_ct`                 |

### option_string

Variable-length nullable string, used for extension fields.

```
+----------------+-----------------+
| len (u16 LE)   | bytes (UTF-8)   |
+----------------+-----------------+
```

- `len = 0` → None (no bytes follow)
- `len > 0` → Some(string), `len` bytes of UTF-8 follow

### Idempotency Tombstones

When a job with an `idp:` key and TTL is deleted, the StateChange record's
`expiry_epoch_secs` is set to the UNIX timestamp when the tombstone expires.
On replay, tombstones with `expiry_epoch_secs > now` are restored to prevent
re-insertion of recently completed jobs.

## Replay Semantics

On startup, files are read in sequence order. For each record:

- **FullJob (v5)**: Insert or replace in the job map; `body` is set to
  `BodyRef::External(BodyId)`. The corresponding body bytes are looked up
  from TOAST at reserve/peek time.
- **FullJob (v3/v4)**: Inline body bytes are read into a `BodyRef::Inline`,
  then **migrated** into TOAST during the post-replay step (see
  "Legacy migration" below). v3/v4 reads require the `--migrate-wal` flag.
- **StateChange (delete)**: Remove from job map. If `expiry_epoch_secs > now`,
  extract the idempotency key and restore the tombstone. Track the body's
  `BodyId` as a candidate orphan for the post-replay reclaim step.
- **StateChange (other)**: Update the job's state, priority, and delay.
  Increment the counter named by `reason` (v4+).

State adjustments during replay:

- **Reserved → Ready** — reservations don't survive restarts
- **Delayed** — `deadline_at` reconstructed as `Instant::now() + delay`
- `reserver_id` always set to `None`

### Orphan body reclamation

After replay, the WAL returns a list of `BodyId`s belonging to jobs whose
StateChange-delete records survived but whose owning job is no longer in
the live set. Replay filters out any id that has been re-used by a re-put
within the same WAL (same job_id, fresh body) so live bodies are never
collected. The remaining ids are passed to `BodyStore::delete_many` to
reclaim disk space — this is what makes a crash between TOAST fsync and
runtime `BodyStore::delete` self-healing.

### Legacy migration (`--migrate-wal`)

When the server detects pre-v5 WAL records on startup it refuses to start
unless `--migrate-wal` is set. With the flag, the replay path lifts inline
body bytes from v3/v4 FullJob records into TOAST (assigning fresh
`BodyId`s), fsyncs TOAST, and proceeds. Subsequent writes are v5.

The migration is one-way and in-place. Back up the WAL directory first if
you want a rollback option.

### Corruption handling

Corrupt or truncated records terminate processing of that file (with a
warning). Valid records before the corruption point are preserved. The
file is truncated at the corrupt offset to prevent repeated warnings on
future restarts.

## Durability

Writes pass through a 64 KiB userland `BufWriter` to amortise syscall
overhead. `fsync` cadence is controlled by `--sync-interval` (env
`TUBER_SYNC_INTERVAL`, default `100ms`). The legacy `--wal-sync-interval`
flag is accepted as a hidden alias for backward compatibility.

The same interval drives both WAL and TOAST fsyncs. On every sync tick,
TOAST is fsynced *first*, then WAL — see the ordering rationale in the
overview.

- **`--sync-interval 0`** — TOAST and WAL are flushed and `fsync`'d on every
  put/state-change *before* the server acknowledges the client. Zero
  ack-vs-durable window; the engine task blocks on fsync, so throughput
  drops sharply.
- **`--sync-interval <duration>`** (e.g. `100ms`, `1s`) — writes are buffered
  and `fsync` runs at most once per interval, plus once on segment rotation
  and once on clean shutdown. Up to `interval` of acknowledged records can
  be lost on a crash. On clean shutdown the tail is always synced regardless
  of interval.

There is no "never fsync" mode: if you don't care about durability at all,
don't pass `-b`/`--binlog-dir`. Setting a very large interval (e.g. `24h`)
approximates that behaviour while still syncing on clean shutdown.

When `sync_interval` is shorter than the engine's 100 ms tick, the tick
shortens to match so fsyncs don't back up behind tick cadence.

## File Management

### Segmentation and Rotation

WAL files rotate when the current segment reaches `max_file_size` (default
10 MiB). The current file is fsynced before a new segment is created.

StateChange records are allowed to slightly exceed the size limit — this
avoids per-file balancing complexity while keeping segments roughly bounded.

### Garbage Collection

Each WAL file tracks a reference count (`refs`) — the number of live jobs
whose most recent FullJob record is in that file. On each server tick,
head files with `refs == 0` are deleted. The current writable file is
never removed.

### WAL compaction

When the dead-to-live ratio across all WAL files reaches 2:1, the engine
migrates jobs out of the oldest file into the current one (re-writing
their FullJob records) at a rate of `ratio` jobs per tick. This frees the
oldest file for GC. Self-regulating: more waste = more migration.

### Storage budget

`--max-storage-bytes` is **mandatory** when `-b` is set. It caps the
combined WAL + TOAST footprint. Puts return `OUT_OF_STORAGE` once the
projected footprint plus a one-WAL-segment reserve would exceed the
budget. State changes (delete, release, bury, kick, touch) bypass the
cap entirely so an operator can always drain a wedged queue.

The WAL reserve guarantees there's always room to journal the deletes
that would free TOAST space — without it, a TOAST-full state would
deadlock.

### Directory Locking

The WAL directory is exclusively locked via `flock(2)` on a `lock` file.
This prevents multiple tuber instances from writing to the same WAL.

## Version History

| Version | Header Size | Changes |
|---------|-------------|---------|
| 1       | 8 bytes     | Initial format: magic + version. |
| 2       | 12 bytes    | Added `flags` (u32). Added `expiry_epoch_secs` to StateChange for idempotency tombstones. Added `concurrency_limit` and `idempotency_ttl` to FullJob payload. |
| 3       | 12 bytes    | Reordered FullJob payload: body moved to end, idempotency_ttl grouped with idempotency_key, concurrency_limit grouped with concurrency_key. |
| 4       | 12 bytes    | Added `reason` (u8) byte to StateChange payload (21 → 22 bytes), enabling per-job counter reconstruction on replay. |
| 5       | 12 bytes    | Replaced inline body bytes (`body_len u32` + raw bytes) in FullJob with `body_id (u64)` reference. Bodies live in TOAST. v3/v4 reads still supported via `--migrate-wal`. |

---

# Part 2 — TOAST format

The TOAST ("The Oversized-Attribute Storage Technique") store holds raw
job body bytes, addressed by monotonically-increasing `BodyId`. WAL
records reference body ids; the in-memory `BodyId → BodyLocation` index
maps each id to a `(segment_seq, offset, len)` triple. The index is
**not** persisted — it's rebuilt on startup by scanning the file header
and per-body record headers in each segment file (body bytes themselves
are skipped during scan, so recovery cost is bounded by job count, not
body volume).

## Files

```
toast/
  body.000000     # oldest TOAST segment
  body.000001
  body.000002     # current writable segment
```

Files are named `body.NNNNNN` (zero-padded sequence number). New segments
are created when the current one would exceed the segment size limit
(default 64 MiB). Segments are sealed and `fsync`'d before rotation; a
freshly-created segment is also `fsync`'d after writing its file header.

## File Header

```
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| 'T'  'B'  'O'  'D' | version  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         reserved (u64)         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Offset | Size | Field    | Value                            |
|--------|------|----------|----------------------------------|
| 0      | 4    | magic    | `TBOD` (`0x54424F44`)            |
| 4      | 4    | version  | `1` (u32 LE)                     |
| 8      | 8    | reserved | `0` — for future use             |

**Total: 16 bytes**

## Body Records

After the file header, each body is laid out as:

```
+------------------------------------+
| body_id (u64)                       |  8
+------------------------------------+
| len (u32)                           |  4
+------------------------------------+
| crc32 (u32)                         |  4
+------------------------------------+
| reserved (u32)                      |  4
+------------------------------------+
| body bytes (len)                    |  N
+------------------------------------+
```

**Header total: 20 bytes** + `len` body bytes.

- `body_id` — the logical id stored in the WAL's FullJob record.
- `len` — body byte count (does not include the header).
- `crc32` — `crc32fast` over the body bytes only. Header corruption is
  handled separately at scan time (truncated tails are recovered, see
  below).
- `reserved` — currently zero, available for future per-body flags.

The CRC is verified on every read. Compaction also re-verifies the CRC
when copying bodies to a new segment, so silent disk corruption surfaces
loudly rather than silently propagating.

## In-RAM Index

The store maintains:

```rust
HashMap<BodyId, BodyLocation { seq, offset, len }>
```

- `seq` — the segment sequence number where the body lives.
- `offset` — byte offset of the **body data** within the file (i.e. the
  body header sits at `offset - 20`).
- `len` — body byte count (mirrors the header field for fast budget
  accounting).

The index is reconstructed on startup by `BodyStore::open`: each segment
is opened, its file header is validated, and its body record headers are
walked sequentially. Body bytes are skipped during scan; only header
bytes are read.

## Compaction

A background tokio task scans every 5 seconds for sealed segments whose
**live ratio** (`live_bytes / total_bytes`, where `live_bytes` excludes
deleted bodies) has dropped below `0.5`. The most-wasted qualifying
segment is compacted: its surviving bodies are migrated into the current
write segment (with a stale-entry guard so concurrent deletes can't
resurrect a body), then the old segment file is unlinked.

Outstanding `Arc<File>` handles held by in-flight readers keep the file
descriptor valid through the unlink (POSIX semantics) — that's what makes
unlink-during-read safe.

Compaction works precisely *because* the WAL references logical
`BodyId`s, not physical locations: the index update is the only
synchronisation point.

## Crash Safety

- **TOAST-then-WAL fsync ordering** (see overview) — a WAL record never
  becomes durable before the body it references. A crash leaves orphan
  bodies, never dangling references.
- **Truncated tail recovery** — `BodyStore::open` stops scanning at the
  first incomplete record and records `consumed = offset`. The next write
  resumes at `consumed`, overwriting the partial record.
- **Orphan reclamation** — bodies whose owning jobs were deleted before
  the runtime `BodyStore::delete` could run are detected during WAL
  replay and dropped via `BodyStore::delete_many`.
- **Compaction restart** — if a crash happens between writing the
  migrated body and unlinking the old segment, the same `BodyId` exists
  in two segments. On restart the header scan iterates segments in seq
  order; the new (later) location wins via `index.insert`. Subsequent
  compaction reclaims the duplicate.

## TOAST Version History

| Version | Changes |
|---------|---------|
| 1       | Initial format: 16-byte file header + 20-byte body record headers + raw bytes. CRC32 over body bytes only. |
