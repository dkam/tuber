Tuber Body-Spill / TOAST Design
Goal
Allow Tuber to absorb bursty workloads where ingest rate temporarily exceeds processing rate, without holding all job bodies in RAM. Splat is the motivating use case: Sentry-style envelope ingest with 10–100KB JSON payloads, where a few hours of downstream backlog should be survivable rather than triggering OOM.
The default Tuber posture remains "if you're running out of RAM, you have a processing problem, not a queue problem." Body-spill is opt-in via persistence mode, not a workaround for slow workers.
Core decision: split storage, keep all metadata in RAM
Two separate on-disk stores when persistence is enabled (-b <dir>):

WAL — recovery log. Holds metadata records (FullJob with body-ref, StateChange) only. Small, churny, append-only. Replays on startup to rebuild the in-memory job map.
TOAST — body store. Holds raw body bytes. Append-only segments with their own lifecycle and GC.

All job metadata (priority, tube, state, idempotency keys, group membership, concurrency keys, counters) stays in RAM. Only the opaque body bytes go to TOAST.
Why split rather than bodies-in-WAL: the body-in-WAL approach (piggyback on existing FullJob records carrying the body) is simpler to ship but conflates two responsibilities — recovery logging and body storage — and the conflation makes WAL GC awkward (segments stay live because of bodies, not because of recovery needs). It also means restart replay drags 70GB of body bytes through to recover a few hundred MB of metadata. The split keeps the WAL small and fast, lets TOAST GC independently, and allows future evolution (different storage tiers, encryption-at-rest) without rewriting WAL records.
Why metadata stays in RAM: every hot-path operation (heap ordering, dedup, scheduling, group/concurrency accounting, aft: resolution) needs metadata, none needs the body. Bodies are only read at reserve/peek time, which is already an IO-bound operation (network response). Spilling metadata to disk would put a disk read on the hot path; spilling bodies hides the cost behind work we were doing anyway. A 10M-job backlog at ~200 bytes of metadata each is 2GB of RAM — manageable. The same backlog with 50KB bodies inline would be 500GB — not.
Always split bodies to TOAST when persistence is on
No threshold. Every body, regardless of size, goes through TOAST when -b is set.
Why no threshold: the obvious-seeming win for keeping small bodies inline is illusory. Small bodies are already going through the WAL (and would still go through it for the metadata record); the additional cost is one positioned read at reserve time against bytes that are almost certainly still in the OS page cache. That's tens of nanoseconds for the pread syscall, hidden behind the network response that's about to happen anyway. The cost of having a threshold is real: branches in every body-handling code path (Inline vs External), two cases for replay, two cases for stats, two cases to test. Always-external is one path, uniformly small WAL records, predictable replay speed, simpler mental model. Take the simplification.
Two operating modes
Clean two-mode matrix:

No -b: pure in-memory. Jobs (with bodies) live in the HashMap, OOM if you run out of RAM, lost on restart. Same as today.
-b <dir> + --max-storage-bytes N: WAL holds metadata, TOAST holds bodies, both durable, OUT_OF_STORAGE returned when budget exhausted, recovers on restart.

No hybrid modes, no thresholds between them. --max-storage-bytes is mandatory when -b is set.
Why mandatory: unbounded TOAST is exactly the kind of footgun that doesn't bite in testing and bites hard in production six months in, when the operator hasn't realised they're behind on processing. Forcing a number at startup means the operator has to think about the budget once. A single combined budget (WAL + TOAST) is simpler to reason about than two separate caps — "Tuber uses at most N bytes of disk" — and the internal split is Tuber's problem, not the operator's.
Storage budget enforcement: soft split with WAL reserve
Within --max-storage-bytes:

WAL gets a small reserved minimum (one segment's worth, ~10MB).
TOAST gets the rest.

Why a WAL reserve: if TOAST fills the entire budget, you can't fsync the StateChange records (deletes, releases) that would free TOAST space. Deadlock. The WAL reserve must be at least enough to flush a worst-case batch of StateChanges and let space recovery proceed. One segment's worth is plenty.
When the budget is hit, new puts return OUT_OF_STORAGE (renamed from OUT_OF_MEMORY — different cause, different operator response).
Logical body addressing
Metadata never names physical body locations. Instead:
struct JobMeta { ... body_ref: BodyRef, ... }
enum BodyRef { Inline(Vec<u8>), External(BodyId) }
struct BodyId(u64);  // monotonic, never reused
The TOAST store maintains an in-memory index BodyId → BodyLocation { segment, offset, len, ... }.
Why the indirection from day one: if metadata records named physical locations directly (segment, offset, len), the body store could never be reorganised — compaction, segment migration, format changes would all require rewriting the durable WAL records that reference them. Logical IDs let the body store evolve underneath without touching the WAL. This is the same reason Postgres TOAST uses logical OIDs. The indirection is cheap (32 bytes per entry, in-RAM index, bounded by job count not body size) and expensive to retrofit. Take it from v1 even though body-in-WAL approaches don't strictly need it — it preserves the option to refactor later.
Inline remains in the enum even though current design always uses External when persistence is on, because the no--b mode still uses Inline. Keeps one Job struct shape across modes.
Index recovery
On startup, the BodyId → BodyLocation index is reconstructed by scanning TOAST segment headers (not body bytes). Each body in a segment carries a small header: BodyId, original_len, stored_len, checksum. Header scan is sequential reads, body bytes are skipped — fast even for large stores.
Why this approach: the index is the canonical truth and is reconstructible from disk, so it doesn't need its own durability mechanism. Periodic index snapshots could speed up startup further but add moving parts; not worth it in v1. Header scan is simple, self-correcting (any drift between index and disk is fixed by restart), and the recovery cost is bounded by job count rather than total body bytes.
TOAST segment GC and compaction
Per-segment utilisation tracked in RAM (segment_id → live_bytes). On body delete, decrement the counter. When live_bytes / total_bytes drops below a threshold (start with 50%), schedule the segment for compaction: walk live bodies, copy them to the current segment with new locations, atomically update the index, delete the old segment.
Why this design: standard log-structured-store pattern. Eager compaction would cause write amplification on hot delete patterns; lazy compaction with a utilisation threshold strikes the balance. Same shape as DuckLake's merge_adjacent_files. Compaction works precisely because of logical body addressing — the index update is the only thing that needs to happen atomically.
Single sync interval, TOAST-then-WAL ordering
One --sync-interval (rename from --wal-sync-interval) covers both stores. On each tick: fsync TOAST first, then fsync WAL, then acknowledge clients waiting on puts in that window.
Why one interval, not two: the durability invariant is "for any acknowledged put, both the body and the metadata-with-body-ref must survive a crash." TOAST must be durable before or at the same time as the WAL record referencing it. If WAL syncs more often than TOAST, you create a window where metadata is durable but bodies aren't — dangling references on crash. The reverse direction (TOAST more aggressive than WAL) has no clear use case: both stores are sequential append, fsync cost is dominated by the syscall not the byte count, and there's no workload where syncing one twice as often as the other is correct.
The TOAST-then-WAL order means a crash mid-sync produces orphan bodies (TOAST has bytes, WAL doesn't reference them) — wasted space, not corruption. Cleaned up on next compaction or detected on startup.
Two-interval design was reflexive symmetry — split stores, two policies — that doesn't survive examination. One coordinated sync is correct.
No compression in v1
Users who care about compression compress their own bodies before put. Tuber stays focused on being a queue, not a storage system pretending to be a queue.
Why drop compression: the original argument (zstd on JSON is "better than free" because less disk write) is correct in isolation but the per-tube config required to turn it on creates more problems than it solves. Tubes are ephemeral — they come and go with traffic — so per-tube config drifts and surprises operators. Users who know they want compression know how to compress; users who don't know shouldn't be making that decision implicitly via tube settings. Cut it.
If a user has a real need (e.g., large JSON payloads with crypto-bound CPU budgets), they can compress client-side and Tuber is none the wiser.
What this enables

Splat-style backlog absorption: 100 events/sec with 50KB payloads through a 4-hour downstream outage = 1.4M jobs / ~70GB. With body-spill, that's ~280MB metadata in RAM + 70GB streaming sequentially through TOAST on disk. Survivable.
Image/binary-pipeline workloads: Tuber becomes a credible durable, ordered, scheduling-aware staging area for binary blobs. Sidekiq stuffs them in Redis (RAM-bound), Solid Queue stuffs them in SQLite (write-amplified), beanstalkd's default 64KB job size cap blocks them entirely. Tuber-with-TOAST handles natively, falls out of the same machinery splat needs.
Predictable resource bounds: operators specify one number (--max-storage-bytes) and get a predictable resource envelope. RAM scales with job count, disk scales with body volume, both bounded.

What this does not enable

Unbounded job count. Even at ~200 bytes of metadata each, 100M resident jobs is 20GB of RAM. At that scale, a different system (indexed disk storage for metadata too) is appropriate. Tuber doesn't need to be that system.
Ultra-low-latency reserve for huge bodies. A reserve always pays a pread against the TOAST segment. For typical workloads this is page-cache-hot and microseconds; for cold reads on bodies last touched hours ago, it's a real disk read. Acceptable for a queue, not for a database.

Implementation summary

New BodyStore module managing TOAST segments, in-memory BodyId → BodyLocation index, segment headers with checksums, free-space tracking, compaction.
Job.body: Vec<u8> → Job.body: BodyRef (enum of Inline | External(BodyId)).
WAL FullJob record format: replace inline body bytes with BodyId reference. Bump WAL version (v3 → v4).
Startup recovery: WAL replay rebuilds metadata, TOAST header scan rebuilds body index.
Put path: assign BodyId, write body to TOAST, write WAL record with BodyId, sync both on next tick, ack on sync.
Reserve path: lookup BodyId in index, positioned read against segment, send to client.
Delete path: WAL StateChange as today, plus index removal and free-space accounting.
Rename --wal-sync-interval → --sync-interval (deprecate old name).
Rename OUT_OF_MEMORY → OUT_OF_STORAGE for the persistence-budget case (keep OUT_OF_MEMORY for the no--b mode where it still applies).
New: --max-storage-bytes (mandatory with -b), enforces combined WAL+TOAST budget with WAL reserve.

Surface area is contained. Existing code is structured well enough to absorb this without major surgery — the cleanest way in is to introduce BodyRef and the BodyStore module as new types, then change the put/reserve/replay paths to use them.
