// External body store ("TOAST"): append-only segments holding raw job
// payloads, addressed by `BodyId`. Used when persistence is enabled so the
// WAL only needs to reference bodies by id rather than carry their bytes.
//
// On-disk format
// --------------
// Each segment file (`body.NNNNNN`) starts with a 16-byte file header:
//   magic(4) "TBOD" | version(4) u32 LE | reserved(8) u64
// followed by a sequence of body records:
//   header(20) = body_id(8) u64 LE | len(4) u32 LE | crc32(4) u32 LE | reserved(4)
//   body bytes (len)
// All multi-byte integers are little-endian. The CRC covers the body bytes
// only — header corruption is handled separately at scan time.
//
// In-RAM state
// ------------
// The store keeps a `HashMap<BodyId, BodyLocation>` so reads can issue a
// single positioned read against the right segment. Index entries are
// reconstructed on startup by walking each segment's headers; body bytes
// are skipped, so recovery cost is bounded by job count, not body volume.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::job::BodyId;

// --- Constants ---

const TOAST_MAGIC: &[u8; 4] = b"TBOD";
const TOAST_VERSION: u32 = 1;
/// Bytes occupied at the head of every segment file: magic(4) + version(4) + reserved(8).
pub const FILE_HEADER_SIZE: usize = 16;
/// Bytes occupied by each body's record header: body_id(8) + len(4) + crc32(4) + reserved(4).
pub const BODY_HEADER_SIZE: usize = 20;
const TOAST_FILE_PREFIX: &str = "body.";

/// Default segment size (64 MiB). Operator-tunable via the eventual
/// `--toast-segment-size` flag.
pub const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Live-bytes ratio below which a sealed segment becomes a compaction
/// candidate. At 0.5, half-empty segments get rewritten — small enough
/// that the rewrite cost is bounded, large enough that fragmentation
/// stays in check.
pub const COMPACTION_LIVE_RATIO_THRESHOLD: f64 = 0.5;

// --- Errors ---

#[derive(Debug)]
pub enum BodyStoreError {
    Io(io::Error),
    BadMagic,
    BadVersion(u32),
    BadCrc { expected: u32, found: u32, body_id: BodyId },
    NotFound(BodyId),
}

impl From<io::Error> for BodyStoreError {
    fn from(e: io::Error) -> Self {
        BodyStoreError::Io(e)
    }
}

impl std::fmt::Display for BodyStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BodyStoreError::Io(e) => write!(f, "TOAST I/O: {}", e),
            BodyStoreError::BadMagic => write!(f, "TOAST bad magic"),
            BodyStoreError::BadVersion(v) => write!(f, "TOAST bad version: {}", v),
            BodyStoreError::BadCrc { expected, found, body_id } => write!(
                f,
                "TOAST CRC mismatch for body {}: expected {:08x}, got {:08x}",
                body_id.0, expected, found
            ),
            BodyStoreError::NotFound(id) => write!(f, "TOAST body {} not found", id.0),
        }
    }
}

impl std::error::Error for BodyStoreError {}

impl From<BodyStoreError> for io::Error {
    fn from(e: BodyStoreError) -> Self {
        match e {
            BodyStoreError::Io(io_err) => io_err,
            other => io::Error::other(other.to_string()),
        }
    }
}

// --- Types ---

/// Where a body lives on disk. Stable across restarts (the index is
/// rebuilt by scanning segment headers on open).
#[derive(Debug, Clone, Copy)]
pub struct BodyLocation {
    pub seq: u64,
    pub offset: u64, // offset of the body bytes (after the body header)
    pub len: u32,
}

struct Segment {
    file: Arc<File>,
    /// Total bytes used in the file (including file header and all body
    /// headers). Equal to the next-write offset for the current segment.
    total_bytes: u64,
    /// Bytes still referenced by live `BodyId`s. Counts body bytes only,
    /// not headers — drives the eventual compaction trigger.
    live_bytes: u64,
}

struct Inner {
    /// All segments, keyed by seq.
    segments: BTreeMap<u64, Segment>,
    /// The seq of the segment we're currently appending to. `None` until the
    /// first body is written.
    current_seq: Option<u64>,
    next_seq: u64,
    index: HashMap<BodyId, BodyLocation>,
}

pub struct BodyStore {
    dir: PathBuf,
    segment_size: u64,
    next_body_id: AtomicU64,
    /// Sum of `Segment.total_bytes` across all segments. Mirrors per-segment
    /// state but is read lock-free from the put hot path's storage-budget
    /// check, where the inner mutex would otherwise serialize against
    /// concurrent writes.
    total_bytes: AtomicU64,
    /// Sum of body bytes still referenced. Compaction trigger.
    live_bytes: AtomicU64,
    /// Count of segment files currently on disk. Mirrors `Inner.segments.len()`
    /// but readable lock-free so the stats / Prometheus paths don't acquire
    /// the inner mutex on every poll.
    segment_count: AtomicUsize,
    /// Number of `compact_segment` calls that ran to completion (including
    /// no-op compactions of empty segments). Surfaced via `stats`.
    compactions_total: AtomicU64,
    /// Number of bodies physically rewritten across all compactions.
    /// Useful for spotting churn that's costing IO without freeing much
    /// disk.
    bodies_migrated_total: AtomicU64,
    /// Number of bodies dropped during compaction because their CRC failed
    /// to verify against the on-disk record. Bumped per-body, not per-
    /// compaction. Every increment indicates real bit-rot — alert on
    /// `rate(...) > 0`. The corresponding `BodyId` becomes unreadable
    /// (subsequent reserve/peek returns NotFound, surfaced as
    /// INTERNAL_ERROR by the server).
    bodies_dropped_corrupted: AtomicU64,
    inner: Mutex<Inner>,
}

// --- Public API ---

impl BodyStore {
    /// Open a `BodyStore` rooted at `dir`. Creates the directory if missing.
    /// Existing segments are scanned to rebuild the in-memory index. The
    /// next assigned `BodyId` is one greater than the highest id seen on
    /// disk, so ids remain monotonic across restarts.
    pub fn open(dir: &Path, segment_size: u64) -> Result<Self, BodyStoreError> {
        fs::create_dir_all(dir)?;

        let mut seqs: Vec<u64> = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(s) = name.strip_prefix(TOAST_FILE_PREFIX)
                && let Ok(seq) = s.parse::<u64>()
            {
                seqs.push(seq);
            }
        }
        seqs.sort();

        let mut segments: BTreeMap<u64, Segment> = BTreeMap::new();
        let mut index: HashMap<BodyId, BodyLocation> = HashMap::new();
        let mut max_body_id: u64 = 0;
        let mut total_bytes_acc: u64 = 0;
        let mut live_bytes_acc: u64 = 0;

        for seq in &seqs {
            let path = segment_path(dir, *seq);
            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            let scan = scan_segment(&mut file)?;
            let mut live_bytes: u64 = 0;
            for entry in scan.entries {
                max_body_id = max_body_id.max(entry.body_id.0 + 1);
                live_bytes += entry.len as u64;
                index.insert(
                    entry.body_id,
                    BodyLocation { seq: *seq, offset: entry.body_offset, len: entry.len },
                );
            }
            total_bytes_acc += scan.consumed;
            live_bytes_acc += live_bytes;
            segments.insert(
                *seq,
                Segment { file: Arc::new(file), total_bytes: scan.consumed, live_bytes },
            );
        }

        let next_seq = seqs.last().map(|s| s + 1).unwrap_or(0);
        let current_seq = seqs.last().copied();

        let segment_count = segments.len();
        Ok(BodyStore {
            dir: dir.to_path_buf(),
            segment_size,
            next_body_id: AtomicU64::new(max_body_id),
            total_bytes: AtomicU64::new(total_bytes_acc),
            live_bytes: AtomicU64::new(live_bytes_acc),
            segment_count: AtomicUsize::new(segment_count),
            compactions_total: AtomicU64::new(0),
            bodies_migrated_total: AtomicU64::new(0),
            bodies_dropped_corrupted: AtomicU64::new(0),
            inner: Mutex::new(Inner { segments, current_seq, next_seq, index }),
        })
    }

    /// Append `bytes` to the current segment (rotating first if it would
    /// exceed `segment_size`). Returns the assigned `BodyId`.
    ///
    /// Bytes hit the kernel via `pwrite`, but durability requires a
    /// subsequent [`BodyStore::fsync`].
    pub fn write_body(&self, bytes: &[u8]) -> Result<BodyId, BodyStoreError> {
        let body_id = BodyId(self.next_body_id.fetch_add(1, Ordering::SeqCst));
        let crc = crc32fast::hash(bytes);
        let mut inner = self.inner.lock().unwrap();
        let location = self.append_body_locked(&mut inner, body_id, bytes, crc)?;
        inner.index.insert(body_id, location);
        self.live_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(body_id)
    }

    /// Append a body to the current segment under the lock, rotating
    /// first if needed. Updates per-segment counters and the global
    /// `total_bytes` atomic but does **not** insert into the index or
    /// touch `live_bytes` — callers handle those, since `write_body`
    /// adds a new live entry while `migrate_body` only relocates.
    fn append_body_locked(
        &self,
        inner: &mut Inner,
        body_id: BodyId,
        bytes: &[u8],
        crc: u32,
    ) -> Result<BodyLocation, BodyStoreError> {
        let needs_rotation = match inner.current_seq {
            None => true,
            Some(seq) => {
                let seg = inner.segments.get(&seq).expect("current segment present");
                seg.total_bytes + (BODY_HEADER_SIZE as u64) + (bytes.len() as u64)
                    > self.segment_size
            }
        };
        if needs_rotation {
            inner.rotate(&self.dir)?;
            self.total_bytes
                .fetch_add(FILE_HEADER_SIZE as u64, Ordering::Relaxed);
            self.segment_count.fetch_add(1, Ordering::Relaxed);
        }

        let current_seq = inner.current_seq.expect("rotation populates current_seq");
        let seg = inner
            .segments
            .get_mut(&current_seq)
            .expect("current segment present");
        let header_offset = seg.total_bytes;
        let body_offset = header_offset + BODY_HEADER_SIZE as u64;

        let header = encode_body_header(body_id, bytes.len() as u32, crc);
        seg.file.write_all_at(&header, header_offset)?;
        seg.file.write_all_at(bytes, body_offset)?;

        seg.total_bytes = body_offset + bytes.len() as u64;
        seg.live_bytes += bytes.len() as u64;
        let added = (BODY_HEADER_SIZE as u64) + bytes.len() as u64;
        self.total_bytes.fetch_add(added, Ordering::Relaxed);

        Ok(BodyLocation {
            seq: current_seq,
            offset: body_offset,
            len: bytes.len() as u32,
        })
    }

    /// Read the body bytes for `id`. Verifies the body's CRC against the
    /// header recorded at write time. The inner lock is released before
    /// disk IO, so reads can proceed concurrently.
    pub fn read_body(&self, id: BodyId) -> Result<Vec<u8>, BodyStoreError> {
        let (file, location) = {
            let inner = self.inner.lock().unwrap();
            let location = inner
                .index
                .get(&id)
                .copied()
                .ok_or(BodyStoreError::NotFound(id))?;
            let seg = inner
                .segments
                .get(&location.seq)
                .expect("indexed body's segment must exist");
            (Arc::clone(&seg.file), location)
        };

        let mut header = [0u8; BODY_HEADER_SIZE];
        file.read_exact_at(&mut header, location.offset - BODY_HEADER_SIZE as u64)?;
        let expected_crc = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);

        let mut buf = vec![0u8; location.len as usize];
        file.read_exact_at(&mut buf, location.offset)?;

        let found_crc = crc32fast::hash(&buf);
        if found_crc != expected_crc {
            return Err(BodyStoreError::BadCrc {
                expected: expected_crc,
                found: found_crc,
                body_id: id,
            });
        }

        Ok(buf)
    }

    /// Mark `id` as no longer referenced. Decrements the host segment's
    /// live-bytes counter. Bytes on disk are reclaimed by future
    /// compaction; deleting an unknown id is a silent no-op.
    pub fn delete(&self, id: BodyId) {
        let mut inner = self.inner.lock().unwrap();
        let freed = Self::delete_locked(&mut inner, id);
        drop(inner);
        if freed > 0 {
            self.live_bytes.fetch_sub(freed, Ordering::Relaxed);
        }
    }

    /// Bulk delete that takes the inner lock once. Hot for `flush-tube`
    /// and replay-time orphan cleanup.
    pub fn delete_many(&self, ids: &[BodyId]) {
        if ids.is_empty() {
            return;
        }
        let mut total_freed: u64 = 0;
        {
            let mut inner = self.inner.lock().unwrap();
            for id in ids {
                total_freed += Self::delete_locked(&mut inner, *id);
            }
        }
        if total_freed > 0 {
            self.live_bytes.fetch_sub(total_freed, Ordering::Relaxed);
        }
    }

    fn delete_locked(inner: &mut Inner, id: BodyId) -> u64 {
        if let Some(loc) = inner.index.remove(&id)
            && let Some(seg) = inner.segments.get_mut(&loc.seq)
        {
            let len = loc.len as u64;
            seg.live_bytes = seg.live_bytes.saturating_sub(len);
            return len;
        }
        0
    }

    /// `fsync` the current segment file. Sealed segments are synced before
    /// rotation, so they don't need re-syncing here.
    pub fn fsync(&self) -> Result<(), BodyStoreError> {
        let file = {
            let inner = self.inner.lock().unwrap();
            inner
                .current_seq
                .and_then(|seq| inner.segments.get(&seq))
                .map(|seg| Arc::clone(&seg.file))
        };
        if let Some(file) = file {
            file.sync_data()?;
        }
        Ok(())
    }

    /// Sum of bytes used across all segment files (file headers + body
    /// records). Drives the disk-budget calculation. Lock-free read.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Sum of body bytes still referenced by live ids. The ratio
    /// `live_bytes / total_bytes` per segment drives compaction.
    pub fn live_bytes(&self) -> u64 {
        self.live_bytes.load(Ordering::Relaxed)
    }

    /// Number of segments currently on disk. Lock-free read.
    pub fn segment_count(&self) -> usize {
        self.segment_count.load(Ordering::Relaxed)
    }

    /// Number of `compact_segment` calls completed since startup.
    pub fn compactions_total(&self) -> u64 {
        self.compactions_total.load(Ordering::Relaxed)
    }

    /// Number of bodies physically rewritten by compaction since startup.
    pub fn bodies_migrated_total(&self) -> u64 {
        self.bodies_migrated_total.load(Ordering::Relaxed)
    }

    /// Bit-rot counter — see the `bodies_dropped_corrupted` field doc.
    pub fn bodies_dropped_corrupted(&self) -> u64 {
        self.bodies_dropped_corrupted.load(Ordering::Relaxed)
    }

    /// Whether the given `BodyId` is known to the in-memory index. Cheap:
    /// a single HashMap lookup, no IO and no CRC verification. Used by
    /// the startup integrity check to detect WAL records referencing
    /// bodies that no longer exist in TOAST (silent bit-rot in a segment
    /// header, manual file removal, the corruption-drop path in
    /// `compact_segment`, etc.).
    pub fn contains_body(&self, id: BodyId) -> bool {
        self.inner.lock().unwrap().index.contains_key(&id)
    }

    /// Snapshot of every `BodyId` currently in the index. Allocates a
    /// `Vec` the size of the live body set — cheap at startup, would
    /// be expensive on the hot path. Used by the startup integrity
    /// check to find TOAST bodies that no WAL record references
    /// (the symmetric case of `contains_body`: `cmd_put` wrote the
    /// body successfully but `wal.write_put` failed, leaving a
    /// stranded body that nothing will ever read).
    pub fn body_ids(&self) -> Vec<BodyId> {
        self.inner.lock().unwrap().index.keys().copied().collect()
    }

    /// Drop every TOAST body whose `BodyId` is not in `live_ids`, then
    /// compact any segment that lost bodies as a result. The compaction
    /// step is what makes this idempotent: without it, the bytes stay
    /// in the segment file and `BodyStore::open` re-discovers them on
    /// the next restart.
    ///
    /// Returns the number of bodies reclaimed. Compaction errors are
    /// logged but don't fail the call — the index drop already
    /// succeeded; bytes will be reclaimed by the next regular
    /// compaction tick if startup compaction failed.
    ///
    /// Used by `build_state` for the symmetric counterpart of 4.5:
    /// when a `cmd_put` wrote the body but the WAL write failed, the
    /// body has no WAL anchor at all and no live job will ever
    /// reference it.
    pub fn reclaim_stranded(&self, live_ids: &std::collections::HashSet<BodyId>) -> u64 {
        // Snapshot stranded ids and the segments they live in under one
        // lock; release before doing per-segment compaction (which
        // takes the lock itself).
        let (stranded, affected_segs): (Vec<BodyId>, std::collections::HashSet<u64>) = {
            let inner = self.inner.lock().unwrap();
            let mut stranded = Vec::new();
            let mut segs = std::collections::HashSet::new();
            for (id, loc) in inner.index.iter() {
                if !live_ids.contains(id) {
                    stranded.push(*id);
                    segs.insert(loc.seq);
                }
            }
            (stranded, segs)
        };

        if stranded.is_empty() {
            return 0;
        }

        let count = stranded.len() as u64;
        self.delete_many(&stranded);

        // If a stranded body sits in the *current* write segment,
        // `compact_segment` would no-op on it (the current segment can
        // still accept appends, and unlinking it would lose the next-
        // write target). Force a rotation so that segment becomes
        // sealed and compactable. Cost: a fresh 16-byte segment file
        // that the next put will append into. Only worth it when the
        // current segment is genuinely affected.
        let current = self.inner.lock().unwrap().current_seq;
        if let Some(cur) = current
            && affected_segs.contains(&cur)
        {
            let mut inner = self.inner.lock().unwrap();
            if let Err(e) = inner.rotate(&self.dir) {
                tracing::warn!(
                    "stranded-body reclamation: rotation failed: {} \
                     (current segment's strandeds will linger until next \
                     compaction tick)",
                    e,
                );
            } else {
                self.total_bytes
                    .fetch_add(FILE_HEADER_SIZE as u64, Ordering::Relaxed);
                self.segment_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Compact each affected segment so the bytes physically leave
        // disk. compact_segment skips the (new) current segment.
        for seq in affected_segs {
            if let Err(e) = self.compact_segment(seq) {
                tracing::warn!(
                    "stranded-body compaction failed for segment {}: {} \
                     (bytes will linger until the next compaction tick)",
                    seq,
                    e,
                );
            }
        }

        count
    }

    /// Pick a sealed segment (i.e. not the one currently being appended to)
    /// whose live ratio has dropped below `threshold` (in [0.0, 1.0]). The
    /// most-wasted segment wins; ties go to the lowest seq. Returns `None`
    /// when nothing qualifies.
    ///
    /// A segment with `total_bytes == 0` is impossible (file header is
    /// always written), but a segment with `live_bytes == 0` is the
    /// natural happy case — every body got deleted, so the entire file
    /// is reclaimable.
    pub fn compaction_candidate(&self, threshold: f64) -> Option<u64> {
        let threshold = threshold.clamp(0.0, 1.0);
        let inner = self.inner.lock().unwrap();
        let current = inner.current_seq;
        let mut best: Option<(u64, f64)> = None;
        for (seq, seg) in inner.segments.iter() {
            if Some(*seq) == current {
                continue;
            }
            if seg.total_bytes == 0 {
                continue;
            }
            let ratio = seg.live_bytes as f64 / seg.total_bytes as f64;
            if ratio < threshold
                && best.as_ref().is_none_or(|(_, best_ratio)| ratio < *best_ratio)
            {
                best = Some((*seq, ratio));
            }
        }
        best.map(|(seq, _)| seq)
    }

    /// Compact a single segment: walk every live body still pointing at it,
    /// copy each into the current write segment, atomically swap the index
    /// entry, and unlink the old file. Returns the number of bodies migrated.
    ///
    /// Concurrency model: each per-body migration takes the inner lock for
    /// the index swap, with a stale-entry guard — if a delete or another
    /// compactor already moved the body, we skip and treat the freshly
    /// written copy as garbage that the next compaction will reclaim. A
    /// no-op for a non-existent or current segment.
    ///
    /// Corruption handling: when a body's on-disk CRC fails to verify, we
    /// log loudly, drop the entry from the index, and continue — one
    /// bit-rotted body must not block reclamation of an entire segment.
    /// The dropped `BodyId` becomes unreadable (`reserve`/`peek` will
    /// surface `INTERNAL_ERROR` to the client). Counted in
    /// [`BodyStore::bodies_dropped_corrupted`].
    pub fn compact_segment(&self, seq: u64) -> Result<u64, BodyStoreError> {
        // Snapshot live bodies in this segment under the lock. The list is
        // immutable from here on; concurrent puts/deletes touch the index,
        // not this snapshot.
        let (seg_file, body_ids, segment_total): (Arc<File>, Vec<(BodyId, BodyLocation)>, u64) = {
            let inner = self.inner.lock().unwrap();
            // Refuse to compact the current write segment — it can still
            // accept appends, and unlinking it would lose data.
            if Some(seq) == inner.current_seq {
                return Ok(0);
            }
            let seg = match inner.segments.get(&seq) {
                Some(s) => s,
                None => return Ok(0),
            };
            let file = Arc::clone(&seg.file);
            let total = seg.total_bytes;
            let bodies: Vec<(BodyId, BodyLocation)> = inner
                .index
                .iter()
                .filter(|(_, loc)| loc.seq == seq)
                .map(|(id, loc)| (*id, *loc))
                .collect();
            (file, bodies, total)
        };

        let mut migrated = 0u64;
        // Bit-rotted bodies discovered during this compaction. We collect
        // and delete them in one batch at the end of the loop instead of
        // calling self.delete() per body — a single delete_many takes the
        // inner mutex once instead of once per corrupt body.
        let mut corrupted: Vec<BodyId> = Vec::new();
        for (body_id, old_loc) in body_ids {
            // Read body + its existing record header from the old segment
            // outside the lock. Pulling the header lets us reuse the
            // original CRC instead of re-hashing the body, and validates
            // the move source against silent disk corruption.
            let mut header = [0u8; BODY_HEADER_SIZE];
            seg_file.read_exact_at(&mut header, old_loc.offset - BODY_HEADER_SIZE as u64)?;
            let expected_crc =
                u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
            let mut buf = vec![0u8; old_loc.len as usize];
            seg_file.read_exact_at(&mut buf, old_loc.offset)?;
            let found_crc = crc32fast::hash(&buf);
            if found_crc != expected_crc {
                // Bit-rot on disk. Drop this body from the index and keep
                // going — failing the whole compaction would pin the
                // entire segment indefinitely. Future reads of this
                // BodyId will hit NotFound (surfaced as INTERNAL_ERROR).
                tracing::error!(
                    body_id = body_id.0,
                    seq = old_loc.seq,
                    offset = old_loc.offset,
                    expected_crc = format!("{:08x}", expected_crc),
                    found_crc = format!("{:08x}", found_crc),
                    "TOAST corruption: dropping body during compaction",
                );
                corrupted.push(body_id);
                continue;
            }

            if self.migrate_body(body_id, &buf, expected_crc, old_loc)? {
                migrated += 1;
            }
        }
        if !corrupted.is_empty() {
            self.bodies_dropped_corrupted
                .fetch_add(corrupted.len() as u64, Ordering::Relaxed);
            self.delete_many(&corrupted);
        }

        // Force migrated bytes to disk before unlinking the old segment.
        // Without this, a crash after `remove_file` but before the kernel
        // writes the new segment through would lose every body we just
        // migrated. Rotation mid-migration already fsyncs sealed
        // segments (`Inner::rotate`), so only the current write segment
        // needs syncing here. Skip the syscall when nothing migrated.
        if migrated > 0 {
            self.fsync()?;
        }

        // Tear down the old segment: drop from map, account, unlink. Any
        // outstanding read holding `Arc<File>` keeps its FD valid through
        // the unlink (POSIX semantics) — that's what makes this safe.
        let path = {
            let mut inner = self.inner.lock().unwrap();
            inner.segments.remove(&seq);
            segment_path(&self.dir, seq)
        };
        self.total_bytes
            .fetch_sub(segment_total, Ordering::Relaxed);
        self.segment_count.fetch_sub(1, Ordering::Relaxed);
        let _ = std::fs::remove_file(&path);

        self.compactions_total.fetch_add(1, Ordering::Relaxed);
        self.bodies_migrated_total
            .fetch_add(migrated, Ordering::Relaxed);

        Ok(migrated)
    }

    /// Append a single migrated body to the current segment and atomically
    /// flip the index entry. Returns `Ok(true)` if migrated, `Ok(false)` if
    /// the index no longer points at `expected_old` (deleted or already
    /// migrated by a concurrent path). The caller passes the CRC from the
    /// original record so we don't re-hash bytes the read just verified.
    fn migrate_body(
        &self,
        body_id: BodyId,
        bytes: &[u8],
        crc: u32,
        expected_old: BodyLocation,
    ) -> Result<bool, BodyStoreError> {
        let mut inner = self.inner.lock().unwrap();

        // Stale-entry guard: only proceed if the index still points at the
        // exact (seq, offset) we just read from. Anything else means a
        // delete or a concurrent migration won the race.
        match inner.index.get(&body_id) {
            Some(loc) if loc.seq == expected_old.seq && loc.offset == expected_old.offset => {}
            _ => return Ok(false),
        }

        let new_location = self.append_body_locked(&mut inner, body_id, bytes, crc)?;
        inner.index.insert(body_id, new_location);

        // Live bytes are conserved across the move — the global atomic is
        // untouched. The old segment's per-segment live_bytes drops; the
        // bytes still occupy disk until the segment is unlinked.
        if let Some(old_seg) = inner.segments.get_mut(&expected_old.seq) {
            old_seg.live_bytes = old_seg.live_bytes.saturating_sub(bytes.len() as u64);
        }

        Ok(true)
    }
}

impl Inner {
    fn rotate(&mut self, dir: &Path) -> Result<(), BodyStoreError> {
        // Seal the previous segment durably before flipping current_seq —
        // otherwise its tail bytes may not survive a crash.
        if let Some(prev_seq) = self.current_seq
            && let Some(prev) = self.segments.get(&prev_seq)
        {
            prev.file.sync_data()?;
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        let path = segment_path(dir, seq);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        write_file_header(&mut file)?;
        file.sync_data()?;

        self.segments.insert(
            seq,
            Segment {
                file: Arc::new(file),
                total_bytes: FILE_HEADER_SIZE as u64,
                live_bytes: 0,
            },
        );
        self.current_seq = Some(seq);
        Ok(())
    }
}

// --- File-level helpers ---

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{}{:06}", TOAST_FILE_PREFIX, seq))
}

fn encode_body_header(body_id: BodyId, len: u32, crc: u32) -> [u8; BODY_HEADER_SIZE] {
    let mut h = [0u8; BODY_HEADER_SIZE];
    h[0..8].copy_from_slice(&body_id.0.to_le_bytes());
    h[8..12].copy_from_slice(&len.to_le_bytes());
    h[12..16].copy_from_slice(&crc.to_le_bytes());
    h
}

fn decode_body_header(h: &[u8; BODY_HEADER_SIZE]) -> (BodyId, u32) {
    let body_id = BodyId(u64::from_le_bytes([
        h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7],
    ]));
    let len = u32::from_le_bytes([h[8], h[9], h[10], h[11]]);
    (body_id, len)
}

fn write_file_header(file: &mut File) -> io::Result<()> {
    use std::io::Write;
    let mut buf = [0u8; FILE_HEADER_SIZE];
    buf[0..4].copy_from_slice(TOAST_MAGIC);
    buf[4..8].copy_from_slice(&TOAST_VERSION.to_le_bytes());
    file.write_all(&buf)?;
    Ok(())
}

struct ScanEntry {
    body_id: BodyId,
    body_offset: u64,
    len: u32,
}

struct SegmentScan {
    entries: Vec<ScanEntry>,
    /// Bytes consumed by the file header plus all complete body records.
    /// A truncated tail is treated as not-present and the segment will be
    /// re-appended past `consumed` on next write.
    consumed: u64,
}

fn scan_segment(file: &mut File) -> Result<SegmentScan, BodyStoreError> {
    file.seek(SeekFrom::Start(0))?;
    let file_len = file.metadata()?.len();

    if file_len < FILE_HEADER_SIZE as u64 {
        // Empty or truncated file header — treat as fresh segment, but a
        // missing header on a non-empty file is genuinely corrupt.
        if file_len == 0 {
            return Ok(SegmentScan { entries: Vec::new(), consumed: 0 });
        }
        return Err(BodyStoreError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "TOAST segment file shorter than file header",
        )));
    }

    let mut hbuf = [0u8; FILE_HEADER_SIZE];
    file.read_exact(&mut hbuf)?;
    if &hbuf[0..4] != TOAST_MAGIC {
        return Err(BodyStoreError::BadMagic);
    }
    let version = u32::from_le_bytes([hbuf[4], hbuf[5], hbuf[6], hbuf[7]]);
    if version != TOAST_VERSION {
        return Err(BodyStoreError::BadVersion(version));
    }

    let mut entries = Vec::new();
    let mut offset = FILE_HEADER_SIZE as u64;

    while offset + BODY_HEADER_SIZE as u64 <= file_len {
        let mut bh = [0u8; BODY_HEADER_SIZE];
        file.read_exact_at(&mut bh, offset)?;
        let (body_id, len) = decode_body_header(&bh);
        let body_offset = offset + BODY_HEADER_SIZE as u64;
        let next_offset = body_offset + len as u64;

        if next_offset > file_len {
            // Truncated tail. Stop here; the segment will be re-appended at
            // `offset` on the next write. (CRC verification of the body
            // happens at read time, not scan time, so we don't read body
            // bytes here.)
            break;
        }

        entries.push(ScanEntry { body_id, body_offset, len });
        offset = next_offset;
    }

    Ok(SegmentScan { entries, consumed: offset })
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open(dir: &Path) -> BodyStore {
        BodyStore::open(dir, 1024 * 1024).expect("open body store")
    }

    /// Deterministic xorshift PRNG, in tests only. Avoids pulling in `rand`
    /// for a few lines of fuzz-coverage.
    struct Xorshift(u64);
    impl Xorshift {
        fn new(seed: u64) -> Self {
            Self(seed.max(1))
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn gen_range(&mut self, lo: usize, hi_inclusive: usize) -> usize {
            lo + (self.next_u64() as usize % (hi_inclusive - lo + 1))
        }
        fn fill(&mut self, buf: &mut [u8]) {
            for b in buf.iter_mut() {
                *b = (self.next_u64() & 0xFF) as u8;
            }
        }
    }

    #[test]
    fn write_then_read_round_trip() {
        let tmp = TempDir::new().unwrap();
        let bs = open(tmp.path());
        let id = bs.write_body(b"hello world").unwrap();
        assert_eq!(bs.read_body(id).unwrap(), b"hello world");
    }

    #[test]
    fn many_random_bodies_round_trip() {
        let tmp = TempDir::new().unwrap();
        // Small segment size to force several rotations.
        let bs = BodyStore::open(tmp.path(), 32 * 1024).unwrap();
        let mut rng = Xorshift::new(42);

        let mut written: Vec<(BodyId, Vec<u8>)> = Vec::new();
        for _ in 0..1000 {
            let len = rng.gen_range(1, 512);
            let mut body = vec![0u8; len];
            rng.fill(&mut body);
            let id = bs.write_body(&body).unwrap();
            written.push((id, body));
        }

        // Multiple segments must have been created.
        assert!(bs.segment_count() > 1);

        for (id, expected) in &written {
            assert_eq!(&bs.read_body(*id).unwrap(), expected);
        }
    }

    #[test]
    fn restart_rebuilds_index_from_headers() {
        let tmp = TempDir::new().unwrap();
        let mut written: Vec<(BodyId, Vec<u8>)> = Vec::new();
        {
            let bs = BodyStore::open(tmp.path(), 8 * 1024).unwrap();
            let mut rng = Xorshift::new(1);
            for _ in 0..50 {
                let len = rng.gen_range(64, 256);
                let mut body = vec![0u8; len];
                rng.fill(&mut body);
                let id = bs.write_body(&body).unwrap();
                written.push((id, body));
            }
            bs.fsync().unwrap();
        }

        // Reopen — index is reconstructed by scanning segment headers.
        let bs2 = BodyStore::open(tmp.path(), 8 * 1024).unwrap();
        for (id, expected) in &written {
            assert_eq!(&bs2.read_body(*id).unwrap(), expected);
        }

        // New writes get fresh, monotonically-greater ids.
        let next_id = bs2.write_body(b"after-restart").unwrap();
        assert!(next_id.0 > written.last().unwrap().0.0);
    }

    #[test]
    fn delete_decrements_live_bytes() {
        let tmp = TempDir::new().unwrap();
        let bs = open(tmp.path());
        let id1 = bs.write_body(&[0u8; 100]).unwrap();
        let id2 = bs.write_body(&[0u8; 200]).unwrap();
        assert_eq!(bs.live_bytes(), 300);

        bs.delete(id1);
        assert_eq!(bs.live_bytes(), 200);

        // The bytes are still on disk (no compaction yet), but the id is gone.
        match bs.read_body(id1) {
            Err(BodyStoreError::NotFound(_)) => {}
            other => panic!("expected NotFound after delete, got {:?}", other),
        }

        // The other body is still readable.
        assert_eq!(bs.read_body(id2).unwrap(), &[0u8; 200]);
    }

    #[test]
    fn truncated_tail_is_recoverable() {
        let tmp = TempDir::new().unwrap();
        let id1;
        let id2;
        {
            let bs = open(tmp.path());
            id1 = bs.write_body(b"first").unwrap();
            id2 = bs.write_body(b"second").unwrap();
            bs.fsync().unwrap();
        }

        // Lop off the last few bytes of the most recent segment, simulating
        // a crash mid-write.
        let mut entries: Vec<_> = fs::read_dir(tmp.path()).unwrap().collect();
        entries.sort_by_key(|e| e.as_ref().unwrap().file_name());
        let last = entries.last().unwrap().as_ref().unwrap().path();
        let len = fs::metadata(&last).unwrap().len();
        let f = OpenOptions::new().write(true).open(&last).unwrap();
        f.set_len(len - 3).unwrap();

        let bs2 = open(tmp.path());
        // First body still present.
        assert_eq!(bs2.read_body(id1).unwrap(), b"first");
        // Second body got truncated and should not be present.
        assert!(matches!(bs2.read_body(id2), Err(BodyStoreError::NotFound(_))));

        // Subsequent writes succeed and land at the truncation point, not
        // past the corrupted record.
        let id3 = bs2.write_body(b"third").unwrap();
        assert_eq!(bs2.read_body(id3).unwrap(), b"third");
    }

    #[test]
    fn corrupted_body_bytes_detected_via_crc() {
        let tmp = TempDir::new().unwrap();
        let id;
        let path;
        let body_offset;
        {
            let bs = open(tmp.path());
            id = bs.write_body(b"verify-me").unwrap();
            bs.fsync().unwrap();
            let inner = bs.inner.lock().unwrap();
            let loc = inner.index.get(&id).copied().unwrap();
            body_offset = loc.offset;
            path = segment_path(&bs.dir, loc.seq);
        }

        // Flip a byte in the body region.
        let f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        let mut byte = [0u8; 1];
        f.read_exact_at(&mut byte, body_offset).unwrap();
        byte[0] ^= 0xFF;
        f.write_all_at(&byte, body_offset).unwrap();

        let bs2 = open(tmp.path());
        match bs2.read_body(id) {
            Err(BodyStoreError::BadCrc { body_id, .. }) => assert_eq!(body_id.0, id.0),
            other => panic!("expected BadCrc, got {:?}", other),
        }
    }

    #[test]
    fn segment_rotation_at_size_threshold() {
        let tmp = TempDir::new().unwrap();
        // Tiny segment: file header + one ~512-byte record fills it.
        let bs = BodyStore::open(tmp.path(), 600).unwrap();
        let body = vec![0xAB; 500];
        let id1 = bs.write_body(&body).unwrap();
        let id2 = bs.write_body(&body).unwrap();
        assert!(bs.segment_count() >= 2);
        assert_eq!(bs.read_body(id1).unwrap(), body);
        assert_eq!(bs.read_body(id2).unwrap(), body);
    }

    #[test]
    fn compaction_candidate_picks_most_wasted_sealed_segment() {
        let tmp = TempDir::new().unwrap();
        let bs = BodyStore::open(tmp.path(), 600).unwrap();

        // Each 500-byte body fills its segment (file_header + body_header + body > 600).
        let id_a = bs.write_body(&vec![0xAA; 500]).unwrap(); // segment 0
        let _id_b = bs.write_body(&vec![0xBB; 500]).unwrap(); // segment 1
        let _id_c = bs.write_body(&vec![0xCC; 500]).unwrap(); // segment 2 (current)

        // Delete the body in segment 0 — it's now 0% live.
        bs.delete(id_a);

        // Threshold of 0.5 should pick segment 0; segments 1 and 2 are 100% live
        // (segment 2 is the current write segment anyway).
        assert_eq!(bs.compaction_candidate(0.5), Some(0));
        // A high threshold (above 1.0) still won't pick the current segment.
        assert_ne!(bs.compaction_candidate(2.0), Some(2));
    }

    #[test]
    fn compaction_candidate_clamps_threshold_to_unit_range() {
        let tmp = TempDir::new().unwrap();
        let bs = BodyStore::open(tmp.path(), 600).unwrap();
        let id_a = bs.write_body(&vec![0xAA; 500]).unwrap(); // seg 0
        let _id_b = bs.write_body(&vec![0xBB; 500]).unwrap(); // seg 1 (current)
        bs.delete(id_a);

        // 5.0 collapses to 1.0 — picks seg 0 (live ratio 0.0 < 1.0).
        assert_eq!(bs.compaction_candidate(5.0), Some(0));
        // -1.0 collapses to 0.0 — no segment has a ratio below zero.
        assert_eq!(bs.compaction_candidate(-1.0), None);
    }

    #[test]
    fn compaction_candidate_returns_none_when_nothing_qualifies() {
        let tmp = TempDir::new().unwrap();
        let bs = open(tmp.path());
        let _ = bs.write_body(b"alpha").unwrap();
        let _ = bs.write_body(b"beta").unwrap();
        assert_eq!(bs.compaction_candidate(0.5), None);
    }

    #[test]
    fn compact_segment_unlinks_old_file_and_preserves_live_bodies() {
        let tmp = TempDir::new().unwrap();
        // Segment size 80 means each ~40-byte body gets its own segment.
        let bs = BodyStore::open(tmp.path(), 80).unwrap();

        let _empty = bs.write_body(&vec![0xAA; 40]).unwrap(); // seg 0
        let alive = bs.write_body(&vec![0xBB; 40]).unwrap(); // seg 1
        let _filler = bs.write_body(&vec![0xCC; 40]).unwrap(); // seg 2 (current)

        // Delete the body in segment 0 — it goes to 0% live and qualifies.
        bs.delete(_empty);

        let target = bs.compaction_candidate(0.5).expect("a candidate exists");
        assert_eq!(target, 0, "the empty sealed segment is the candidate");

        let segs_before = bs.segment_count();
        let migrated = bs.compact_segment(target).unwrap();
        assert_eq!(migrated, 0, "segment was empty — nothing to migrate");
        assert_eq!(bs.segment_count(), segs_before - 1);

        // Segment file is gone from disk.
        let path = tmp.path().join(format!("body.{:06}", target));
        assert!(!path.exists(), "compacted segment file should be unlinked");

        // Bodies in other segments still readable.
        assert_eq!(bs.read_body(alive).unwrap(), vec![0xBB; 40]);
    }

    #[test]
    fn compact_segment_migrates_live_bodies_into_current_segment() {
        let tmp = TempDir::new().unwrap();
        // Segment ~120 bytes: file_header(16) + body_header(20) + body(64) = 100,
        // next body would push it past 120 → rotation.
        let bs = BodyStore::open(tmp.path(), 120).unwrap();

        // Fill three segments with one body each.
        let id0 = bs.write_body(&vec![1u8; 64]).unwrap(); // seg 0
        let id1 = bs.write_body(&vec![2u8; 64]).unwrap(); // seg 1
        let _id2 = bs.write_body(&vec![3u8; 64]).unwrap(); // seg 2 (current)
        assert_eq!(bs.segment_count(), 3);

        // Compacting segment 0 (1 live body) migrates it. With segment size 120,
        // a fresh segment must be allocated to hold the migrated body.
        let migrated = bs.compact_segment(0).unwrap();
        assert_eq!(migrated, 1);

        // Segment 0's file is gone; segment 1 untouched; a new segment ≥ 3
        // exists holding the migrated body.
        let p0 = tmp.path().join("body.000000");
        let p1 = tmp.path().join("body.000001");
        assert!(!p0.exists(), "segment 0 must be unlinked");
        assert!(p1.exists(), "segment 1 must remain");

        // The migrated body is still readable via its original BodyId.
        assert_eq!(bs.read_body(id0).unwrap(), vec![1u8; 64]);
        // Other bodies untouched.
        assert_eq!(bs.read_body(id1).unwrap(), vec![2u8; 64]);
    }

    #[test]
    fn compact_segment_respects_concurrent_delete() {
        // Stale-entry guard: if a body gets deleted between snapshot and
        // migration, the index update is skipped and the body is not
        // resurrected.
        let tmp = TempDir::new().unwrap();
        let bs = BodyStore::open(tmp.path(), 200).unwrap();

        let id_a = bs.write_body(&vec![1u8; 40]).unwrap(); // seg 0
        let _id_b = bs.write_body(&vec![2u8; 40]).unwrap(); // seg 1 (current)

        // Delete `id_a` before compaction starts, then run compaction.
        // The pre-snapshot index lookup finds nothing; nothing migrates.
        bs.delete(id_a);
        let migrated = bs.compact_segment(0).unwrap();
        assert_eq!(migrated, 0, "deleted body must not be migrated");
        assert!(matches!(bs.read_body(id_a), Err(BodyStoreError::NotFound(_))));
    }

    #[test]
    fn compact_current_segment_is_a_noop() {
        let tmp = TempDir::new().unwrap();
        let bs = open(tmp.path());
        let id = bs.write_body(b"alive").unwrap();
        let inner = bs.inner.lock().unwrap();
        let current = inner.current_seq.unwrap();
        drop(inner);
        assert_eq!(bs.compact_segment(current).unwrap(), 0);
        assert_eq!(bs.read_body(id).unwrap(), b"alive");
    }

    #[test]
    fn compact_segment_skips_corrupted_bodies_and_continues() {
        // One bit-rotted body must not block reclamation of the rest of
        // the segment. The corrupted id becomes unreadable; healthy
        // bodies in the same segment migrate normally; the counter is
        // bumped exactly once per dropped body.
        let tmp = TempDir::new().unwrap();
        // 140-byte segment: header(16) + 2×(body_header(20) + body(40)) = 136
        // fits two 40-byte bodies in seg 0; third (136 + 60 = 196 > 140)
        // forces rotation into seg 1, which becomes the current segment.
        let bs = BodyStore::open(tmp.path(), 140).unwrap();

        let id_corrupt = bs.write_body(&vec![0xAA; 40]).unwrap(); // seg 0
        let id_healthy = bs.write_body(&vec![0xBB; 40]).unwrap(); // seg 0
        let _id_filler = bs.write_body(&vec![0xCC; 40]).unwrap(); // seg 1 (current)
        bs.fsync().unwrap();

        // Snapshot the corrupt body's location, then flip a byte in its
        // payload to invalidate the CRC.
        let (path, body_offset) = {
            let inner = bs.inner.lock().unwrap();
            let loc = inner.index.get(&id_corrupt).copied().unwrap();
            (segment_path(&bs.dir, loc.seq), loc.offset)
        };
        let f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        let mut byte = [0u8; 1];
        f.read_exact_at(&mut byte, body_offset).unwrap();
        byte[0] ^= 0xFF;
        f.write_all_at(&byte, body_offset).unwrap();
        drop(f);

        // Compact segment 0. Should skip the corrupt body, migrate the
        // healthy one, and unlink the segment.
        let migrated = bs.compact_segment(0).unwrap();
        assert_eq!(migrated, 1, "healthy body must still migrate");
        assert_eq!(
            bs.bodies_dropped_corrupted(),
            1,
            "corrupted body must be counted exactly once"
        );

        // Old segment file is gone — compaction completed despite the corruption.
        assert!(!path.exists(), "old segment must be unlinked");

        // The healthy body is still readable; the corrupt one is now NotFound.
        assert_eq!(bs.read_body(id_healthy).unwrap(), vec![0xBB; 40]);
        match bs.read_body(id_corrupt) {
            Err(BodyStoreError::NotFound(_)) => {}
            other => panic!("expected NotFound for dropped body, got {:?}", other),
        }
    }
}
