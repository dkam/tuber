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
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::job::BodyId;

// --- Constants ---

const TOAST_MAGIC: &[u8; 4] = b"TBOD";
const TOAST_VERSION: u32 = 1;
const FILE_HEADER_SIZE: usize = 16; // magic(4) + version(4) + reserved(8)
const BODY_HEADER_SIZE: usize = 20; // body_id(8) + len(4) + crc32(4) + reserved(4)
const TOAST_FILE_PREFIX: &str = "body.";

/// Default segment size (64 MiB). Operator-tunable via the eventual
/// `--toast-segment-size` flag.
pub const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

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
    /// Equal to the BTreeMap key. Stored for use by compaction in a
    /// future phase.
    #[allow(dead_code)]
    seq: u64,
    /// Filesystem path. Kept for compaction (which renames/unlinks).
    #[allow(dead_code)]
    path: PathBuf,
    file: File,
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

        for seq in &seqs {
            let path = dir.join(format!("{}{:06}", TOAST_FILE_PREFIX, seq));
            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            let scan = scan_segment(&mut file)?;
            let mut live_bytes: u64 = 0;
            for entry in scan.entries {
                if entry.body_id.0 >= max_body_id {
                    max_body_id = entry.body_id.0 + 1;
                }
                live_bytes += entry.len as u64;
                index.insert(
                    entry.body_id,
                    BodyLocation { seq: *seq, offset: entry.body_offset, len: entry.len },
                );
            }
            segments.insert(
                *seq,
                Segment { seq: *seq, path, file, total_bytes: scan.consumed, live_bytes },
            );
        }

        let next_seq = seqs.last().map(|s| s + 1).unwrap_or(0);
        let current_seq = seqs.last().copied();

        Ok(BodyStore {
            dir: dir.to_path_buf(),
            segment_size,
            next_body_id: AtomicU64::new(max_body_id),
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
        let mut inner = self.inner.lock().unwrap();

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
        }

        let current_seq = inner.current_seq.expect("rotation populates current_seq");
        let seg = inner.segments.get_mut(&current_seq).expect("current segment present");
        let header_offset = seg.total_bytes;
        let body_offset = header_offset + BODY_HEADER_SIZE as u64;

        let crc = crc32fast::hash(bytes);
        let mut header = [0u8; BODY_HEADER_SIZE];
        header[0..8].copy_from_slice(&body_id.0.to_le_bytes());
        header[8..12].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
        header[12..16].copy_from_slice(&crc.to_le_bytes());
        // bytes 16..20 reserved, zero

        seg.file.write_all_at(&header, header_offset)?;
        seg.file.write_all_at(bytes, body_offset)?;

        seg.total_bytes = body_offset + bytes.len() as u64;
        seg.live_bytes += bytes.len() as u64;

        inner.index.insert(
            body_id,
            BodyLocation { seq: current_seq, offset: body_offset, len: bytes.len() as u32 },
        );

        Ok(body_id)
    }

    /// Read the body bytes for `id`. Verifies the body's CRC against the
    /// header recorded at write time.
    pub fn read_body(&self, id: BodyId) -> Result<Vec<u8>, BodyStoreError> {
        let inner = self.inner.lock().unwrap();
        let location = inner
            .index
            .get(&id)
            .copied()
            .ok_or(BodyStoreError::NotFound(id))?;
        let seg = inner
            .segments
            .get(&location.seq)
            .ok_or(BodyStoreError::NotFound(id))?;

        // Header sits immediately before the body bytes.
        let mut header = [0u8; BODY_HEADER_SIZE];
        seg.file
            .read_exact_at(&mut header, location.offset - BODY_HEADER_SIZE as u64)?;
        let expected_crc = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);

        let mut buf = vec![0u8; location.len as usize];
        seg.file.read_exact_at(&mut buf, location.offset)?;

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
        if let Some(loc) = inner.index.remove(&id)
            && let Some(seg) = inner.segments.get_mut(&loc.seq)
        {
            seg.live_bytes = seg.live_bytes.saturating_sub(loc.len as u64);
        }
    }

    /// `fsync` the current segment file. Sealed segments are not re-synced —
    /// they were synced when last written.
    pub fn fsync(&self) -> Result<(), BodyStoreError> {
        let inner = self.inner.lock().unwrap();
        if let Some(seq) = inner.current_seq
            && let Some(seg) = inner.segments.get(&seq)
        {
            seg.file.sync_data()?;
        }
        Ok(())
    }

    /// Sum of bytes used across all segment files (file headers + body
    /// records). Drives the disk-budget calculation.
    pub fn total_bytes(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.segments.values().map(|s| s.total_bytes).sum()
    }

    /// Sum of body bytes still referenced by live ids. The ratio
    /// `live_bytes / total_bytes` per segment drives compaction.
    pub fn live_bytes(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.segments.values().map(|s| s.live_bytes).sum()
    }

    /// Number of segments currently on disk.
    pub fn segment_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.segments.len()
    }
}

impl Inner {
    fn rotate(&mut self, dir: &Path) -> Result<(), BodyStoreError> {
        let seq = self.next_seq;
        self.next_seq += 1;
        let path = dir.join(format!("{}{:06}", TOAST_FILE_PREFIX, seq));
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
                seq,
                path,
                file,
                total_bytes: FILE_HEADER_SIZE as u64,
                live_bytes: 0,
            },
        );
        self.current_seq = Some(seq);
        Ok(())
    }
}

// --- File-level helpers ---

fn write_file_header(file: &mut File) -> io::Result<()> {
    use std::io::Write;
    let mut buf = [0u8; FILE_HEADER_SIZE];
    buf[0..4].copy_from_slice(TOAST_MAGIC);
    buf[4..8].copy_from_slice(&TOAST_VERSION.to_le_bytes());
    // bytes 8..16 reserved, zero
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

        let body_id = BodyId(u64::from_le_bytes([
            bh[0], bh[1], bh[2], bh[3], bh[4], bh[5], bh[6], bh[7],
        ]));
        let len = u32::from_le_bytes([bh[8], bh[9], bh[10], bh[11]]);
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
    struct Lcg(u64);
    impl Lcg {
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
        let mut rng = Lcg::new(42);

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
            let mut rng = Lcg::new(1);
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
            path = inner.segments.get(&loc.seq).unwrap().path.clone();
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
}
