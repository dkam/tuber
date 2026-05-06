// Write-ahead log for persistence.
//
// When enabled via `-b <dir>`, all job mutations are logged to append-only files.
// On restart, the WAL is replayed to restore state.

use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::job::{Job, JobState};
#[cfg(test)]
use crate::job::BodyRef;

// --- Constants ---

const WAL_MAGIC: &[u8; 4] = b"TWAL";
const WAL_VERSION: u32 = 4;
const WAL_VERSION_MIN: u32 = 3; // Oldest version we can still read
const HEADER_SIZE: usize = 12; // magic(4) + version(4) + flags(4)
const RECORD_TYPE_FULL_JOB: u8 = 0x01;
const RECORD_TYPE_STATE_CHANGE: u8 = 0x02;
const STATE_CHANGE_PAYLOAD_LEN_V3: u32 = 21;
const STATE_CHANGE_PAYLOAD_LEN: u32 = 22; // v4: added reason byte
/// Size of a state change record: type(1) + job_id(8) + payload_len(4) + payload(22) + crc(4)
const STATE_CHANGE_RECORD_SIZE: usize = 1 + 8 + 4 + 22 + 4;
const DEFAULT_MAX_FILE_SIZE: usize = 10 * 1024 * 1024; // 10 MB
const FILE_PREFIX: &str = "binlog.";
/// Userland write buffer capacity per WAL file. Amortises syscall overhead.
/// Durability is not affected: every path that calls `sync_all` first calls `flush`.
const BUF_CAPACITY: usize = 64 * 1024;
/// Default interval between fsyncs. `Duration::ZERO` means fsync on every write.
pub const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(100);

// --- State encoding ---

fn state_to_u8(state: JobState) -> u8 {
    match state {
        JobState::Ready => 0,
        JobState::Reserved => 1,
        JobState::Delayed => 2,
        JobState::Buried => 3,
    }
}

const STATE_DELETED: u8 = 0xFF;

// --- State change reason encoding ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateChangeReason {
    None = 0,
    Reserve = 1,
    Release = 2,
    Bury = 3,
    Kick = 4,
    Timeout = 5,
}

fn reason_to_u8(r: StateChangeReason) -> u8 {
    r as u8
}

fn u8_to_reason(v: u8) -> StateChangeReason {
    match v {
        1 => StateChangeReason::Reserve,
        2 => StateChangeReason::Release,
        3 => StateChangeReason::Bury,
        4 => StateChangeReason::Kick,
        5 => StateChangeReason::Timeout,
        _ => StateChangeReason::None,
    }
}

fn u8_to_state(v: u8) -> Option<JobState> {
    match v {
        0 => Some(JobState::Ready),
        1 => Some(JobState::Reserved),
        2 => Some(JobState::Delayed),
        3 => Some(JobState::Buried),
        _ => None,
    }
}

// --- Serialization helpers ---

fn write_option_string(buf: &mut Vec<u8>, s: &Option<String>) {
    match s {
        None => buf.extend_from_slice(&0u16.to_le_bytes()),
        Some(s) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
    }
}

fn read_option_string(data: &[u8], offset: &mut usize) -> Result<Option<String>, WalError> {
    if *offset + 2 > data.len() {
        return Err(WalError::Truncated);
    }
    let len = u16::from_le_bytes([data[*offset], data[*offset + 1]]) as usize;
    *offset += 2;
    if len == 0 {
        return Ok(None);
    }
    if *offset + len > data.len() {
        return Err(WalError::Truncated);
    }
    let s = String::from_utf8(data[*offset..*offset + len].to_vec())
        .map_err(|_| WalError::InvalidData)?;
    *offset += len;
    Ok(Some(s))
}

// --- Error type ---

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    BadMagic,
    BadVersion(u32),
    BadCrc,
    Truncated,
    InvalidData,
    UnknownRecordType(u8),
}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        WalError::Io(e)
    }
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "WAL I/O error: {}", e),
            WalError::BadMagic => write!(f, "WAL bad magic"),
            WalError::BadVersion(v) => write!(f, "WAL bad version: {}", v),
            WalError::BadCrc => write!(f, "WAL CRC mismatch"),
            WalError::Truncated => write!(f, "WAL record truncated"),
            WalError::InvalidData => write!(f, "WAL invalid data"),
            WalError::UnknownRecordType(t) => write!(f, "WAL unknown record type: {}", t),
        }
    }
}

// --- WAL record types ---

/// An idempotency tombstone recovered from WAL replay.
#[derive(Debug)]
pub struct IdpTombstone {
    pub tube_name: String,
    pub key: String,
    pub job_id: u64,
    pub expires_at: SystemTime,
}

#[derive(Debug)]
pub enum WalRecord {
    FullJob(Box<Job>),
    StateChange {
        job_id: u64,
        new_state: Option<JobState>, // None = Deleted
        new_priority: u32,
        new_delay_nanos: u64,
        expiry_epoch_secs: u64, // For idempotency tombstones (0 = no tombstone)
        reason: StateChangeReason,
    },
}

// --- Serialization ---

pub fn serialize_full_job(job: &Job) -> Vec<u8> {
    let mut payload = Vec::new();

    // priority, delay_nanos, ttr_nanos, created_at_epoch_secs
    payload.extend_from_slice(&job.priority.to_le_bytes());
    payload.extend_from_slice(&job.delay.as_nanos().min(u64::MAX as u128).to_le_bytes()[..8]);
    payload.extend_from_slice(&job.ttr.as_nanos().min(u64::MAX as u128).to_le_bytes()[..8]);
    payload.extend_from_slice(&job.created_at_epoch.to_le_bytes());

    // state
    payload.push(state_to_u8(job.state));

    // counters
    payload.extend_from_slice(&job.reserve_ct.to_le_bytes());
    payload.extend_from_slice(&job.timeout_ct.to_le_bytes());
    payload.extend_from_slice(&job.release_ct.to_le_bytes());
    payload.extend_from_slice(&job.bury_ct.to_le_bytes());
    payload.extend_from_slice(&job.kick_ct.to_le_bytes());

    // tube_name
    let tn = job.tube_name.as_bytes();
    payload.extend_from_slice(&(tn.len() as u16).to_le_bytes());
    payload.extend_from_slice(tn);

    // extension fields (grouped: key + its associated value)
    // idempotency_key + ttl
    write_option_string(
        &mut payload,
        &job.idempotency_key.as_ref().map(|(k, _)| k.clone()),
    );
    payload.extend_from_slice(
        &job.idempotency_key
            .as_ref()
            .map_or(0u32, |(_, ttl)| *ttl)
            .to_le_bytes(),
    );

    // group, after_group
    write_option_string(&mut payload, &job.group);
    write_option_string(&mut payload, &job.after_group);

    // concurrency_key + limit
    write_option_string(
        &mut payload,
        &job.concurrency_key.as_ref().map(|(k, _)| k.clone()),
    );
    payload.extend_from_slice(
        &job.concurrency_key
            .as_ref()
            .map_or(0u32, |(_, l)| *l)
            .to_le_bytes(),
    );

    // body (last — largest variable-length field)
    let body_bytes = job.body_bytes();
    payload.extend_from_slice(&(body_bytes.len() as u32).to_le_bytes());
    payload.extend_from_slice(body_bytes);

    // Build full record: type + job_id + payload_len + payload + crc
    let mut record = Vec::with_capacity(1 + 8 + 4 + payload.len() + 4);
    record.push(RECORD_TYPE_FULL_JOB);
    record.extend_from_slice(&job.id.to_le_bytes());
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&payload);

    let crc = crc32fast::hash(&record);
    record.extend_from_slice(&crc.to_le_bytes());

    record
}

pub fn serialize_state_change(
    job_id: u64,
    state: Option<JobState>,
    priority: u32,
    delay_nanos: u64,
    expiry_epoch_secs: u64,
    reason: StateChangeReason,
) -> Vec<u8> {
    let mut record = Vec::with_capacity(STATE_CHANGE_RECORD_SIZE);
    record.push(RECORD_TYPE_STATE_CHANGE);
    record.extend_from_slice(&job_id.to_le_bytes());
    record.extend_from_slice(&STATE_CHANGE_PAYLOAD_LEN.to_le_bytes());

    // payload: state + priority + delay_nanos + expiry_epoch_secs + reason
    let state_byte = match state {
        Some(s) => state_to_u8(s),
        None => STATE_DELETED,
    };
    record.push(state_byte);
    record.extend_from_slice(&priority.to_le_bytes());
    record.extend_from_slice(&delay_nanos.to_le_bytes());
    record.extend_from_slice(&expiry_epoch_secs.to_le_bytes());
    record.push(reason_to_u8(reason));

    let crc = crc32fast::hash(&record);
    record.extend_from_slice(&crc.to_le_bytes());

    record
}

pub fn estimate_full_job_size(job: &Job) -> usize {
    // type(1) + job_id(8) + payload_len(4) + crc(4) = 17 overhead
    // payload: pri(4) + delay(8) + ttr(8) + epoch(8) + state(1) +
    //          5 counters * 4 = 20 + tube_name_len(2) + tube_name +
    //          body_len(4) + body + 4 option_strings (2 bytes each min) + concurrency_limit(4) + idp_ttl(4)
    let fixed = 17 + 4 + 8 + 8 + 8 + 1 + 20 + 2 + 4 + 8 + 4 + 4; // +4 concurrency limit +4 idp ttl
    let variable = job.tube_name.len()
        + job.body_size()
        + job.idempotency_key.as_ref().map_or(0, |(s, _)| s.len())
        + job.group.as_ref().map_or(0, |s| s.len())
        + job.after_group.as_ref().map_or(0, |s| s.len())
        + job.concurrency_key.as_ref().map_or(0, |(s, _)| s.len());
    fixed + variable
}

/// Estimate full job record size without needing a Job struct.
pub fn estimate_full_job_size_raw(
    tube_name: &str,
    body_len: usize,
    idempotency_key: &Option<String>,
    group: &Option<String>,
    after_group: &Option<String>,
    concurrency_key: &Option<(String, u32)>,
) -> usize {
    // type(1) + job_id(8) + payload_len(4) + crc(4) = 17 overhead
    // payload: pri(4) + delay(8) + ttr(8) + epoch(8) + state(1) +
    //          5 counters * 4 = 20 + tube_name_len(2) + tube_name +
    //          body_len(4) + body + 4 option_strings (2 bytes each min) + concurrency_limit(4) + idp_ttl(4)
    let fixed = 17 + 4 + 8 + 8 + 8 + 1 + 20 + 2 + 4 + 8 + 4 + 4;
    let variable = tube_name.len()
        + body_len
        + idempotency_key.as_ref().map_or(0, |s| s.len())
        + group.as_ref().map_or(0, |s| s.len())
        + after_group.as_ref().map_or(0, |s| s.len())
        + concurrency_key.as_ref().map_or(0, |(s, _)| s.len());
    fixed + variable
}

// --- Deserialization ---

/// Deserialize a single record from `data`. Returns (record, bytes_consumed).
pub fn deserialize_record(data: &[u8]) -> Result<(WalRecord, usize), WalError> {
    if data.is_empty() {
        return Err(WalError::Truncated);
    }

    let record_type = data[0];
    match record_type {
        RECORD_TYPE_FULL_JOB => deserialize_full_job(data),
        RECORD_TYPE_STATE_CHANGE => deserialize_state_change(data),
        _ => Err(WalError::UnknownRecordType(record_type)),
    }
}

fn deserialize_full_job(data: &[u8]) -> Result<(WalRecord, usize), WalError> {
    // type(1) + job_id(8) + payload_len(4) = 13 byte header minimum
    if data.len() < 13 {
        return Err(WalError::Truncated);
    }

    let job_id = u64::from_le_bytes(data[1..9].try_into().map_err(|_| WalError::Truncated)?);
    let payload_len =
        u32::from_le_bytes(data[9..13].try_into().map_err(|_| WalError::Truncated)?) as usize;

    let total_len = 1 + 8 + 4 + payload_len + 4; // +4 for CRC
    if data.len() < total_len {
        return Err(WalError::Truncated);
    }

    // Verify CRC
    let stored_crc = u32::from_le_bytes(
        data[total_len - 4..total_len]
            .try_into()
            .map_err(|_| WalError::Truncated)?,
    );
    let computed_crc = crc32fast::hash(&data[..total_len - 4]);
    if stored_crc != computed_crc {
        return Err(WalError::BadCrc);
    }

    // Parse payload
    let payload = &data[13..13 + payload_len];
    let mut off = 0;

    macro_rules! read_u32 {
        () => {{
            if off + 4 > payload.len() {
                return Err(WalError::Truncated);
            }
            let v = u32::from_le_bytes(
                payload[off..off + 4]
                    .try_into()
                    .map_err(|_| WalError::Truncated)?,
            );
            off += 4;
            v
        }};
    }
    macro_rules! read_u64 {
        () => {{
            if off + 8 > payload.len() {
                return Err(WalError::Truncated);
            }
            let v = u64::from_le_bytes(
                payload[off..off + 8]
                    .try_into()
                    .map_err(|_| WalError::Truncated)?,
            );
            off += 8;
            v
        }};
    }
    macro_rules! read_u16 {
        () => {{
            if off + 2 > payload.len() {
                return Err(WalError::Truncated);
            }
            let v = u16::from_le_bytes(
                payload[off..off + 2]
                    .try_into()
                    .map_err(|_| WalError::Truncated)?,
            );
            off += 2;
            v
        }};
    }

    let priority = read_u32!();
    let delay_nanos = read_u64!();
    let ttr_nanos = read_u64!();
    let created_at_epoch = read_u64!();

    if off >= payload.len() {
        return Err(WalError::Truncated);
    }
    let state_byte = payload[off];
    off += 1;
    let state = u8_to_state(state_byte).ok_or(WalError::InvalidData)?;

    let reserve_ct = read_u32!();
    let timeout_ct = read_u32!();
    let release_ct = read_u32!();
    let bury_ct = read_u32!();
    let kick_ct = read_u32!();

    let tube_name_len = read_u16!() as usize;
    if off + tube_name_len > payload.len() {
        return Err(WalError::Truncated);
    }
    let tube_name = String::from_utf8(payload[off..off + tube_name_len].to_vec())
        .map_err(|_| WalError::InvalidData)?;
    off += tube_name_len;

    // Extension fields (grouped: key + associated value)
    // idempotency_key + ttl
    let idempotency_key_str = read_option_string(payload, &mut off)?;
    let idempotency_ttl = read_u32!();
    let idempotency_key = idempotency_key_str.map(|k| (k, idempotency_ttl));

    // group, after_group
    let group = read_option_string(payload, &mut off)?;
    let after_group = read_option_string(payload, &mut off)?;

    // concurrency_key + limit
    let concurrency_key_str = read_option_string(payload, &mut off)?;
    let concurrency_limit = read_u32!();
    let concurrency_key = concurrency_key_str.map(|k| (k, concurrency_limit.max(1)));

    // body (last — largest variable-length field)
    let body_len = read_u32!() as usize;
    if off + body_len > payload.len() {
        return Err(WalError::Truncated);
    }
    let body = payload[off..off + body_len].to_vec();
    off += body_len;
    let _ = off;

    let delay = Duration::from_nanos(delay_nanos);
    let ttr = Duration::from_nanos(ttr_nanos);
    let now = Instant::now();

    let (replay_state, deadline_at) = match state {
        // Reserved jobs replay as Ready
        JobState::Reserved => (JobState::Ready, None),
        JobState::Delayed => (JobState::Delayed, Some(now + delay)),
        _ => (state, None),
    };

    let mut job = Job::new(job_id, priority, Duration::ZERO, ttr, body, tube_name);
    job.delay = delay;
    job.state = replay_state;
    job.deadline_at = deadline_at;
    job.created_at_epoch = created_at_epoch;
    job.reserve_ct = reserve_ct;
    job.timeout_ct = timeout_ct;
    job.release_ct = release_ct;
    job.bury_ct = bury_ct;
    job.kick_ct = kick_ct;
    job.idempotency_key = idempotency_key;
    job.group = group;
    job.after_group = after_group;
    job.concurrency_key = concurrency_key;
    job.reserver_id = None;

    Ok((WalRecord::FullJob(Box::new(job)), total_len))
}

fn deserialize_state_change(data: &[u8]) -> Result<(WalRecord, usize), WalError> {
    // Determine record size from payload_len to support both v3 (21) and v4 (22)
    if data.len() < 13 {
        return Err(WalError::Truncated);
    }
    let payload_len = u32::from_le_bytes(data[9..13].try_into().map_err(|_| WalError::Truncated)?);
    let record_size = (1 + 8 + 4 + payload_len + 4) as usize; // type + id + len + payload + crc

    if data.len() < record_size {
        return Err(WalError::Truncated);
    }
    if payload_len != STATE_CHANGE_PAYLOAD_LEN_V3 && payload_len != STATE_CHANGE_PAYLOAD_LEN {
        return Err(WalError::InvalidData);
    }

    let job_id = u64::from_le_bytes(data[1..9].try_into().map_err(|_| WalError::Truncated)?);

    // Verify CRC
    let stored_crc = u32::from_le_bytes(
        data[record_size - 4..record_size]
            .try_into()
            .map_err(|_| WalError::Truncated)?,
    );
    let computed_crc = crc32fast::hash(&data[..record_size - 4]);
    if stored_crc != computed_crc {
        return Err(WalError::BadCrc);
    }

    let state_byte = data[13];
    let new_state = if state_byte == STATE_DELETED {
        None
    } else {
        Some(u8_to_state(state_byte).ok_or(WalError::InvalidData)?)
    };
    let new_priority =
        u32::from_le_bytes(data[14..18].try_into().map_err(|_| WalError::Truncated)?);
    let new_delay_nanos =
        u64::from_le_bytes(data[18..26].try_into().map_err(|_| WalError::Truncated)?);
    let expiry_epoch_secs =
        u64::from_le_bytes(data[26..34].try_into().map_err(|_| WalError::Truncated)?);

    let reason = if payload_len >= STATE_CHANGE_PAYLOAD_LEN {
        u8_to_reason(data[34])
    } else {
        StateChangeReason::None
    };

    Ok((
        WalRecord::StateChange {
            job_id,
            new_state,
            new_priority,
            new_delay_nanos,
            expiry_epoch_secs,
            reason,
        },
        record_size,
    ))
}

fn write_header(w: &mut impl Write) -> io::Result<()> {
    w.write_all(WAL_MAGIC)?;
    w.write_all(&WAL_VERSION.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // reserved flags
    Ok(())
}

fn read_header(data: &[u8]) -> Result<u32, WalError> {
    if data.len() < HEADER_SIZE {
        return Err(WalError::Truncated);
    }
    if &data[0..4] != WAL_MAGIC {
        return Err(WalError::BadMagic);
    }
    let version = u32::from_le_bytes(data[4..8].try_into().map_err(|_| WalError::Truncated)?);
    if !(WAL_VERSION_MIN..=WAL_VERSION).contains(&version) {
        return Err(WalError::BadVersion(version));
    }
    let flags = u32::from_le_bytes(data[8..12].try_into().map_err(|_| WalError::Truncated)?);
    Ok(flags)
}

// --- WAL file management ---

struct WalFile {
    seq: u64,
    path: PathBuf,
    fd: Option<BufWriter<File>>,
    refs: u64,
    bytes_written: usize,
}

/// Configuration for [`Wal::open`].
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Maximum size of a single segment file before rotation. `None` uses [`DEFAULT_MAX_FILE_SIZE`].
    pub max_file_size: Option<usize>,
    /// Minimum interval between fsyncs. `Duration::ZERO` fsyncs on every write
    /// (blocking the caller until durable). Positive values bound how much committed
    /// state can be lost on crash.
    pub sync_interval: Duration,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            max_file_size: None,
            sync_interval: DEFAULT_SYNC_INTERVAL,
        }
    }
}

impl WalConfig {
    /// Convenience for callers that only need to override the file size.
    pub fn with_max_file_size(max: usize) -> Self {
        WalConfig {
            max_file_size: Some(max),
            sync_interval: DEFAULT_SYNC_INTERVAL,
        }
    }
}

pub struct Wal {
    dir: PathBuf,
    max_file_size: usize,
    sync_interval: Duration,
    last_sync_at: Instant,
    files: VecDeque<WalFile>,
    next_seq: u64,
    reserved_bytes: u64,
    alive_bytes: u64,
    records_migrated: u64,
    #[allow(dead_code)] // held for flock side effect
    lock_fd: Option<File>,
    #[cfg(test)]
    sync_count: u64,
}

impl Wal {
    /// Sequence number of the oldest (first) WAL file, or 0 if none.
    pub fn oldest_seq(&self) -> u64 {
        self.files.front().map(|f| f.seq).unwrap_or(0)
    }

    /// Sequence number of the current (last) writable WAL file, or 0 if none.
    pub fn current_seq(&self) -> u64 {
        self.files.back().map(|f| f.seq).unwrap_or(0)
    }

    /// Maximum file size for WAL files.
    pub fn max_file_size(&self) -> usize {
        self.max_file_size
    }

    /// Number of WAL files currently on disk.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Total bytes written across all WAL files.
    pub fn total_disk_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.bytes_written as u64).sum()
    }

    /// Number of job records migrated during compaction.
    pub fn records_migrated(&self) -> u64 {
        self.records_migrated
    }

    /// Increment the compaction migration counter.
    pub fn record_migration(&mut self) {
        self.records_migrated += 1;
    }

    /// Returns the oldest file's seq and how many jobs to migrate, if compaction is needed.
    ///
    /// Uses the beanstalkd waste-ratio strategy: ratio = waste / live.
    /// When ratio >= 2 (i.e. 2/3+ of WAL space is dead), migrate `ratio` jobs per tick
    /// from the oldest file. Self-regulating: more waste = more jobs moved per tick.
    pub fn compaction_target(&self) -> Option<(u64, usize)> {
        if self.files.len() <= 1 {
            return None;
        }

        let live_bytes = self.alive_bytes + self.reserved_bytes;
        if live_bytes == 0 {
            return None;
        }

        let total_space = self.files.len() as u64 * self.max_file_size as u64;
        let waste = total_space.saturating_sub(live_bytes);
        let ratio = waste / live_bytes;

        if ratio < 2 {
            return None;
        }

        let oldest_seq = self.files.front().unwrap().seq;
        Some((oldest_seq, ratio as usize))
    }

    pub fn open(dir: &Path, config: WalConfig) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        let lock_fd = Self::acquire_lock(dir)?;

        let mut wal = Wal {
            dir: dir.to_path_buf(),
            max_file_size: config.max_file_size.unwrap_or(DEFAULT_MAX_FILE_SIZE),
            sync_interval: config.sync_interval,
            last_sync_at: Instant::now(),
            files: VecDeque::new(),
            next_seq: 1,
            reserved_bytes: 0,
            alive_bytes: 0,
            records_migrated: 0,
            lock_fd: Some(lock_fd),
            #[cfg(test)]
            sync_count: 0,
        };

        wal.scan_dir()?;

        Ok(wal)
    }

    /// Returns the configured fsync interval. `Duration::ZERO` means per-write.
    pub fn sync_interval(&self) -> Duration {
        self.sync_interval
    }

    fn acquire_lock(dir: &Path) -> io::Result<File> {
        let lock_path = dir.join("lock");
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;

        // Use flock on unix
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let ret = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if ret != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "WAL directory is locked by another process",
                ));
            }
        }

        Ok(f)
    }

    fn scan_dir(&mut self) -> io::Result<()> {
        let mut seqs: Vec<u64> = Vec::new();

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(seq_str) = name.strip_prefix(FILE_PREFIX)
                && let Ok(seq) = seq_str.parse::<u64>()
            {
                seqs.push(seq);
            }
        }

        seqs.sort();

        for seq in seqs {
            let path = self.dir.join(format!("{}{:06}", FILE_PREFIX, seq));
            let meta = fs::metadata(&path)?;
            self.files.push_back(WalFile {
                seq,
                path,
                fd: None,
                refs: 0,
                bytes_written: meta.len() as usize,
            });
            if seq >= self.next_seq {
                self.next_seq = seq + 1;
            }
        }

        Ok(())
    }

    fn file_path(&self, seq: u64) -> PathBuf {
        self.dir.join(format!("{}{:06}", FILE_PREFIX, seq))
    }

    fn create_next_file(&mut self) -> io::Result<()> {
        let seq = self.next_seq;
        self.next_seq += 1;
        let path = self.file_path(seq);

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        let mut fd = BufWriter::with_capacity(BUF_CAPACITY, file);
        write_header(&mut fd)?;

        self.files.push_back(WalFile {
            seq,
            path,
            fd: Some(fd),
            refs: 0,
            bytes_written: HEADER_SIZE,
        });

        Ok(())
    }

    fn current_file_mut(&mut self) -> io::Result<&mut WalFile> {
        if self.files.is_empty() || self.files.back().and_then(|f| f.fd.as_ref()).is_none() {
            self.create_next_file()?;
        }
        Ok(self.files.back_mut().unwrap())
    }

    fn should_rotate(&self) -> bool {
        self.files
            .back()
            .map(|f| f.bytes_written >= self.max_file_size)
            .unwrap_or(true)
    }

    /// Flush the userland buffer of `fd` and fsync the inner file.
    fn flush_and_fsync(fd: &mut BufWriter<File>) -> io::Result<()> {
        fd.flush()?;
        fd.get_ref().sync_all()
    }

    /// Record that a sync happened, updating tracking state.
    fn record_sync(&mut self) {
        #[cfg(test)]
        {
            self.sync_count += 1;
        }
        self.last_sync_at = Instant::now();
    }

    fn rotate_if_needed(&mut self) -> io::Result<()> {
        if self.should_rotate() {
            // Close current file
            if let Some(f) = self.files.back_mut()
                && let Some(mut fd) = f.fd.take()
            {
                Self::flush_and_fsync(&mut fd)?;
                self.record_sync();
            }
            self.create_next_file()?;
        }
        Ok(())
    }

    // --- Space reservation ---

    pub fn reserve_put(&self, record_size: usize) -> bool {
        // The WAL creates new files on demand, so total space is unbounded.
        // Just verify the record + its future delete fit in a single file.
        let needed = record_size + STATE_CHANGE_RECORD_SIZE;
        needed <= self.max_file_size
    }

    // --- Write operations ---

    pub fn write_put(&mut self, job: &mut Job) -> io::Result<()> {
        self.rotate_if_needed()?;

        let record = serialize_full_job(job);
        let record_len = record.len();

        let sync_per_write = self.sync_interval.is_zero();
        let file = self.current_file_mut()?;
        let fd = file
            .fd
            .as_mut()
            .ok_or_else(|| io::Error::other("WAL file not open for writing"))?;
        fd.write_all(&record)?;
        file.bytes_written += record_len;
        if sync_per_write {
            Self::flush_and_fsync(fd)?;
        }

        // Update job's WAL tracking
        let old_seq = job.wal_file_seq;
        let old_used = job.wal_used;
        job.wal_file_seq = Some(file.seq);
        job.wal_used = record_len;

        // Decref old file
        if let Some(old) = old_seq {
            self.decref_file(old, old_used);
        }

        // Incref new file
        let seq = job.wal_file_seq.unwrap();
        self.incref_file(seq, record_len);

        // Reserve space for future state change (delete).
        // Only add a reservation for NEW jobs (old_seq is None).
        // Compaction migrations (old_seq is Some) already have a reservation
        // from the original put — adding another would leak reserved_bytes
        // on every migration cycle.
        if old_seq.is_none() {
            self.reserved_bytes += STATE_CHANGE_RECORD_SIZE as u64;
        }

        if sync_per_write {
            self.record_sync();
        }

        Ok(())
    }

    pub fn write_state_change(
        &mut self,
        job: &mut Job,
        new_state: Option<JobState>,
        new_priority: u32,
        new_delay: Duration,
        expiry_epoch_secs: u64,
        reason: StateChangeReason,
    ) -> io::Result<()> {
        self.rotate_if_needed()?;

        let delay_nanos = new_delay.as_nanos().min(u64::MAX as u128) as u64;
        let record = serialize_state_change(
            job.id,
            new_state,
            new_priority,
            delay_nanos,
            expiry_epoch_secs,
            reason,
        );
        let record_len = record.len();

        let sync_per_write = self.sync_interval.is_zero();
        let file = self.current_file_mut()?;
        let fd = file
            .fd
            .as_mut()
            .ok_or_else(|| io::Error::other("WAL file not open for writing"))?;
        fd.write_all(&record)?;
        file.bytes_written += record_len;
        if sync_per_write {
            Self::flush_and_fsync(fd)?;
        }

        // Release reservation
        self.reserved_bytes = self
            .reserved_bytes
            .saturating_sub(STATE_CHANGE_RECORD_SIZE as u64);

        // For deletes, decref old file and reduce alive bytes
        if new_state.is_none() {
            if let Some(old_seq) = job.wal_file_seq {
                self.decref_file(old_seq, job.wal_used);
            }
            job.wal_file_seq = None;
            job.wal_used = 0;
        } else {
            // For non-delete state changes (bury, release, kick), do NOT move
            // the job's WAL ref. The wal_file_seq must keep pointing at the file
            // containing the FullJob record, since StateChange records don't
            // carry the full job body. Moving the ref would allow GC to delete
            // the FullJob's file, causing silent data loss on replay.
        }

        if sync_per_write {
            self.record_sync();
        }

        Ok(())
    }

    fn incref_file(&mut self, seq: u64, used: usize) {
        for f in self.files.iter_mut() {
            if f.seq == seq {
                f.refs += 1;
                break;
            }
        }
        self.alive_bytes += used as u64;
    }

    fn decref_file(&mut self, seq: u64, used: usize) {
        for f in self.files.iter_mut() {
            if f.seq == seq {
                f.refs = f.refs.saturating_sub(1);
                break;
            }
        }
        self.alive_bytes = self.alive_bytes.saturating_sub(used as u64);
    }

    // --- Replay ---

    pub fn replay(&mut self) -> io::Result<(HashMap<u64, Job>, u64, Vec<IdpTombstone>)> {
        let mut jobs: HashMap<u64, Job> = HashMap::new();
        let mut max_id: u64 = 0;
        let mut tombstones: Vec<IdpTombstone> = Vec::new();
        let replay_time = SystemTime::now();

        // Read and process each file
        let file_infos: Vec<(u64, PathBuf)> =
            self.files.iter().map(|f| (f.seq, f.path.clone())).collect();

        // Reset refs
        for f in self.files.iter_mut() {
            f.refs = 0;
        }
        self.alive_bytes = 0;
        self.reserved_bytes = 0;

        for (seq, path) in &file_infos {
            let data = match fs::read(path) {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!("WAL: skipping file {:?}: {}", path, e);
                    continue;
                }
            };

            if let Err(e) = read_header(&data) {
                tracing::warn!("WAL: bad header in {:?}: {}", path, e);
                continue;
            }
            // flags field read but currently unused (reserved for future features)

            let mut offset = HEADER_SIZE;
            while offset < data.len() {
                match deserialize_record(&data[offset..]) {
                    Ok((record, consumed)) => {
                        match record {
                            WalRecord::FullJob(mut job) => {
                                if job.id > max_id {
                                    max_id = job.id;
                                }
                                // Track WAL position
                                let record_size = consumed;
                                job.wal_file_seq = Some(*seq);
                                job.wal_used = record_size;

                                // Remove old ref if replacing
                                if let Some(old_job) = jobs.get(&job.id)
                                    && let Some(old_seq) = old_job.wal_file_seq
                                {
                                    self.decref_file(old_seq, old_job.wal_used);
                                }

                                self.incref_file(*seq, record_size);
                                jobs.insert(job.id, *job);
                            }
                            WalRecord::StateChange {
                                job_id,
                                new_state,
                                new_priority,
                                new_delay_nanos,
                                expiry_epoch_secs,
                                reason,
                            } => {
                                if job_id > max_id {
                                    max_id = job_id;
                                }
                                match new_state {
                                    None => {
                                        // Deleted — check for idempotency tombstone
                                        if expiry_epoch_secs > 0 {
                                            let expires_at =
                                                UNIX_EPOCH + Duration::from_secs(expiry_epoch_secs);
                                            if expires_at > replay_time {
                                                // Tombstone still active — extract idp info from job before removing
                                                if let Some(job) = jobs.get(&job_id)
                                                    && let Some((ref key, _)) = job.idempotency_key
                                                {
                                                    tombstones.push(IdpTombstone {
                                                        tube_name: job.tube_name.clone(),
                                                        key: key.clone(),
                                                        job_id,
                                                        expires_at,
                                                    });
                                                }
                                            }
                                        }
                                        if let Some(old_job) = jobs.remove(&job_id)
                                            && let Some(old_seq) = old_job.wal_file_seq
                                        {
                                            self.decref_file(old_seq, old_job.wal_used);
                                        }
                                    }
                                    Some(state) => {
                                        if let Some(job) = jobs.get_mut(&job_id) {
                                            // Update state
                                            let replay_state = match state {
                                                JobState::Reserved => JobState::Ready,
                                                other => other,
                                            };
                                            job.state = replay_state;
                                            job.priority = new_priority;
                                            job.delay = Duration::from_nanos(new_delay_nanos);
                                            if replay_state == JobState::Delayed {
                                                job.deadline_at = Some(Instant::now() + job.delay);
                                            } else {
                                                job.deadline_at = None;
                                            }
                                            job.reserver_id = None;

                                            // Increment counter based on reason
                                            match reason {
                                                StateChangeReason::Reserve => job.reserve_ct += 1,
                                                StateChangeReason::Release => job.release_ct += 1,
                                                StateChangeReason::Bury => job.bury_ct += 1,
                                                StateChangeReason::Kick => job.kick_ct += 1,
                                                StateChangeReason::Timeout => job.timeout_ct += 1,
                                                StateChangeReason::None => {}
                                            }

                                            // Do NOT update WAL tracking here.
                                            // The job's wal_file_seq must keep
                                            // pointing at its FullJob record.
                                        }
                                    }
                                }
                            }
                        }
                        offset += consumed;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "WAL: error reading record in {:?} at offset {}: {}, truncating",
                            path,
                            offset,
                            e
                        );
                        // Truncate the file at the corrupt offset to prevent
                        // repeated warnings on every restart.
                        if let Ok(f) = OpenOptions::new().write(true).open(path) {
                            if let Err(te) = f.set_len(offset as u64) {
                                tracing::warn!("WAL: failed to truncate {:?}: {}", path, te);
                            }
                        }
                        break;
                    }
                }
            }
        }

        // Reserve bytes for each live job's future delete
        self.reserved_bytes = jobs.len() as u64 * STATE_CHANGE_RECORD_SIZE as u64;

        // Create new writable file
        self.create_next_file()?;

        Ok((jobs, max_id + 1, tombstones))
    }

    // --- GC and compaction ---

    pub fn gc(&mut self) {
        // Remove head files with refs == 0, but never the current writable file
        while self.files.len() > 1 {
            if self.files.front().map(|f| f.refs == 0).unwrap_or(false) {
                let f = self.files.pop_front().unwrap();
                if let Err(e) = fs::remove_file(&f.path) {
                    tracing::warn!("WAL: failed to remove {:?}: {}", f.path, e);
                }
            } else {
                break;
            }
        }
    }

    /// Run GC and, if the configured interval has elapsed, fsync the current file.
    ///
    /// The userland buffer is always flushed so that GC, stats, and replay see a
    /// consistent view. The fsync itself is skipped when `sync_interval` is zero
    /// (writes already synced inline) or when the interval has not yet elapsed.
    pub fn maintain(&mut self) {
        self.gc();

        if let Some(f) = self.files.back_mut()
            && let Some(fd) = f.fd.as_mut()
        {
            let _ = fd.flush();

            // Skip interval sync when interval=0 — writes already synced inline.
            if !self.sync_interval.is_zero() && self.last_sync_at.elapsed() >= self.sync_interval {
                let _ = fd.get_ref().sync_all();
                self.record_sync();
            }
        }
    }

    /// Flush the userland buffer and fsync the current file unconditionally.
    /// Used on shutdown to guarantee the buffered tail reaches disk regardless
    /// of the configured sync interval.
    pub fn flush_and_sync(&mut self) {
        if let Some(f) = self.files.back_mut()
            && let Some(fd) = f.fd.as_mut()
        {
            let _ = Self::flush_and_fsync(fd);
            self.record_sync();
        }
    }

    /// Test-only: number of `sync_all` calls observed since the WAL was opened.
    #[cfg(test)]
    pub fn sync_count(&self) -> u64 {
        self.sync_count
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_job(id: u64, body: &[u8]) -> Job {
        let mut job = Job::new(
            id,
            100,
            Duration::from_secs(5),
            Duration::from_secs(30),
            body.to_vec(),
            "test-tube".to_string(),
        );
        job.reserve_ct = 3;
        job.timeout_ct = 1;
        job.release_ct = 2;
        job.bury_ct = 0;
        job.kick_ct = 1;
        job.idempotency_key = Some(("idem-key".to_string(), 60));
        job.group = Some("group1".to_string());
        job.after_group = None;
        job.concurrency_key = Some(("conc".to_string(), 3));
        job
    }

    #[test]
    fn test_serialize_deserialize_full_job() {
        let job = make_test_job(42, b"hello world");
        let record = serialize_full_job(&job);
        let (rec, consumed) = deserialize_record(&record).unwrap();
        assert_eq!(consumed, record.len());

        if let WalRecord::FullJob(j) = rec {
            assert_eq!(j.id, 42);
            assert_eq!(j.priority, 100);
            assert_eq!(j.delay, Duration::from_secs(5));
            assert_eq!(j.ttr, Duration::from_secs(30));
            assert_eq!(j.body_bytes(), b"hello world");
            assert_eq!(j.tube_name, "test-tube");
            assert_eq!(j.reserve_ct, 3);
            assert_eq!(j.timeout_ct, 1);
            assert_eq!(j.release_ct, 2);
            assert_eq!(j.bury_ct, 0);
            assert_eq!(j.kick_ct, 1);
            assert_eq!(j.idempotency_key, Some(("idem-key".to_string(), 60)));
            assert_eq!(j.group.as_deref(), Some("group1"));
            assert!(j.after_group.is_none());
            assert_eq!(j.concurrency_key, Some(("conc".to_string(), 3)));
            // Reserved replays as Ready
            assert_eq!(j.state, JobState::Delayed); // original was delayed (delay > 0)
            // Body must round-trip as Inline — Phase 2 invariant.
            assert!(matches!(j.body, BodyRef::Inline(_)));
        } else {
            panic!("expected FullJob");
        }
    }

    #[test]
    fn test_serialize_deserialize_state_change() {
        let record = serialize_state_change(
            99,
            Some(JobState::Buried),
            500,
            0,
            0,
            StateChangeReason::Bury,
        );
        assert_eq!(record.len(), STATE_CHANGE_RECORD_SIZE);

        let (rec, consumed) = deserialize_record(&record).unwrap();
        assert_eq!(consumed, STATE_CHANGE_RECORD_SIZE);

        if let WalRecord::StateChange {
            job_id,
            new_state,
            new_priority,
            new_delay_nanos,
            expiry_epoch_secs,
            reason,
        } = rec
        {
            assert_eq!(job_id, 99);
            assert_eq!(new_state, Some(JobState::Buried));
            assert_eq!(new_priority, 500);
            assert_eq!(new_delay_nanos, 0);
            assert_eq!(expiry_epoch_secs, 0);
            assert_eq!(reason, StateChangeReason::Bury);
        } else {
            panic!("expected StateChange");
        }
    }

    #[test]
    fn test_serialize_deserialize_state_change_deleted() {
        let record = serialize_state_change(77, None, 0, 0, 0, StateChangeReason::None);
        let (rec, _) = deserialize_record(&record).unwrap();

        if let WalRecord::StateChange { new_state, .. } = rec {
            assert!(new_state.is_none());
        } else {
            panic!("expected StateChange");
        }
    }

    #[test]
    fn test_crc32_validation() {
        let mut record =
            serialize_state_change(99, Some(JobState::Ready), 0, 0, 0, StateChangeReason::None);
        // Corrupt a byte in the middle
        record[5] ^= 0xFF;
        let result = deserialize_record(&record);
        assert!(matches!(result, Err(WalError::BadCrc)));
    }

    #[test]
    fn test_file_header_roundtrip() {
        let mut buf = Vec::new();
        write_header(&mut buf).unwrap();
        assert_eq!(buf.len(), HEADER_SIZE);
        read_header(&buf).unwrap();
    }

    #[test]
    fn test_invalid_magic() {
        let buf = b"XXXX\x01\x00\x00\x00\x00\x00\x00\x00";
        assert!(matches!(read_header(buf), Err(WalError::BadMagic)));
    }

    #[test]
    fn test_option_string_encoding() {
        let mut buf = Vec::new();

        // None
        write_option_string(&mut buf, &None);
        let mut off = 0;
        assert_eq!(read_option_string(&buf, &mut off).unwrap(), None);

        // Some("")
        buf.clear();
        write_option_string(&mut buf, &Some("".to_string()));
        off = 0;
        assert_eq!(read_option_string(&buf, &mut off).unwrap(), None); // empty = len 0 = None

        // Some("abc")
        buf.clear();
        write_option_string(&mut buf, &Some("abc".to_string()));
        off = 0;
        assert_eq!(
            read_option_string(&buf, &mut off).unwrap(),
            Some("abc".to_string())
        );
    }

    #[test]
    fn test_state_encoding_roundtrip() {
        for state in [
            JobState::Ready,
            JobState::Reserved,
            JobState::Delayed,
            JobState::Buried,
        ] {
            let byte = state_to_u8(state);
            assert_eq!(u8_to_state(byte), Some(state));
        }
        assert_eq!(u8_to_state(STATE_DELETED), None);
    }

    #[test]
    fn test_estimate_full_job_size() {
        let job = make_test_job(1, b"data");
        let estimated = estimate_full_job_size(&job);
        let actual = serialize_full_job(&job).len();
        assert_eq!(estimated, actual);
    }

    // --- Corruption / edge-case tests ---

    #[test]
    fn test_truncated_full_job_header() {
        // Only 5 bytes — not enough for the 13-byte header
        let data = vec![RECORD_TYPE_FULL_JOB, 1, 2, 3, 4];
        assert!(matches!(
            deserialize_record(&data),
            Err(WalError::Truncated)
        ));
    }

    #[test]
    fn test_truncated_full_job_payload() {
        // Valid header claiming a large payload, but data is short
        let mut data = vec![RECORD_TYPE_FULL_JOB];
        data.extend_from_slice(&42u64.to_le_bytes()); // job_id
        data.extend_from_slice(&1000u32.to_le_bytes()); // payload_len = 1000
        // Only 13 bytes total, nowhere near 1000 + 4 CRC
        assert!(matches!(
            deserialize_record(&data),
            Err(WalError::Truncated)
        ));
    }

    #[test]
    fn test_corrupted_full_job_crc() {
        let job = make_test_job(1, b"hello");
        let mut record = serialize_full_job(&job);
        // Corrupt the last byte (CRC)
        let len = record.len();
        record[len - 1] ^= 0xFF;
        assert!(matches!(deserialize_record(&record), Err(WalError::BadCrc)));
    }

    #[test]
    fn test_corrupted_full_job_body() {
        let job = make_test_job(1, b"hello");
        let mut record = serialize_full_job(&job);
        // Corrupt a payload byte (not the CRC itself)
        record[20] ^= 0xFF;
        assert!(matches!(deserialize_record(&record), Err(WalError::BadCrc)));
    }

    #[test]
    fn test_truncated_state_change() {
        // Too short for STATE_CHANGE_RECORD_SIZE
        let data = vec![RECORD_TYPE_STATE_CHANGE, 0, 0, 0];
        assert!(matches!(
            deserialize_record(&data),
            Err(WalError::Truncated)
        ));
    }

    #[test]
    fn test_corrupted_state_change_crc() {
        let mut record =
            serialize_state_change(1, Some(JobState::Ready), 100, 0, 0, StateChangeReason::None);
        let len = record.len();
        record[len - 2] ^= 0xFF;
        assert!(matches!(deserialize_record(&record), Err(WalError::BadCrc)));
    }

    #[test]
    fn test_invalid_state_byte_in_full_job() {
        let job = make_test_job(1, b"x");
        let mut record = serialize_full_job(&job);
        // The state byte is at payload offset 4+8+8+8 = 28 from payload start.
        // Payload starts at byte 13 of the record. So state byte is at 13+28 = 41.
        let state_offset = 13 + 4 + 8 + 8 + 8; // = 41
        record[state_offset] = 0xEE; // invalid state
        // Recompute CRC: everything before last 4 bytes
        let crc_offset = record.len() - 4;
        let crc = crc32fast::hash(&record[..crc_offset]);
        record[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            deserialize_record(&record),
            Err(WalError::InvalidData)
        ));
    }

    #[test]
    fn test_invalid_state_byte_in_state_change() {
        let mut record =
            serialize_state_change(1, Some(JobState::Ready), 0, 0, 0, StateChangeReason::None);
        // State byte is at offset 13 in state change record
        record[13] = 0xEE; // invalid state (not 0-3 or 0xFF)
        // Recompute CRC
        let crc_offset = record.len() - 4;
        let crc = crc32fast::hash(&record[..crc_offset]);
        record[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            deserialize_record(&record),
            Err(WalError::InvalidData)
        ));
    }

    #[test]
    fn test_unknown_record_type() {
        let data = vec![0xFF; STATE_CHANGE_RECORD_SIZE];
        assert!(matches!(
            deserialize_record(&data),
            Err(WalError::UnknownRecordType(0xFF))
        ));
    }

    #[test]
    fn test_empty_data() {
        assert!(matches!(deserialize_record(&[]), Err(WalError::Truncated)));
    }

    #[test]
    fn test_bad_version_header() {
        let mut buf = Vec::new();
        buf.extend_from_slice(WAL_MAGIC);
        buf.extend_from_slice(&99u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // flags
        assert!(matches!(read_header(&buf), Err(WalError::BadVersion(99))));
    }

    #[test]
    fn test_truncated_header() {
        assert!(matches!(
            read_header(&[b'T', b'W']),
            Err(WalError::Truncated)
        ));
    }

    #[test]
    fn test_replay_skips_corrupted_records() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Write two valid jobs, then corrupt the file by appending garbage
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();

            let mut job1 = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body1".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job1).unwrap();

            let mut job2 = Job::new(
                2,
                20,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body2".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job2).unwrap();
        }

        // Append garbage to the WAL file to simulate a partial/corrupted write
        let wal_file = fs::read_dir(dir_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with(FILE_PREFIX))
            .unwrap();
        {
            let mut f = OpenOptions::new()
                .append(true)
                .open(wal_file.path())
                .unwrap();
            f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01]).unwrap();
        }

        // Replay should recover both valid jobs, skip the garbage
        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (jobs, next_id, _) = wal.replay().unwrap();
        assert_eq!(jobs.len(), 2);
        assert!(jobs.contains_key(&1));
        assert!(jobs.contains_key(&2));
        assert!(next_id >= 3);
    }

    #[test]
    fn test_replay_skips_file_with_bad_header() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Write one valid job
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            let mut job1 = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body1".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job1).unwrap();
        }

        // Overwrite the WAL file header with garbage
        let wal_file = fs::read_dir(dir_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with(FILE_PREFIX))
            .unwrap();
        {
            let mut data = fs::read(wal_file.path()).unwrap();
            data[0..4].copy_from_slice(b"BAAD");
            fs::write(wal_file.path(), &data).unwrap();
        }

        // Replay should skip the bad file, recover nothing
        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (jobs, _, _) = wal.replay().unwrap();
        assert!(jobs.is_empty());
    }

    #[test]
    fn test_replay_truncated_record_mid_job() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Write two jobs
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            let mut job1 = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body1".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job1).unwrap();
            let mut job2 = Job::new(
                2,
                20,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body2".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job2).unwrap();
        }

        // Truncate file to cut off the second job mid-record
        let wal_file = fs::read_dir(dir_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with(FILE_PREFIX))
            .unwrap();
        {
            let data = fs::read(wal_file.path()).unwrap();
            // Keep header + first job + a few bytes of the second
            let truncated_len = data.len() - 10;
            fs::write(wal_file.path(), &data[..truncated_len]).unwrap();
        }

        // Should recover job 1 only
        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
        let (jobs, next_id, _) = wal.replay().unwrap();
        assert_eq!(jobs.len(), 1);
        assert!(jobs.contains_key(&1));
        assert!(next_id >= 2);
    }

    #[test]
    fn test_file_count_and_total_disk_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();

        // Before any writes, no files
        assert_eq!(wal.file_count(), 0);
        assert_eq!(wal.total_disk_bytes(), 0);

        // Write a job — triggers file creation
        let mut job1 = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body1".to_vec(),
            "default".to_string(),
        );
        wal.write_put(&mut job1).unwrap();

        assert_eq!(wal.file_count(), 1);
        assert!(
            wal.total_disk_bytes() > 0,
            "should have bytes after a write"
        );

        let bytes_after_one = wal.total_disk_bytes();

        // Write another job — same file
        let mut job2 = Job::new(
            2,
            20,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body2".to_vec(),
            "default".to_string(),
        );
        wal.write_put(&mut job2).unwrap();

        assert_eq!(wal.file_count(), 1);
        assert!(
            wal.total_disk_bytes() > bytes_after_one,
            "total bytes should increase after second write"
        );
    }

    #[test]
    fn test_file_count_after_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Very small max file size to force rotation
        let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(64)).unwrap();

        let mut job1 = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body1".to_vec(),
            "default".to_string(),
        );
        wal.write_put(&mut job1).unwrap();

        let mut job2 = Job::new(
            2,
            20,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body2".to_vec(),
            "default".to_string(),
        );
        wal.write_put(&mut job2).unwrap();

        assert!(
            wal.file_count() >= 2,
            "should have multiple files after rotation, got {}",
            wal.file_count()
        );

        // total_disk_bytes should cover all files
        let total = wal.total_disk_bytes();
        assert!(total > 0);
    }

    #[test]
    fn test_wal_write_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // Write some records
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();

            let mut job1 = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body1".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job1).unwrap();

            let mut job2 = Job::new(
                2,
                20,
                Duration::from_secs(10),
                Duration::from_secs(60),
                b"body2".to_vec(),
                "other".to_string(),
            );
            wal.write_put(&mut job2).unwrap();

            // Delete job 1
            wal.write_state_change(
                &mut job1,
                None,
                0,
                Duration::ZERO,
                0,
                StateChangeReason::None,
            )
            .unwrap();
        }

        // Replay
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            let (jobs, next_id, _) = wal.replay().unwrap();

            // Job 1 was deleted
            assert!(!jobs.contains_key(&1));
            // Job 2 should exist
            assert!(jobs.contains_key(&2));
            let j2 = &jobs[&2];
            assert_eq!(j2.priority, 20);
            assert_eq!(j2.body_bytes(), b"body2");
            assert_eq!(j2.tube_name, "other");
            assert_eq!(j2.state, JobState::Delayed);
            assert!(next_id >= 3);
        }
    }

    #[test]
    fn test_compaction_target_none_when_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(1024 * 1024)).unwrap();

        let mut job = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body".to_vec(),
            "default".to_string(),
        );
        wal.write_put(&mut job).unwrap();

        // Only one file — no compaction target
        assert!(wal.compaction_target().is_none());
    }

    #[test]
    fn test_compaction_target_none_when_low_waste() {
        let dir = tempfile::tempdir().unwrap();
        // Use a large file size so all jobs fit in two files with low waste
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(4096)).unwrap();

        // Write jobs that span exactly 2 files
        for i in 1..=50 {
            let mut job = Job::new(
                i,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job).unwrap();
        }

        assert!(wal.file_count() > 1, "should have multiple files");
        // All jobs alive — alive_bytes is substantial relative to total space,
        // so waste / live_bytes < 2 and no compaction should trigger
        assert!(
            wal.compaction_target().is_none(),
            "should not trigger compaction when all jobs are alive"
        );
    }

    #[test]
    fn test_compaction_target_some_when_high_waste() {
        let dir = tempfile::tempdir().unwrap();
        // Small file size to force many files
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(128)).unwrap();

        // Write many jobs then delete them all except one
        let mut jobs: Vec<Job> = Vec::new();
        for i in 1..=20 {
            let mut job = Job::new(
                i,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"body".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job).unwrap();
            jobs.push(job);
        }

        assert!(wal.file_count() > 1, "should have multiple files");

        // Delete all but the last job
        for job in jobs.iter_mut().take(19) {
            wal.write_state_change(job, None, 0, Duration::ZERO, 0, StateChangeReason::None)
                .unwrap();
        }

        // Now most files have dead data — waste ratio should be high
        let target = wal.compaction_target();
        assert!(
            target.is_some(),
            "should have a compaction target with high waste"
        );
        let (seq, count) = target.unwrap();
        assert_eq!(seq, wal.oldest_seq());
        assert!(count >= 2, "ratio should be >= 2, got {}", count);
    }

    #[test]
    fn test_gc_after_all_refs_removed() {
        let dir = tempfile::tempdir().unwrap();
        // Very small file size to force each job into its own file
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(64)).unwrap();

        let mut job1 = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body1".to_vec(),
            "default".to_string(),
        );
        wal.write_put(&mut job1).unwrap();

        let mut job2 = Job::new(
            2,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body2".to_vec(),
            "default".to_string(),
        );
        wal.write_put(&mut job2).unwrap();

        let mut job3 = Job::new(
            3,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body3".to_vec(),
            "default".to_string(),
        );
        wal.write_put(&mut job3).unwrap();

        let count_before_gc = wal.file_count();
        assert!(count_before_gc > 1, "should have multiple files");

        // Delete all jobs so all files become reclaimable except current
        wal.write_state_change(
            &mut job1,
            None,
            0,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();
        wal.write_state_change(
            &mut job2,
            None,
            0,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();
        wal.write_state_change(
            &mut job3,
            None,
            0,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();

        wal.gc();
        assert!(
            wal.file_count() < count_before_gc,
            "gc should remove files with 0 refs: before={}, after={}",
            count_before_gc,
            wal.file_count()
        );
    }

    #[test]
    fn test_compaction_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(128)).unwrap();

        // Create jobs spanning multiple files
        let mut jobs: Vec<Job> = Vec::new();
        for i in 1..=20 {
            let mut job = Job::new(
                i,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"data".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job).unwrap();
            jobs.push(job);
        }

        let files_before = wal.file_count();

        // Delete most jobs (keep only job 20 in the last file)
        for job in jobs.iter_mut().take(19) {
            wal.write_state_change(job, None, 0, Duration::ZERO, 0, StateChangeReason::None)
                .unwrap();
        }

        // Run gc — should remove leading files with 0 refs
        wal.maintain();
        let files_after = wal.file_count();
        assert!(
            files_after <= files_before,
            "gc should remove files: before={}, after={}",
            files_before,
            files_after
        );

        // The remaining live job (20) may still pin one old file.
        // Simulate compaction: re-write job 20 to move it to the current file.
        let job20 = &mut jobs[19];
        if let Some(old_seq) = job20.wal_file_seq {
            let current_seq = wal.current_seq();
            if old_seq != current_seq {
                // Re-write to current file (this is what the server does)
                wal.write_put(job20).unwrap();
                wal.record_migration();
                wal.maintain(); // gc again
                // Should have fewer files now
                assert!(
                    wal.file_count() < files_before,
                    "compaction should reduce file count"
                );
            }
        }
    }

    #[test]
    fn test_state_change_does_not_lose_job_after_gc() {
        let dir = tempfile::tempdir().unwrap();

        // Use a tiny max file size so the put and state change land in different files
        {
            let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(64)).unwrap();

            let mut job = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"important-data".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job).unwrap();

            // State change goes to a new file due to small max_file_size
            let pri = job.priority;
            wal.write_state_change(
                &mut job,
                Some(JobState::Buried),
                pri,
                Duration::ZERO,
                0,
                StateChangeReason::Bury,
            )
            .unwrap();

            assert!(
                wal.file_count() >= 2,
                "expected multiple WAL files, got {}",
                wal.file_count()
            );

            // GC should NOT remove the file containing the FullJob
            wal.gc();
            assert!(
                wal.file_count() >= 2,
                "GC must not delete the FullJob's file"
            );
        }

        // Reopen and replay — job must survive
        {
            let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(64)).unwrap();
            let (jobs, _next_id, _tombstones) = wal.replay().unwrap();

            assert!(jobs.contains_key(&1), "job 1 must survive replay after GC");
            let job = jobs.get(&1).unwrap();
            assert_eq!(job.state, JobState::Buried);
            assert_eq!(job.body_bytes(), b"important-data");
        }
    }

    #[test]
    fn test_compaction_migration_does_not_leak_reserved_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), WalConfig::with_max_file_size(512)).unwrap();

        let mut job = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"hello".to_vec(),
            "default".to_string(),
        );

        // Initial put — should add one reservation
        wal.write_put(&mut job).unwrap();
        let after_put = wal.reserved_bytes;
        assert_eq!(after_put, STATE_CHANGE_RECORD_SIZE as u64);

        // Simulate compaction migration — should NOT add another reservation
        wal.write_put(&mut job).unwrap();
        assert_eq!(
            wal.reserved_bytes, after_put,
            "compaction migration must not increase reserved_bytes"
        );

        // A third migration — still no increase
        wal.write_put(&mut job).unwrap();
        assert_eq!(
            wal.reserved_bytes, after_put,
            "repeated migrations must not leak reserved_bytes"
        );

        // State change (delete) should consume the single reservation
        wal.write_state_change(
            &mut job,
            None,
            10,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();
        assert_eq!(wal.reserved_bytes, 0, "delete should consume reservation");
    }

    #[test]
    fn test_reserve_put_rejects_oversized_record() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open(dir.path(), WalConfig::with_max_file_size(512)).unwrap();

        // A record that fits
        assert!(wal.reserve_put(100));

        // A record larger than max_file_size
        assert!(!wal.reserve_put(1024));
    }

    #[test]
    fn test_replay_restores_counters_from_reason() {
        // Verifies that state change reason bytes allow replay to reconstruct
        // per-job counters. Simulates: put → reserve → release → reserve →
        // timeout → reserve → bury → kick → bury (final state: Buried).
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();

            let mut job = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"test-body".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job).unwrap();

            // reserve #1
            job.reserve_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Reserved),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Reserve,
            )
            .unwrap();
            // release #1
            job.release_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Ready),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Release,
            )
            .unwrap();
            // reserve #2
            job.reserve_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Reserved),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Reserve,
            )
            .unwrap();
            // timeout #1
            job.timeout_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Ready),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Timeout,
            )
            .unwrap();
            // reserve #3
            job.reserve_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Reserved),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Reserve,
            )
            .unwrap();
            // bury #1
            job.bury_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Buried),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Bury,
            )
            .unwrap();
            // kick #1
            job.kick_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Ready),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Kick,
            )
            .unwrap();
            // reserve #4 (to bury again)
            job.reserve_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Reserved),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Reserve,
            )
            .unwrap();
            // bury #2
            job.bury_ct += 1;
            wal.write_state_change(
                &mut job,
                Some(JobState::Buried),
                10,
                Duration::ZERO,
                0,
                StateChangeReason::Bury,
            )
            .unwrap();
        }

        // Replay and verify all counters
        {
            let mut wal = Wal::open(dir_path, WalConfig::with_max_file_size(1024 * 1024)).unwrap();
            let (jobs, _, _) = wal.replay().unwrap();

            let job = jobs.get(&1).expect("job 1 should exist after replay");
            assert_eq!(job.state, JobState::Buried);
            assert_eq!(job.reserve_ct, 4, "reserve_ct: 4 reserves");
            assert_eq!(job.release_ct, 1, "release_ct: 1 release");
            assert_eq!(job.timeout_ct, 1, "timeout_ct: 1 timeout");
            assert_eq!(job.bury_ct, 2, "bury_ct: 2 buries");
            assert_eq!(job.kick_ct, 1, "kick_ct: 1 kick");
        }
    }

    #[test]
    fn test_replay_v3_state_change_no_counter_update() {
        // Verify that v3 StateChange records (payload_len=21, no reason byte)
        // are still readable — counters just stay at 0 (backward compatible).
        let v3_record = {
            // Manually build a v3 state change record (payload_len=21, no reason byte)
            let mut record = Vec::new();
            record.push(RECORD_TYPE_STATE_CHANGE);
            record.extend_from_slice(&1u64.to_le_bytes()); // job_id
            record.extend_from_slice(&STATE_CHANGE_PAYLOAD_LEN_V3.to_le_bytes());
            record.push(state_to_u8(JobState::Buried)); // state
            record.extend_from_slice(&10u32.to_le_bytes()); // priority
            record.extend_from_slice(&0u64.to_le_bytes()); // delay_nanos
            record.extend_from_slice(&0u64.to_le_bytes()); // expiry_epoch_secs
            let crc = crc32fast::hash(&record);
            record.extend_from_slice(&crc.to_le_bytes());
            record
        };
        assert_eq!(v3_record.len(), 38); // old size

        let (rec, consumed) = deserialize_record(&v3_record).unwrap();
        assert_eq!(consumed, 38);
        if let WalRecord::StateChange {
            reason, new_state, ..
        } = rec
        {
            assert_eq!(reason, StateChangeReason::None);
            assert_eq!(new_state, Some(JobState::Buried));
        } else {
            panic!("expected StateChange");
        }
    }

    // --- Sync-mode / buffering tests ---

    fn config_with_interval(max: usize, interval: Duration) -> WalConfig {
        WalConfig {
            max_file_size: Some(max),
            sync_interval: interval,
        }
    }

    #[test]
    fn test_sync_interval_zero_syncs_per_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(
            dir.path(),
            config_with_interval(1024 * 1024, Duration::ZERO),
        )
        .unwrap();

        let baseline = wal.sync_count();

        let mut job1 = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"a".to_vec(),
            "default".to_string(),
        );
        wal.write_put(&mut job1).unwrap();
        let mut job2 = Job::new(
            2,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"b".to_vec(),
            "default".to_string(),
        );
        wal.write_put(&mut job2).unwrap();
        wal.write_state_change(
            &mut job1,
            None,
            10,
            Duration::ZERO,
            0,
            StateChangeReason::None,
        )
        .unwrap();

        // 3 writes × 1 sync each = 3 additional syncs.
        assert_eq!(wal.sync_count() - baseline, 3);
    }

    #[test]
    fn test_sync_interval_positive_respects_interval() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(
            dir.path(),
            config_with_interval(1024 * 1024, Duration::from_millis(500)),
        )
        .unwrap();

        // First write creates a file (which rotates from "no current file" and syncs
        // the previous one — but there is none, so no sync on rotation). Write is buffered.
        let mut job = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(60),
            b"body".to_vec(),
            "default".to_string(),
        );
        wal.write_put(&mut job).unwrap();

        let baseline = wal.sync_count();

        // Repeated maintain() calls well within the interval should not fsync.
        for _ in 0..5 {
            wal.maintain();
        }
        assert_eq!(
            wal.sync_count(),
            baseline,
            "maintain must not sync before interval elapses"
        );

        // Sleep past the interval and call maintain once — should sync exactly once.
        std::thread::sleep(Duration::from_millis(550));
        wal.maintain();
        assert_eq!(
            wal.sync_count(),
            baseline + 1,
            "maintain must sync once after interval"
        );

        // Immediately calling maintain again should not re-sync (interval reset).
        wal.maintain();
        assert_eq!(wal.sync_count(), baseline + 1);
    }

    #[test]
    fn test_buffered_write_replays_after_maintain() {
        // Guards the invariant that maintain()'s flush makes buffered data
        // visible to a subsequent replay on a fresh Wal instance.
        let dir = tempfile::tempdir().unwrap();

        {
            let mut wal = Wal::open(
                dir.path(),
                config_with_interval(1024 * 1024, Duration::from_secs(3600)),
            )
            .unwrap();
            let mut job = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"buffered".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job).unwrap();
            // Force a flush through maintain (the interval is far in the future,
            // so no fsync, but the BufWriter flush must still run).
            wal.maintain();
            // Drop without calling flush_and_sync — relying on maintain's flush.
        }

        let mut wal = Wal::open(
            dir.path(),
            config_with_interval(1024 * 1024, Duration::from_secs(3600)),
        )
        .unwrap();
        let (jobs, _, _) = wal.replay().unwrap();
        assert!(jobs.contains_key(&1), "job should survive buffered replay");
        assert_eq!(jobs[&1].body_bytes(), b"buffered");
    }

    #[test]
    fn test_flush_and_sync_makes_data_durable() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut wal = Wal::open(
                dir.path(),
                config_with_interval(1024 * 1024, Duration::from_secs(3600)),
            )
            .unwrap();
            let before = wal.sync_count();
            let mut job = Job::new(
                1,
                10,
                Duration::ZERO,
                Duration::from_secs(60),
                b"shutdown".to_vec(),
                "default".to_string(),
            );
            wal.write_put(&mut job).unwrap();
            wal.flush_and_sync();
            assert!(wal.sync_count() > before, "flush_and_sync must always sync");
        }

        let mut wal = Wal::open(
            dir.path(),
            config_with_interval(1024 * 1024, Duration::from_secs(3600)),
        )
        .unwrap();
        let (jobs, _, _) = wal.replay().unwrap();
        assert!(jobs.contains_key(&1));
    }
}
