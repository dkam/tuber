use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::body_store::BodyStore;
use crate::conn::{ConnState, ReserveMode, WatchedTube};
use crate::job::{BodyId, BodyRef, Job, JobState, URGENT_THRESHOLD};
use crate::protocol::{self, Command, MAX_DELETE_BATCH, Response};
use crate::tube::{Tube, TubeStats};
use crate::wal::{IdpTombstone, StateChangeReason, Wal};

/// EWMA smoothing factor for all timing stats (processing time, queue time).
const EWMA_ALPHA: f64 = 0.1;

/// Threshold separating "fast" from "slow" jobs for dual EWMA tracking (seconds).
const FAST_THRESHOLD: f64 = 0.1;

/// Minimum processing time EWMA (seconds) used in weighted-fair mode to prevent
/// tubes with very fast jobs from dominating selection.
const FAIR_EWMA_FLOOR: f64 = 0.01;

/// Fixed memory overhead per live job, covering the Job struct baseline
/// (~250 bytes of fields), HashMap bucket overhead (~48 bytes), the ready/delay
/// heap entry (~16 bytes), plus a safety margin. Users budget
/// `body_len + JOB_OVERHEAD_BYTES` per job against `--max-jobs-size`.
///
/// If `struct Job` in src/job.rs grows significantly, revisit this constant.
const JOB_OVERHEAD_BYTES: u64 = 512;

/// Fixed memory overhead per idempotency-cooldown tombstone.
const TOMBSTONE_OVERHEAD_BYTES: u64 = 128;

/// Minimum number of processing time samples before weighted-fair uses EWMA.
/// Below this threshold, raw weights are used.
const FAIR_MIN_SAMPLES: u64 = 10;

/// Cap on heap entries inspected by find_best_unblocked_ready when the heap
/// top is concurrency-blocked. Bounds slow-path cost on large tubes; if the
/// real unblocked job is deeper than this, the next process_queue tick or
/// state-change event re-checks.
const FIND_UNBLOCKED_MAX_VISITS: usize = 256;

/// Interval at which the background TOAST compactor scans for sealed
/// segments below the live-ratio threshold. Brisk enough to absorb
/// realistic delete bursts without burning cycles on idle queues.
const TOAST_COMPACTION_INTERVAL: Duration = Duration::from_secs(5);

// Op index constants matching beanstalkd (see tmp/prot.c)
const OP_PUT: usize = 1;
const OP_PEEKJOB: usize = 2;
const OP_RESERVE: usize = 3;
const OP_DELETE: usize = 4;
const OP_RELEASE: usize = 5;
const OP_BURY: usize = 6;
const OP_KICK: usize = 7;
const OP_STATS: usize = 8;
const OP_STATSJOB: usize = 9;
const OP_PEEK_BURIED: usize = 10;
const OP_USE: usize = 11;
const OP_WATCH: usize = 12;
const OP_IGNORE: usize = 13;
const OP_LIST_TUBES: usize = 14;
const OP_LIST_TUBE_USED: usize = 15;
const OP_LIST_TUBES_WATCHED: usize = 16;
const OP_STATS_TUBE: usize = 17;
const OP_PEEK_READY: usize = 18;
const OP_PEEK_DELAYED: usize = 19;
const OP_RESERVE_TIMEOUT: usize = 20;
const OP_TOUCH: usize = 21;
const OP_PAUSE_TUBE: usize = 23;
const OP_RESERVE_MODE: usize = 26;
const OP_PEEK_RESERVED: usize = 27;

/// Message from a connection task to the engine.
struct EngineMsg {
    conn_id: u64,
    payload: EnginePayload,
}

enum EnginePayload {
    Command {
        cmd: Command,
        body: Option<Vec<u8>>,
        reply_tx: oneshot::Sender<Response>,
    },
    Disconnect,
    Shutdown,
}

/// Waiting reservation request, stored when no job is immediately available.
struct WaitingReserve {
    conn_id: u64,
    reply_tx: oneshot::Sender<Response>,
    deadline: Option<Instant>,
}

#[derive(Debug, Default)]
struct GlobalStats {
    urgent_ct: u64,
    buried_ct: u64,
    reserved_ct: u64,
    waiting_ct: u64,
    total_jobs_ct: u64,
    total_delete_ct: u64,
    timeout_ct: u64,
    op_ct: [u64; 28],
    total_connections: u64,
    /// Number of times the tick-time drift detector observed a non-zero
    /// `total_job_bytes` while the live set (jobs + tombstones) was empty.
    /// Every increment is a tuber bug; alert on it.
    accounting_drift_events: u64,
    /// Jobs reaped at startup because their WAL FullJob record referenced
    /// a `BodyId` that wasn't present in TOAST (segment header corruption,
    /// manual file removal, or the corruption-drop path in
    /// `compact_segment`). One-shot value set during `build_state`.
    /// Every nonzero value indicates lost data — alert on it.
    recovered_missing_bodies: u64,
}

/// State for a job group (grp:/aft: feature).
#[derive(Debug)]
struct GroupState {
    /// Number of jobs with `grp:<id>` that haven't been deleted yet.
    pending: u64,
    /// Number of jobs with `grp:<id>` that are currently buried.
    buried: u64,
    /// Job IDs with `aft:<id>` that are held waiting for group completion.
    waiting_jobs: Vec<u64>,
}

impl GroupState {
    fn new() -> Self {
        GroupState {
            pending: 0,
            buried: 0,
            waiting_jobs: Vec::new(),
        }
    }

    /// Group is complete when all members are deleted and none are buried.
    fn is_complete(&self) -> bool {
        self.pending == 0 && self.buried == 0
    }

    /// Group can be cleaned up when complete and no after-jobs waiting.
    fn is_idle(&self) -> bool {
        self.is_complete() && self.waiting_jobs.is_empty()
    }

    fn remove_waiting_job(&mut self, id: u64) {
        self.waiting_jobs.retain(|&jid| jid != id);
    }
}

/// All server state, owned by the engine task.
struct ServerState {
    jobs: HashMap<u64, Job>,
    tubes: HashMap<String, Tube>,
    conns: HashMap<u64, ConnState>,
    next_job_id: u64,
    max_job_size: u32,
    /// Optional cap on total in-memory job bytes (bodies + overhead + tombstones).
    /// `None` disables the check.
    max_job_bytes: Option<u64>,
    /// Optional combined disk budget for the WAL + body store (bytes). When
    /// set, `cmd_put` returns `OUT_OF_STORAGE` once accepting a body would
    /// push the projected footprint (live disk + a one-WAL-segment reserve
    /// for future state-change records) past the budget. State changes
    /// (delete, release, bury, kick, touch) bypass this cap so an operator
    /// can always drain a wedged queue. `None` disables the check.
    max_storage_bytes: Option<u64>,
    /// Running sum of `job_memory_cost` for all jobs plus `tombstone_memory_cost`
    /// for every live idempotency-cooldown entry. Maintained via the
    /// `insert_job` / `take_job` / `delete_job` / `insert_tombstone` / `remove_tombstone`
    /// helpers — the raw HashMap must not be mutated directly.
    total_job_bytes: u64,
    drain_mode: bool,
    ready_ct: u64,
    started_at: Instant,
    rng_state: u64,
    stats: GlobalStats,
    /// Connections waiting for a job via reserve.
    waiters: Vec<WaitingReserve>,
    /// Optional write-ahead log for persistence.
    wal: Option<Wal>,
    /// External body store ("TOAST"). Present iff `wal` is — bodies live
    /// here when persistence is enabled, addressed by `BodyId` in the WAL's
    /// `FullJob` records (v5+).
    body_store: Option<Arc<BodyStore>>,
    /// Hex-encoded random instance ID (16 chars).
    instance_id: String,
    /// Optional user-assigned instance name.
    name: Option<String>,
    /// Cached system info from uname.
    hostname: String,
    os: String,
    platform: String,
    /// Job group tracking for grp:/aft: features.
    groups: HashMap<String, GroupState>,
    /// Active reservation count per concurrency key.
    concurrency_keys: HashMap<String, u32>,
    /// Concurrency limit per key (max concurrent reservations allowed).
    concurrency_limits: HashMap<String, u32>,
}

impl ServerState {
    fn new(
        max_job_size: u32,
        max_job_bytes: Option<u64>,
        max_storage_bytes: Option<u64>,
        name: Option<String>,
    ) -> Self {
        let mut tubes = HashMap::new();
        tubes.insert("default".to_string(), Tube::new("default"));

        // Generate instance_id from /dev/urandom (fallback: pid + timestamp)
        let instance_id = {
            let mut bytes = [0u8; 8];
            let got_random = std::fs::File::open("/dev/urandom")
                .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut bytes))
                .is_ok();
            if !got_random {
                let pid = std::process::id() as u64;
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                bytes[..8].copy_from_slice(&(pid ^ ts).to_le_bytes());
            }
            let mut hex = String::with_capacity(16);
            for b in &bytes {
                use std::fmt::Write;
                let _ = write!(hex, "{:02x}", b);
            }
            hex
        };

        // Seed PRNG from the same random bytes used for instance_id
        let rng_seed = {
            let mut seed_bytes = [0u8; 8];
            let got_random = std::fs::File::open("/dev/urandom")
                .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut seed_bytes))
                .is_ok();
            if !got_random {
                let pid = std::process::id() as u64;
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                seed_bytes = (pid ^ ts).to_le_bytes();
            }
            u64::from_le_bytes(seed_bytes)
        };

        // Cache system info from uname
        let (hostname, os, platform) = {
            let mut utsname: libc::utsname = unsafe { std::mem::zeroed() };
            let ret = unsafe { libc::uname(&mut utsname) };
            if ret == 0 {
                let to_string = |arr: &[libc::c_char]| {
                    unsafe { std::ffi::CStr::from_ptr(arr.as_ptr()) }
                        .to_string_lossy()
                        .into_owned()
                };
                (
                    to_string(&utsname.nodename),
                    format!(
                        "{} {}",
                        to_string(&utsname.sysname),
                        to_string(&utsname.release)
                    ),
                    to_string(&utsname.machine),
                )
            } else {
                (
                    "unknown".to_string(),
                    "unknown".to_string(),
                    "unknown".to_string(),
                )
            }
        };

        ServerState {
            jobs: HashMap::new(),
            tubes,
            conns: HashMap::new(),
            next_job_id: 1,

            max_job_size,
            max_job_bytes,
            max_storage_bytes,
            total_job_bytes: 0,
            drain_mode: false,
            ready_ct: 0,
            started_at: Instant::now(),
            rng_state: rng_seed,
            stats: GlobalStats::default(),
            waiters: Vec::new(),
            wal: None,
            body_store: None,
            instance_id,
            name,
            hostname,
            os,
            platform,
            groups: HashMap::new(),
            concurrency_keys: HashMap::new(),
            concurrency_limits: HashMap::new(),
        }
    }

    /// Materialise a job's body bytes for the wire. Inline bodies are cloned
    /// directly; external bodies are read from the body store. Returns
    /// `None` if the body is external but the store is missing or the read
    /// fails — call sites surface this as `Response::InternalError`.
    fn fetch_body(&self, job: &Job) -> Option<Vec<u8>> {
        match &job.body {
            BodyRef::Inline(v) => Some(v.clone()),
            BodyRef::External(id) => {
                let bs = self.body_store.as_ref()?;
                match bs.read_body(*id) {
                    Ok(bytes) => Some(bytes),
                    Err(e) => {
                        tracing::error!(body_id = id.0, "TOAST read failed: {}", e);
                        None
                    }
                }
            }
        }
    }

    /// Wrap a `peek*` lookup in a `Response`. Returns `Found` when the
    /// body materialises, `InternalError` when an external body cannot be
    /// read.
    fn found_or_error(&self, job: &Job) -> Response {
        let id = job.id;
        match self.fetch_body(job) {
            Some(body) => Response::Found { id, body },
            None => Response::InternalError,
        }
    }

    // --- Memory accounting ---
    //
    // All mutations of `self.jobs` and `tube.idempotency_cooldowns` MUST go
    // through these helpers. A missed call site here causes slow drift of
    // `total_job_bytes` away from reality, which manifests as `OUT_OF_MEMORY`
    // responses on near-empty queues. Grep for `self.jobs.insert`,
    // `self.jobs.remove`, and `idempotency_cooldowns` to audit.

    fn job_memory_cost(job: &Job) -> u64 {
        job.body_size() as u64 + JOB_OVERHEAD_BYTES
    }

    fn tombstone_memory_cost(key: &str) -> u64 {
        key.len() as u64 + TOMBSTONE_OVERHEAD_BYTES
    }

    /// True iff adding `additional` bytes would exceed the configured budget.
    /// Returns false (allow) when `max_job_bytes` is `None`.
    fn memory_limit_exceeded(&self, additional: u64) -> bool {
        match self.max_job_bytes {
            Some(limit) => self.total_job_bytes.saturating_add(additional) > limit,
            None => false,
        }
    }

    /// True iff a put of `body_len` bytes would push WAL + TOAST disk usage
    /// past the configured budget, accounting for a one-segment WAL reserve.
    /// Returns false (allow) when `max_storage_bytes` is `None` or the WAL
    /// is disabled (no `-b`). Body-store overhead is approximated as a
    /// constant; this is a soft fence, not a tight ceiling.
    fn storage_limit_exceeded(&self, body_len: u64) -> bool {
        let limit = match self.max_storage_bytes {
            Some(l) => l,
            None => return false,
        };
        let wal = match self.wal.as_ref() {
            Some(w) => w,
            None => return false,
        };
        // Per-body TOAST overhead: record header + worst-case file-header
        // amortisation when every put forces a fresh segment, plus padding
        // so this stays a generous over-estimate as the layout evolves.
        const BODY_OVERHEAD: u64 =
            (crate::body_store::BODY_HEADER_SIZE + crate::body_store::FILE_HEADER_SIZE) as u64 + 28;
        let toast_bytes = self.body_store.as_ref().map_or(0, |bs| bs.total_bytes());
        let wal_bytes = wal.total_disk_bytes();
        // Reserve one WAL segment's worth of headroom for state-change
        // churn, matching the design doc ("WAL gets a small reserved
        // minimum, one segment's worth"). For the default 10 MiB segment
        // this is 10 MiB — same as before. The absolute floor protects
        // pathological configs (max_file_size in the kilobyte range)
        // without imposing the default segment size on operators who
        // deliberately picked a smaller one. ~1600 state-change records
        // fit in 64 KiB, well above any realistic per-tick burst.
        const MIN_WAL_RESERVE: u64 = 64 * 1024;
        let wal_reserve = (wal.max_file_size() as u64).max(MIN_WAL_RESERVE);
        let projected = wal_bytes
            .saturating_add(toast_bytes)
            .saturating_add(body_len)
            .saturating_add(BODY_OVERHEAD)
            .saturating_add(wal_reserve);
        projected > limit
    }

    fn insert_job(&mut self, id: u64, job: Job) {
        self.total_job_bytes = self
            .total_job_bytes
            .saturating_add(Self::job_memory_cost(&job));
        self.jobs.insert(id, job);
    }

    /// Take a job out of the map for short-lived bookkeeping (the caller
    /// re-inserts it before yielding control). Does NOT drop the external
    /// body — see `delete_job` for permanent removal.
    fn take_job(&mut self, id: u64) -> Option<Job> {
        let job = self.jobs.remove(&id)?;
        self.total_job_bytes = self
            .total_job_bytes
            .saturating_sub(Self::job_memory_cost(&job));
        Some(job)
    }

    /// Permanently remove a job and reclaim its external body if any.
    /// The single correct entry point for delete/flush/expiry paths.
    fn delete_job(&mut self, id: u64) -> Option<Job> {
        let job = self.take_job(id)?;
        if let (BodyRef::External(body_id), Some(bs)) = (&job.body, self.body_store.as_ref()) {
            bs.delete(*body_id);
        }
        Some(job)
    }

    fn insert_tombstone(
        &mut self,
        tube_name: &str,
        key: String,
        job_id: u64,
        expires_at: SystemTime,
    ) {
        let cost = Self::tombstone_memory_cost(&key);
        // Replacing an existing tombstone is net-zero. Decrement first so the
        // subsequent add doesn't double-count.
        let replacing = self
            .tubes
            .get(tube_name)
            .is_some_and(|t| t.idempotency_cooldowns.contains_key(&key));
        if replacing {
            self.total_job_bytes = self.total_job_bytes.saturating_sub(cost);
        }
        self.total_job_bytes = self.total_job_bytes.saturating_add(cost);
        if let Some(tube) = self.tubes.get_mut(tube_name) {
            tube.idempotency_cooldowns.insert(key, (job_id, expires_at));
        }
    }

    fn remove_tombstone(&mut self, tube_name: &str, key: &str) {
        if let Some(tube) = self.tubes.get_mut(tube_name)
            && tube.idempotency_cooldowns.remove(key).is_some()
        {
            let cost = Self::tombstone_memory_cost(key);
            self.total_job_bytes = self.total_job_bytes.saturating_sub(cost);
        }
    }

    /// Drain every tombstone from a tube, updating accounting.
    fn drain_tombstones_in_tube(&mut self, tube_name: &str) {
        if let Some(tube) = self.tubes.get_mut(tube_name) {
            let drained: Vec<String> = tube.idempotency_cooldowns.drain().map(|(k, _)| k).collect();
            for k in drained {
                let cost = Self::tombstone_memory_cost(&k);
                self.total_job_bytes = self.total_job_bytes.saturating_sub(cost);
            }
        }
    }

    fn unregister_conn(&mut self, conn_id: u64) {
        // Remove from waiting lists first: remove_waiter_at reads conn.watched
        // to decrement per-tube waiting_ct, so the conn must still be in
        // self.conns when it runs.
        self.remove_waiter(conn_id);
        // Release all reserved jobs back to ready
        if let Some(conn) = self.conns.remove(&conn_id) {

            // Decrement tube counters
            if let Some(t) = self.tubes.get_mut(&conn.use_tube) {
                t.using_ct = t.using_ct.saturating_sub(1);
            }
            for w in &conn.watched {
                if let Some(t) = self.tubes.get_mut(&w.name) {
                    t.watching_ct = t.watching_ct.saturating_sub(1);
                }
            }

            // Re-enqueue reserved jobs
            for job_id in conn.reserved_jobs {
                self.release_concurrency_key(job_id);
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.state = JobState::Ready;
                    job.reserver_id = None;
                    job.deadline_at = None;
                    let tube_name = job.tube_name.clone();
                    let key = job.ready_key();
                    let id = job.id;
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.ready.insert(key, id);
                        self.ready_ct += 1;
                        self.stats.reserved_ct = self.stats.reserved_ct.saturating_sub(1);
                        tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(1);
                        if key.0 < URGENT_THRESHOLD {
                            self.stats.urgent_ct += 1;
                            tube.stat.urgent_ct += 1;
                        }
                    }
                }
            }

            self.process_queue();
        }
    }

    fn ensure_tube(&mut self, name: &str) -> bool {
        if !self.tubes.contains_key(name) {
            self.tubes.insert(name.to_string(), Tube::new(name));
        }
        true
    }

    /// Check if a job's concurrency key already has an active reservation.
    fn is_concurrency_blocked(&self, job_id: u64) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|j| j.concurrency_key.as_ref())
            .map(|(key, _limit)| {
                let count = self.concurrency_keys.get(key).copied().unwrap_or(0);
                let limit = self.concurrency_limits.get(key).copied().unwrap_or(1);
                count >= limit
            })
            .unwrap_or(false)
    }

    /// Increment the concurrency counter for a job's key and register limit.
    fn acquire_concurrency_key(&mut self, job_id: u64) {
        if let Some((key, limit)) = self
            .jobs
            .get(&job_id)
            .and_then(|j| j.concurrency_key.clone())
        {
            *self.concurrency_keys.entry(key.clone()).or_insert(0) += 1;
            // Use max of existing and new limit (safest — never blocks more than intended)
            let entry = self.concurrency_limits.entry(key).or_insert(0);
            *entry = (*entry).max(limit);
        }
    }

    /// Decrement and clean up the concurrency counter for a job's key.
    fn release_concurrency_key(&mut self, job_id: u64) {
        if let Some((key, _limit)) = self
            .jobs
            .get(&job_id)
            .and_then(|j| j.concurrency_key.clone())
            && let Some(count) = self.concurrency_keys.get_mut(&key)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.concurrency_keys.remove(&key);
                self.concurrency_limits.remove(&key);
            }
        }
    }

    /// Find the best unblocked ready job from a tube. The slow-path walk is
    /// capped at FIND_UNBLOCKED_MAX_VISITS so a heavily con-key-blocked heap
    /// doesn't melt CPU; a real unblocked job deeper than the cap is picked
    /// up on the next event-driven process_queue.
    fn find_best_unblocked_ready(&self, tube: &Tube) -> Option<((u32, u64), u64)> {
        let entries = tube.ready.entries();
        let top = *entries.first()?;
        if !self.is_concurrency_blocked(top.1) {
            return Some(top);
        }

        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        // (priority+id key, job id, heap index) — Reverse turns max-heap into min-heap.
        type Pending = Reverse<((u32, u64), u64, usize)>;
        let mut pending: BinaryHeap<Pending> = BinaryHeap::new();
        pending.push(Reverse((top.0, top.1, 0)));
        let mut visited = 0usize;
        while let Some(Reverse((key, jid, idx))) = pending.pop() {
            visited += 1;
            if !self.is_concurrency_blocked(jid) {
                return Some((key, jid));
            }
            if visited >= FIND_UNBLOCKED_MAX_VISITS {
                return None;
            }
            for child in [idx * 2 + 1, idx * 2 + 2] {
                if let Some(&(k, id)) = entries.get(child) {
                    pending.push(Reverse((k, id, child)));
                }
            }
        }
        None
    }

    fn handle_command(&mut self, conn_id: u64, cmd: Command, body: Option<Vec<u8>>) -> Response {
        // Auto-register connection if not known
        if let std::collections::hash_map::Entry::Vacant(e) = self.conns.entry(conn_id) {
            e.insert(ConnState::new(conn_id));
            self.stats.total_connections += 1;
            self.ensure_tube("default");
            if let Some(t) = self.tubes.get_mut("default") {
                t.watching_ct += 1;
                t.using_ct += 1;
            }
        }

        match cmd {
            Command::Put {
                pri,
                delay,
                ttr,
                bytes,
                idempotency_key,
                group,
                after_group,
                concurrency_key,
            } => {
                self.stats.op_ct[OP_PUT] += 1;
                self.cmd_put(
                    conn_id,
                    pri,
                    delay,
                    ttr,
                    bytes,
                    body,
                    idempotency_key,
                    group,
                    after_group,
                    concurrency_key,
                )
            }
            Command::Use { tube } => {
                self.stats.op_ct[OP_USE] += 1;
                self.cmd_use(conn_id, &tube)
            }
            Command::Reserve => {
                self.stats.op_ct[OP_RESERVE] += 1;
                self.cmd_reserve(conn_id, None)
            }
            Command::ReserveWithTimeout { timeout } => {
                self.stats.op_ct[OP_RESERVE_TIMEOUT] += 1;
                self.cmd_reserve(conn_id, Some(timeout))
            }
            Command::ReserveJob { id } => {
                self.stats.op_ct[OP_RESERVE] += 1;
                self.cmd_reserve_job(conn_id, id)
            }
            Command::ReserveMode { mode } => {
                self.stats.op_ct[OP_RESERVE_MODE] += 1;
                self.cmd_reserve_mode(conn_id, &mode)
            }
            Command::ReserveBatch { count } => {
                self.stats.op_ct[OP_RESERVE] += 1;
                self.cmd_reserve_batch(conn_id, count)
            }
            Command::Delete { id } => {
                self.stats.op_ct[OP_DELETE] += 1;
                self.cmd_delete(conn_id, id)
            }
            Command::DeleteBatch { ids } => {
                self.stats.op_ct[OP_DELETE] += ids.len() as u64;
                self.cmd_delete_batch(conn_id, ids)
            }
            Command::Release { id, pri, delay } => {
                self.stats.op_ct[OP_RELEASE] += 1;
                self.cmd_release(conn_id, id, pri, delay)
            }
            Command::Bury { id, pri } => {
                self.stats.op_ct[OP_BURY] += 1;
                self.cmd_bury(conn_id, id, pri)
            }
            Command::Touch { id } => {
                self.stats.op_ct[OP_TOUCH] += 1;
                self.cmd_touch(conn_id, id)
            }
            Command::Watch { tube, weight } => {
                self.stats.op_ct[OP_WATCH] += 1;
                self.cmd_watch(conn_id, &tube, weight)
            }
            Command::Ignore { tube } => {
                self.stats.op_ct[OP_IGNORE] += 1;
                self.cmd_ignore(conn_id, &tube)
            }
            Command::Peek { id } => {
                self.stats.op_ct[OP_PEEKJOB] += 1;
                self.cmd_peek(id)
            }
            Command::PeekReady => {
                self.stats.op_ct[OP_PEEK_READY] += 1;
                self.cmd_peek_ready(conn_id)
            }
            Command::PeekDelayed => {
                self.stats.op_ct[OP_PEEK_DELAYED] += 1;
                self.cmd_peek_delayed(conn_id)
            }
            Command::PeekBuried => {
                self.stats.op_ct[OP_PEEK_BURIED] += 1;
                self.cmd_peek_buried(conn_id)
            }
            Command::PeekReserved => {
                self.stats.op_ct[OP_PEEK_RESERVED] += 1;
                self.cmd_peek_reserved(conn_id)
            }
            Command::Kick { bound } => {
                self.stats.op_ct[OP_KICK] += 1;
                self.cmd_kick(conn_id, bound)
            }
            Command::KickJob { id } => {
                self.stats.op_ct[OP_KICK] += 1;
                self.cmd_kick_job(id)
            }
            Command::StatsJob { id } => {
                self.stats.op_ct[OP_STATSJOB] += 1;
                self.cmd_stats_job(id)
            }
            Command::StatsTube { tube } => {
                self.stats.op_ct[OP_STATS_TUBE] += 1;
                self.cmd_stats_tube(&tube)
            }
            Command::StatsGroup { group } => {
                self.stats.op_ct[OP_STATS] += 1;
                self.cmd_stats_group(&group)
            }
            Command::Stats => {
                self.stats.op_ct[OP_STATS] += 1;
                self.cmd_stats()
            }
            Command::ListTubes => {
                self.stats.op_ct[OP_LIST_TUBES] += 1;
                self.cmd_list_tubes()
            }
            Command::ListTubeUsed => {
                self.stats.op_ct[OP_LIST_TUBE_USED] += 1;
                self.cmd_list_tube_used(conn_id)
            }
            Command::ListTubesWatched => {
                self.stats.op_ct[OP_LIST_TUBES_WATCHED] += 1;
                self.cmd_list_tubes_watched(conn_id)
            }
            Command::PauseTube { tube, delay } => {
                self.stats.op_ct[OP_PAUSE_TUBE] += 1;
                self.cmd_pause_tube(&tube, delay)
            }
            Command::FlushTube { tube } => self.cmd_flush_tube(&tube),
            Command::Drain => {
                self.drain_mode = true;
                tracing::info!("entering drain mode (requested by connection {})", conn_id);
                Response::Draining
            }
            Command::Undrain => {
                self.drain_mode = false;
                tracing::info!("exiting drain mode (requested by connection {})", conn_id);
                Response::NotDraining
            }
            Command::Quit => Response::Deleted, // handled at connection level
        }
    }

    // --- Command implementations ---

    #[allow(clippy::too_many_arguments)]
    fn cmd_put(
        &mut self,
        conn_id: u64,
        pri: u32,
        delay: u32,
        ttr: u32,
        _bytes: u32,
        body: Option<Vec<u8>>,
        idempotency_key: Option<(String, u32)>,
        group: Option<String>,
        after_group: Option<String>,
        concurrency_key: Option<(String, u32)>,
    ) -> Response {
        if self.drain_mode {
            return Response::Draining;
        }

        let body = match body {
            Some(b) => b,
            None => return Response::InternalError,
        };

        // Memory budget: reject new work before allocating anything. We
        // account for the job itself plus any idempotency tombstone that
        // would be created if the job is later deleted with a non-zero idp
        // TTL — that tombstone is part of the same budget, and admitting a
        // put we can't later cooldown would be dishonest. Release/bury/kick
        // never pass through this check because they don't add bytes.
        //
        // When persistence is on, body bytes leave RAM and live in the
        // external body store, so they don't count toward the in-memory
        // budget. The disk side is enforced separately just below.
        let tombstone_cost = idempotency_key
            .as_ref()
            .filter(|(_, ttl)| *ttl > 0)
            .map(|(k, _)| k.len() as u64 + TOMBSTONE_OVERHEAD_BYTES)
            .unwrap_or(0);
        let body_in_ram = if self.body_store.is_some() {
            0
        } else {
            body.len() as u64
        };
        let job_cost = body_in_ram + JOB_OVERHEAD_BYTES;
        if self.memory_limit_exceeded(job_cost + tombstone_cost) {
            return Response::OutOfMemory;
        }

        // Storage budget: combined WAL + TOAST disk usage must stay under
        // `--max-storage-bytes`, with one WAL segment's worth of headroom
        // reserved so deletes/releases can always be journalled — that
        // reserve is what lets `OUT_OF_STORAGE` be recoverable rather than
        // a deadlock. State-change records bypass this check entirely.
        if self.storage_limit_exceeded(body.len() as u64) {
            return Response::OutOfStorage;
        }

        // Mark connection as producer
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.set_producer();
        }

        // Minimum TTR is 1 second
        let ttr = ttr.max(1);

        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(|| "default".to_string());

        self.ensure_tube(&tube_name);

        // Idempotency dedup: if key already exists for a live job, return original ID + state.
        // If the new put has a higher priority (lower number), upgrade the existing job's priority.
        if let Some(ref key_tuple) = idempotency_key
            && let Some(&existing_id) = self
                .tubes
                .get(&tube_name)
                .and_then(|t| t.idempotency_keys.get(&key_tuple.0))
        {
            let upgraded_pri = if let Some(existing_job) = self.jobs.get_mut(&existing_id)
                && pri < existing_job.priority
            {
                let old_pri = existing_job.priority;
                let state = existing_job.state;
                let delay = existing_job.delay;
                existing_job.priority = pri;

                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    // If job is Ready, re-sort the ready heap
                    if state == JobState::Ready {
                        tube.ready.remove_by_id(existing_id);
                        tube.ready.insert((pri, existing_id), existing_id);
                    }
                    // Update urgent stats if crossing threshold
                    if pri < URGENT_THRESHOLD && old_pri >= URGENT_THRESHOLD {
                        self.stats.urgent_ct += 1;
                        tube.stat.urgent_ct += 1;
                    }
                }

                self.wal_write_state_change(
                    existing_id,
                    Some(state),
                    pri,
                    delay,
                    0,
                    StateChangeReason::None,
                );

                Some(pri)
            } else {
                None
            };

            let state_str = self
                .jobs
                .get(&existing_id)
                .expect("job must exist — just looked up by idempotency key")
                .state
                .as_protocol_str();
            return Response::InsertedDup(existing_id, state_str, upgraded_pri);
        }

        // Idempotency cooldown dedup: if key is in cooldown period, return original ID (job is deleted)
        if let Some(ref key_tuple) = idempotency_key {
            let cooldown = self
                .tubes
                .get(&tube_name)
                .and_then(|t| t.idempotency_cooldowns.get(&key_tuple.0).copied());
            if let Some((original_id, expiry)) = cooldown {
                if SystemTime::now() < expiry {
                    return Response::InsertedDup(original_id, "DELETED", None);
                } else {
                    self.remove_tombstone(&tube_name, &key_tuple.0);
                }
            }
        }

        // WAL: check space reservation
        if let Some(wal) = &self.wal {
            // Estimate record size for reservation check
            let idp_key_str = idempotency_key.as_ref().map(|(k, _)| k.clone());
            let est_size = crate::wal::estimate_full_job_size_raw(
                &tube_name,
                &idp_key_str,
                &group,
                &after_group,
                &concurrency_key,
            );
            if !wal.reserve_put(est_size) {
                // The record (mostly metadata after Phase 3 — bodies live in
                // TOAST) is bigger than a single WAL segment can hold, so it
                // can never be journalled. Return JOB_TOO_BIG: this is the
                // existing semantic for "this individual record won't fit",
                // and the operator's only fix is to raise the segment size
                // or shrink whichever oversize field tipped the estimate.
                tracing::warn!(
                    "WAL: record ({est_size} bytes) exceeds segment size ({}) — JOB_TOO_BIG",
                    wal.max_file_size(),
                );
                return Response::JobTooBig;
            }
        }

        let id = self.next_job_id;
        self.next_job_id += 1;

        // When the body store is in use, write bytes to TOAST first and
        // hand the job an `External(BodyId)` reference. The WAL's v5
        // serializer assumes External, and the sync coordinator inside
        // `Wal` will fsync TOAST before the WAL record landing this id.
        let body_ref = if let Some(bs) = self.body_store.as_ref() {
            match bs.write_body(&body) {
                Ok(body_id) => BodyRef::External(body_id),
                Err(e) => {
                    tracing::error!("body store write failed: {}", e);
                    return Response::InternalError;
                }
            }
        } else {
            BodyRef::Inline(body)
        };

        let mut job = Job::new(
            id,
            pri,
            Duration::from_secs(delay as u64),
            Duration::from_secs(ttr as u64),
            Vec::new(),
            tube_name.clone(),
        );
        job.body = body_ref;

        // Set extension fields before inserting
        job.idempotency_key = idempotency_key;
        job.group = group;
        job.after_group = after_group;
        job.concurrency_key = concurrency_key;

        // Register concurrency limit
        if let Some((ref key, limit)) = job.concurrency_key {
            let entry = self.concurrency_limits.entry(key.clone()).or_insert(0);
            *entry = (*entry).max(limit);
        }

        // Register idempotency key in tube index
        if let Some(ref key_tuple) = job.idempotency_key {
            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                tube.idempotency_keys.insert(key_tuple.0.clone(), id);
            }
        }

        // Track group membership
        if let Some(ref grp) = job.group {
            let gs = self
                .groups
                .entry(grp.clone())
                .or_insert_with(GroupState::new);
            gs.pending += 1;
        }

        // Check if this is an after-group job that should be held
        let hold_for_group = if let Some(ref ag) = job.after_group {
            let gs = self
                .groups
                .entry(ag.clone())
                .or_insert_with(GroupState::new);
            !gs.is_complete()
        } else {
            false
        };

        // Enqueue
        if delay > 0 {
            let deadline = Instant::now() + Duration::from_secs(delay as u64);
            job.state = JobState::Delayed;
            job.deadline_at = Some(deadline);
            self.insert_job(id, job);
            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                tube.delay.insert((deadline, id), id);
            }
        } else if hold_for_group {
            // Hold this after-job: mark as delayed with no deadline (held indefinitely)
            job.state = JobState::Delayed;
            job.deadline_at = None;
            let after_group_name = job.after_group.clone();
            self.insert_job(id, job);
            // Add to group's waiting list (will be promoted when group completes)
            if let Some(ref ag) = after_group_name
                && let Some(gs) = self.groups.get_mut(ag)
            {
                gs.waiting_jobs.push(id);
            }
        } else {
            let key = job.ready_key();
            self.insert_job(id, job);
            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                tube.ready.insert(key, id);
            }
            self.ready_ct += 1;
            if pri < URGENT_THRESHOLD {
                self.stats.urgent_ct += 1;
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.stat.urgent_ct += 1;
                }
            }
        }

        self.stats.total_jobs_ct += 1;
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.stat.total_jobs_ct += 1;
        }

        // WAL: write put record
        self.wal_write_put(id);

        if !self.waiters.is_empty() {
            self.process_queue();
        }

        Response::Inserted(id)
    }

    fn cmd_use(&mut self, conn_id: u64, tube: &str) -> Response {
        self.ensure_tube(tube);
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            // Decrement old tube
            if let Some(old) = self.tubes.get_mut(&conn.use_tube) {
                old.using_ct = old.using_ct.saturating_sub(1);
            }
            conn.use_tube = tube.to_string();
        }
        if let Some(t) = self.tubes.get_mut(tube) {
            t.using_ct += 1;
        }
        Response::Using(tube.to_string())
    }

    /// Find the next ready job for a connection, respecting reserve mode.
    fn find_next_ready_job(&mut self, conn_id: u64) -> Option<u64> {
        let reserve_mode = self
            .conns
            .get(&conn_id)
            .map(|c| c.reserve_mode)
            .unwrap_or(ReserveMode::Fifo);

        match reserve_mode {
            ReserveMode::Weighted => self
                .select_weighted_job(conn_id)
                .or_else(|| self.find_ready_job_for_conn(conn_id)),
            ReserveMode::WeightedFair => self
                .select_weighted_fair_job(conn_id)
                .or_else(|| self.find_ready_job_for_conn(conn_id)),
            ReserveMode::Fifo => self.find_ready_job_for_conn(conn_id),
        }
    }

    fn cmd_reserve(&mut self, conn_id: u64, timeout: Option<u32>) -> Response {
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.set_worker();
        }

        // Check deadline_soon
        if self.conn_deadline_soon(conn_id, Instant::now()) && !self.conn_has_ready_job(conn_id) {
            return Response::DeadlineSoon;
        }

        if let Some(job_id) = self.find_next_ready_job(conn_id) {
            return self.do_reserve(conn_id, job_id);
        }

        // No job available -- this will need to wait.
        // Return TimedOut for timeout=0, or let the connection task handle
        // waiting via the waiter mechanism.
        if timeout == Some(0) {
            return Response::TimedOut;
        }

        // For blocking reserve, we return a sentinel that tells the connection
        // task to use the waiting mechanism.
        Response::TimedOut // Will be overridden by the wait mechanism
    }

    fn cmd_reserve_job(&mut self, conn_id: u64, id: u64) -> Response {
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.set_worker();
        }

        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        if job.state == JobState::Reserved {
            return Response::NotFound;
        }

        // Check concurrency constraint
        if self.is_concurrency_blocked(id) {
            return Response::NotFound;
        }

        // Check after_group dependency
        let hold_for_group = job
            .after_group
            .as_ref()
            .and_then(|ag| self.groups.get(ag))
            .map(|gs| !gs.is_complete())
            .unwrap_or(false);
        if hold_for_group {
            return Response::NotFound;
        }

        let state = job.state;
        let tube_name = job.tube_name.clone();

        // Remove from current state
        match state {
            JobState::Ready => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.ready.remove_by_id(id);
                }
                self.ready_ct = self.ready_ct.saturating_sub(1);
                if let Some(j) = self.jobs.get(&id)
                    && j.priority < URGENT_THRESHOLD
                {
                    self.stats.urgent_ct = self.stats.urgent_ct.saturating_sub(1);
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.stat.urgent_ct = tube.stat.urgent_ct.saturating_sub(1);
                    }
                }
            }
            JobState::Buried => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.buried.retain(|&jid| jid != id);
                    self.stats.buried_ct = self.stats.buried_ct.saturating_sub(1);
                    tube.stat.buried_ct = tube.stat.buried_ct.saturating_sub(1);
                }
            }
            JobState::Delayed => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.delay.remove_by_id(id);
                }
            }
            _ => return Response::NotFound,
        }

        self.do_reserve_inner(conn_id, id)
    }

    fn do_reserve(&mut self, conn_id: u64, job_id: u64) -> Response {
        // Remove from ready heap
        let (tube_name, is_urgent) = match self.jobs.get(&job_id) {
            Some(j) => (j.tube_name.clone(), j.priority < URGENT_THRESHOLD),
            None => return Response::NotFound,
        };
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.ready.remove_by_id(job_id);
            if is_urgent {
                tube.stat.urgent_ct = tube.stat.urgent_ct.saturating_sub(1);
            }
        }
        self.ready_ct = self.ready_ct.saturating_sub(1);
        if is_urgent {
            self.stats.urgent_ct = self.stats.urgent_ct.saturating_sub(1);
        }

        self.do_reserve_inner(conn_id, job_id)
    }

    fn do_reserve_inner(&mut self, conn_id: u64, job_id: u64) -> Response {
        let now = Instant::now();
        // Materialise the body before mutating job state — `fetch_body` may
        // hit the body store and needs its own immutable borrow of self.
        let body = match self.jobs.get(&job_id) {
            Some(job) => match self.fetch_body(job) {
                Some(b) => b,
                None => return Response::InternalError,
            },
            None => return Response::NotFound,
        };
        let (tube_name, created_at) = match self.jobs.get_mut(&job_id) {
            Some(job) => {
                let tube_name = job.tube_name.clone();
                let created_at = job.created_at;
                job.state = JobState::Reserved;
                job.reserver_id = Some(conn_id);
                job.reserved_at = Some(now);
                job.deadline_at = Some(now + job.ttr);
                job.reserve_ct += 1;
                (tube_name, created_at)
            }
            None => return Response::NotFound,
        };
        self.acquire_concurrency_key(job_id);
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.stat.reserved_ct += 1;
            tube.stat.total_reserve_ct += 1;

            let queue_secs = now.duration_since(created_at).as_secs_f64();
            TubeStats::record_timing(
                &mut tube.stat.queue_time_ewma,
                &mut tube.stat.queue_time_samples,
                &mut tube.stat.queue_time_min,
                &mut tube.stat.queue_time_max,
                queue_secs,
                EWMA_ALPHA,
            );
        }
        self.stats.reserved_ct += 1;

        let pri = self.job_pri(job_id);
        self.wal_write_state_change(
            job_id,
            Some(JobState::Reserved),
            pri,
            Duration::ZERO,
            0,
            StateChangeReason::Reserve,
        );

        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.reserved_jobs.push(job_id);
        }

        Response::Reserved { id: job_id, body }
    }

    fn cmd_reserve_mode(&mut self, conn_id: u64, mode: &str) -> Response {
        let reserve_mode = match mode {
            "fifo" => ReserveMode::Fifo,
            "weighted" => ReserveMode::Weighted,
            "weighted-fair" => ReserveMode::WeightedFair,
            _ => return Response::BadFormat,
        };
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.reserve_mode = reserve_mode;
        }
        Response::Using(mode.to_string())
    }

    fn cmd_reserve_batch(&mut self, conn_id: u64, count: u32) -> Response {
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.set_worker();
        }

        let mut collected: Vec<(u64, Vec<u8>)> = Vec::new();

        for _ in 0..count {
            let Some(jid) = self.find_next_ready_job(conn_id) else {
                break;
            };
            let resp = self.do_reserve(conn_id, jid);
            if let Response::Reserved { id, body } = resp {
                collected.push((id, body));
            } else {
                break;
            }
        }

        Response::ReservedBatch(collected)
    }

    fn cmd_delete(&mut self, conn_id: u64, id: u64) -> Response {
        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        let state = job.state;
        let tube_name = job.tube_name.clone();
        let pri = job.priority;
        let reserved_at = job.reserved_at;
        let idempotency_key = job.idempotency_key.clone();
        let group_name = job.group.clone();
        let after_group_name = job.after_group.clone();
        let has_concurrency_key = job.concurrency_key.is_some();

        match state {
            JobState::Reserved => {
                // Must be reserved by this connection
                if job.reserver_id != Some(conn_id) {
                    return Response::NotFound;
                }
                if has_concurrency_key {
                    self.release_concurrency_key(id);
                }
                if let Some(conn) = self.conns.get_mut(&conn_id) {
                    if let Some(pos) = conn.reserved_jobs.iter().position(|&jid| jid == id) {
                        conn.reserved_jobs.swap_remove(pos);
                    }
                }
                self.stats.reserved_ct = self.stats.reserved_ct.saturating_sub(1);
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(1);
                }
            }
            JobState::Ready => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.ready.remove_by_id(id);
                }
                self.ready_ct = self.ready_ct.saturating_sub(1);
                if pri < URGENT_THRESHOLD {
                    self.stats.urgent_ct = self.stats.urgent_ct.saturating_sub(1);
                    if let Some(tube) = self.tubes.get_mut(&tube_name) {
                        tube.stat.urgent_ct = tube.stat.urgent_ct.saturating_sub(1);
                    }
                }
            }
            JobState::Buried => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.buried.retain(|&jid| jid != id);
                    tube.stat.buried_ct = tube.stat.buried_ct.saturating_sub(1);
                }
                self.stats.buried_ct = self.stats.buried_ct.saturating_sub(1);
            }
            JobState::Delayed => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.delay.remove_by_id(id);
                }
            }
        }

        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.stat.total_delete_ct += 1;

            if state == JobState::Reserved
                && let Some(ra) = reserved_at
            {
                let secs = Instant::now().duration_since(ra).as_secs_f64();

                TubeStats::record_timing(
                    &mut tube.stat.processing_time_ewma,
                    &mut tube.stat.processing_time_samples,
                    &mut tube.stat.processing_time_min,
                    &mut tube.stat.processing_time_max,
                    secs,
                    EWMA_ALPHA,
                );

                if secs < FAST_THRESHOLD {
                    TubeStats::update_ewma(
                        &mut tube.stat.processing_time_ewma_fast,
                        &mut tube.stat.processing_time_samples_fast,
                        secs,
                        EWMA_ALPHA,
                    );
                    tube.stat.record_fast_sample(secs);
                } else {
                    TubeStats::update_ewma(
                        &mut tube.stat.processing_time_ewma_slow,
                        &mut tube.stat.processing_time_samples_slow,
                        secs,
                        EWMA_ALPHA,
                    );
                    tube.stat.record_slow_sample(secs);
                }
            }
        }
        self.stats.total_delete_ct += 1;

        // Remove idempotency key from tube index (with optional cooldown)
        let mut expiry_epoch_secs: u64 = 0;
        if let Some(ref key_tuple) = idempotency_key {
            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                tube.idempotency_keys.remove(&key_tuple.0);
            }
            if key_tuple.1 > 0 {
                let expires_at = SystemTime::now() + Duration::from_secs(key_tuple.1 as u64);
                expiry_epoch_secs = expires_at
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                self.insert_tombstone(&tube_name, key_tuple.0.clone(), id, expires_at);
            }
        }
        if let Some(ref grp) = group_name
            && let Some(gs) = self.groups.get_mut(grp)
        {
            gs.pending = gs.pending.saturating_sub(1);
            if state == JobState::Buried {
                gs.buried = gs.buried.saturating_sub(1);
            }
        }
        if let Some(ref ag) = after_group_name
            && let Some(gs) = self.groups.get_mut(ag)
        {
            gs.remove_waiting_job(id);
        }

        // WAL: write delete state change (with tombstone expiry if applicable)
        self.wal_write_state_change(
            id,
            None,
            0,
            Duration::ZERO,
            expiry_epoch_secs,
            StateChangeReason::None,
        );

        self.delete_job(id);

        // Check if any group completed and promote waiting after-jobs
        if let Some(ref grp) = group_name {
            self.check_group_completion(grp);
        }

        // Freeing a concurrency slot can unblock a parked waiter; mirror cmd_release.
        if has_concurrency_key && !self.waiters.is_empty() {
            self.process_queue();
        }

        Response::Deleted
    }

    fn cmd_delete_batch(&mut self, conn_id: u64, ids: Vec<u64>) -> Response {
        let mut deleted: u32 = 0;
        let mut not_found: u32 = 0;
        for id in ids {
            match self.cmd_delete(conn_id, id) {
                Response::Deleted => deleted += 1,
                _ => not_found += 1,
            }
        }
        Response::DeletedBatch { deleted, not_found }
    }

    fn cmd_flush_tube(&mut self, tube_name: &str) -> Response {
        let tube = match self.tubes.get(tube_name) {
            Some(t) => t,
            None => return Response::NotFound,
        };

        // Collect all job IDs from ready, delay, and buried queues
        let mut job_ids: Vec<u64> = Vec::new();
        job_ids.extend(tube.ready.ids());
        job_ids.extend(tube.delay.ids());
        job_ids.extend(tube.buried.iter());

        // Find reserved jobs belonging to this tube
        let reserved_ids: Vec<u64> = self
            .jobs
            .values()
            .filter(|j| j.tube_name == tube_name && j.state == JobState::Reserved)
            .map(|j| j.id)
            .collect();
        job_ids.extend(&reserved_ids);

        let count = job_ids.len() as u32;
        if count == 0 {
            // Still clear cooldowns even if no jobs
            self.drain_tombstones_in_tube(tube_name);
            return Response::Flushed(0);
        }

        // Track stats adjustments
        let mut ready_removed = 0u64;
        let mut urgent_removed = 0u64;
        let mut buried_removed = 0u64;
        let mut reserved_removed = 0u64;
        // Categorize jobs by state for stats
        for &id in &job_ids {
            if let Some(job) = self.jobs.get(&id) {
                match job.state {
                    JobState::Ready => {
                        ready_removed += 1;
                        if job.priority < URGENT_THRESHOLD {
                            urgent_removed += 1;
                        }
                    }
                    JobState::Reserved => reserved_removed += 1,
                    JobState::Buried => buried_removed += 1,
                    JobState::Delayed => {}
                }
            }
        }

        // Remove reserved jobs from owning connections and release concurrency keys
        for &id in &reserved_ids {
            self.release_concurrency_key(id);
            if let Some(job) = self.jobs.get(&id)
                && let Some(reserver_id) = job.reserver_id
                && let Some(conn) = self.conns.get_mut(&reserver_id)
            {
                conn.reserved_jobs.retain(|&jid| jid != id);
            }
        }

        // Clear tombstones with accounting, then clear everything else.
        self.drain_tombstones_in_tube(tube_name);
        let tube = self.tubes.get_mut(tube_name).unwrap();
        tube.ready.clear();
        tube.delay.clear();
        tube.buried.clear();
        tube.idempotency_keys.clear();

        // Update tube stats
        tube.stat.total_delete_ct += count as u64;
        tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(reserved_removed);
        tube.stat.buried_ct = tube.stat.buried_ct.saturating_sub(buried_removed);
        tube.stat.urgent_ct = tube.stat.urgent_ct.saturating_sub(urgent_removed);

        // Update global stats
        self.stats.total_delete_ct += count as u64;
        self.ready_ct = self.ready_ct.saturating_sub(ready_removed);
        self.stats.urgent_ct = self.stats.urgent_ct.saturating_sub(urgent_removed);
        self.stats.buried_ct = self.stats.buried_ct.saturating_sub(buried_removed);
        self.stats.reserved_ct = self.stats.reserved_ct.saturating_sub(reserved_removed);

        // Group tracking: decrement pending/buried counts for flushed jobs
        let mut affected_groups: Vec<String> = Vec::new();
        for &id in &job_ids {
            if let Some(job) = self.jobs.get(&id) {
                if let Some(ref grp) = job.group
                    && let Some(gs) = self.groups.get_mut(grp)
                {
                    gs.pending = gs.pending.saturating_sub(1);
                    if job.state == JobState::Buried {
                        gs.buried = gs.buried.saturating_sub(1);
                    }
                    if !affected_groups.contains(grp) {
                        affected_groups.push(grp.clone());
                    }
                }
                if let Some(ref ag) = job.after_group
                    && let Some(gs) = self.groups.get_mut(ag)
                {
                    gs.remove_waiting_job(id);
                }
            }
        }

        // WAL: write delete for each job, then drop them from the in-memory
        // map. Body releases are batched into a single BodyStore call so
        // flush-tube doesn't acquire/release the BodyStore mutex per job.
        //
        // Failure note: if `wal_write_state_change` fails mid-batch, the
        // helper sets `self.wal = None` and the rest of the deletes proceed
        // against memory only. Those deletes won't survive a crash. This
        // is acceptable for a destructive command — flush-tube semantically
        // throws data away, and the alternative (abort the flush partway)
        // leaves the operator no clean recovery either.
        let mut external_bodies: Vec<BodyId> = Vec::new();
        for &id in &job_ids {
            self.wal_write_state_change(id, None, 0, Duration::ZERO, 0, StateChangeReason::None);
            if let Some(job) = self.take_job(id)
                && let BodyRef::External(body_id) = job.body
            {
                external_bodies.push(body_id);
            }
        }
        if let Some(bs) = self.body_store.as_ref() {
            bs.delete_many(&external_bodies);
        }

        // Check if any affected groups completed
        for grp in &affected_groups {
            self.check_group_completion(grp);
        }

        Response::Flushed(count)
    }

    fn cmd_release(&mut self, conn_id: u64, id: u64, pri: u32, delay: u32) -> Response {
        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        if job.state != JobState::Reserved || job.reserver_id != Some(conn_id) {
            return Response::NotFound;
        }

        let tube_name = job.tube_name.clone();

        // Remove from reserved
        self.release_concurrency_key(id);
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.reserved_jobs.retain(|&jid| jid != id);
        }
        self.stats.reserved_ct = self.stats.reserved_ct.saturating_sub(1);
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(1);
        }

        // Update job
        if let Some(job) = self.jobs.get_mut(&id) {
            job.priority = pri;
            job.delay = Duration::from_secs(delay as u64);
            job.release_ct += 1;
            job.reserver_id = None;
            job.reserved_at = None;
        }

        // Enqueue
        if delay > 0 {
            let deadline = Instant::now() + Duration::from_secs(delay as u64);
            if let Some(job) = self.jobs.get_mut(&id) {
                job.state = JobState::Delayed;
                job.deadline_at = Some(deadline);
            }
            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                tube.delay.insert((deadline, id), id);
            }
        } else {
            if let Some(job) = self.jobs.get_mut(&id) {
                job.state = JobState::Ready;
                job.deadline_at = None;
                let key = job.ready_key();
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.ready.insert(key, id);
                }
            }
            self.ready_ct += 1;
            if pri < URGENT_THRESHOLD {
                self.stats.urgent_ct += 1;
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.stat.urgent_ct += 1;
                }
            }
        }

        // WAL: write release state change
        let wal_state = if delay > 0 {
            JobState::Delayed
        } else {
            JobState::Ready
        };
        self.wal_write_state_change(
            id,
            Some(wal_state),
            pri,
            Duration::from_secs(delay as u64),
            0,
            StateChangeReason::Release,
        );

        self.process_queue();
        Response::Released
    }

    fn cmd_bury(&mut self, conn_id: u64, id: u64, pri: u32) -> Response {
        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        if job.state != JobState::Reserved || job.reserver_id != Some(conn_id) {
            return Response::NotFound;
        }

        let tube_name = job.tube_name.clone();

        // Remove from reserved
        self.release_concurrency_key(id);
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.reserved_jobs.retain(|&jid| jid != id);
        }
        self.stats.reserved_ct = self.stats.reserved_ct.saturating_sub(1);
        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.stat.reserved_ct = tube.stat.reserved_ct.saturating_sub(1);
        }

        if let Some(job) = self.jobs.get_mut(&id) {
            job.priority = pri;
            job.state = JobState::Buried;
            job.reserver_id = None;
            job.reserved_at = None;
            job.deadline_at = None;
            job.bury_ct += 1;
        }

        if let Some(tube) = self.tubes.get_mut(&tube_name) {
            tube.buried.push_back(id);
            tube.stat.buried_ct += 1;
            tube.stat.total_bury_ct += 1;
        }
        self.stats.buried_ct += 1;

        // Group tracking: buried jobs block group completion
        if let Some(job) = self.jobs.get(&id)
            && let Some(ref grp) = job.group
            && let Some(gs) = self.groups.get_mut(grp)
        {
            gs.buried += 1;
        }

        // WAL: write bury state change
        self.wal_write_state_change(
            id,
            Some(JobState::Buried),
            pri,
            Duration::ZERO,
            0,
            StateChangeReason::Bury,
        );

        Response::Buried
    }

    fn cmd_touch(&mut self, conn_id: u64, id: u64) -> Response {
        let job = match self.jobs.get_mut(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        if job.state != JobState::Reserved || job.reserver_id != Some(conn_id) {
            return Response::NotFound;
        }

        job.deadline_at = Some(Instant::now() + job.ttr);
        Response::Touched
    }

    fn cmd_watch(&mut self, conn_id: u64, tube: &str, weight: u32) -> Response {
        self.ensure_tube(tube);

        if let Some(conn) = self.conns.get_mut(&conn_id) {
            // Update weight if already watching, otherwise add
            if let Some(w) = conn.watched.iter_mut().find(|w| w.name == tube) {
                w.weight = weight;
            } else {
                conn.watched.push(WatchedTube {
                    name: tube.to_string(),
                    weight,
                });
                if let Some(t) = self.tubes.get_mut(tube) {
                    t.watching_ct += 1;
                }
            }
            Response::Watching(conn.watched.len())
        } else {
            Response::InternalError
        }
    }

    fn cmd_ignore(&mut self, conn_id: u64, tube: &str) -> Response {
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            if conn.watched.len() < 2 {
                return Response::NotIgnored;
            }
            let was_watching = conn.watched.iter().any(|w| w.name == tube);
            conn.watched.retain(|w| w.name != tube);
            if was_watching && let Some(t) = self.tubes.get_mut(tube) {
                t.watching_ct = t.watching_ct.saturating_sub(1);
            }
            Response::Watching(conn.watched.len())
        } else {
            Response::InternalError
        }
    }

    fn cmd_peek(&self, id: u64) -> Response {
        match self.jobs.get(&id) {
            Some(job) => self.found_or_error(job),
            None => Response::NotFound,
        }
    }

    fn cmd_peek_ready(&self, conn_id: u64) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        if let Some(tube) = self.tubes.get(tube_name)
            && let Some(&(_, job_id)) = tube.ready.peek()
            && let Some(job) = self.jobs.get(&job_id)
        {
            return self.found_or_error(job);
        }
        Response::NotFound
    }

    fn cmd_peek_delayed(&self, conn_id: u64) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        if let Some(tube) = self.tubes.get(tube_name)
            && let Some(&(_, job_id)) = tube.delay.peek()
            && let Some(job) = self.jobs.get(&job_id)
        {
            return self.found_or_error(job);
        }
        Response::NotFound
    }

    fn cmd_peek_buried(&self, conn_id: u64) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        if let Some(tube) = self.tubes.get(tube_name)
            && let Some(&job_id) = tube.buried.front()
            && let Some(job) = self.jobs.get(&job_id)
        {
            return self.found_or_error(job);
        }
        Response::NotFound
    }

    fn cmd_peek_reserved(&self, conn_id: u64) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.as_str())
            .unwrap_or("default");
        let found = self
            .jobs
            .values()
            .filter(|j| j.state == JobState::Reserved && j.tube_name == tube_name)
            .min_by_key(|j| j.id);
        match found {
            Some(job) => self.found_or_error(job),
            None => Response::NotFound,
        }
    }

    fn cmd_kick(&mut self, conn_id: u64, bound: u32) -> Response {
        let tube_name = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(|| "default".to_string());

        let mut kicked = 0u32;

        // Kick buried first, then delayed
        let has_buried = self
            .tubes
            .get(&tube_name)
            .map(|t| !t.buried.is_empty())
            .unwrap_or(false);

        if has_buried {
            for _ in 0..bound {
                let job_id = {
                    let tube = match self.tubes.get_mut(&tube_name) {
                        Some(t) => t,
                        None => break,
                    };
                    match tube.buried.pop_front() {
                        Some(id) => {
                            tube.stat.buried_ct = tube.stat.buried_ct.saturating_sub(1);
                            id
                        }
                        None => break,
                    }
                };
                self.stats.buried_ct = self.stats.buried_ct.saturating_sub(1);

                // Group tracking: un-bury decrements buried count
                if let Some(job) = self.jobs.get(&job_id)
                    && let Some(ref grp) = job.group
                    && let Some(gs) = self.groups.get_mut(grp)
                {
                    gs.buried = gs.buried.saturating_sub(1);
                }

                // Re-enqueue as ready
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.state = JobState::Ready;
                    job.deadline_at = None;
                    job.kick_ct += 1;
                    let key = job.ready_key();
                    let tn = job.tube_name.clone();
                    if let Some(tube) = self.tubes.get_mut(&tn) {
                        tube.ready.insert(key, job_id);
                    }
                    self.ready_ct += 1;
                    if key.0 < URGENT_THRESHOLD {
                        self.stats.urgent_ct += 1;
                        if let Some(tube) = self.tubes.get_mut(&tn) {
                            tube.stat.urgent_ct += 1;
                        }
                    }
                }
                // WAL: write kick state change
                let pri = self.job_pri(job_id);
                self.wal_write_state_change(
                    job_id,
                    Some(JobState::Ready),
                    pri,
                    Duration::ZERO,
                    0,
                    StateChangeReason::Kick,
                );
                kicked += 1;
            }
        } else {
            // Kick delayed
            for _ in 0..bound {
                let job_id = {
                    let tube = match self.tubes.get_mut(&tube_name) {
                        Some(t) => t,
                        None => break,
                    };
                    match tube.delay.pop() {
                        Some((_, id)) => id,
                        None => break,
                    }
                };

                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.state = JobState::Ready;
                    job.deadline_at = None;
                    job.kick_ct += 1;
                    let key = job.ready_key();
                    let tn = job.tube_name.clone();
                    if let Some(tube) = self.tubes.get_mut(&tn) {
                        tube.ready.insert(key, job_id);
                    }
                    self.ready_ct += 1;
                    if key.0 < URGENT_THRESHOLD {
                        self.stats.urgent_ct += 1;
                        if let Some(tube) = self.tubes.get_mut(&tn) {
                            tube.stat.urgent_ct += 1;
                        }
                    }
                }
                // WAL: write kick state change
                let pri = self.job_pri(job_id);
                self.wal_write_state_change(
                    job_id,
                    Some(JobState::Ready),
                    pri,
                    Duration::ZERO,
                    0,
                    StateChangeReason::Kick,
                );
                kicked += 1;
            }
        }

        self.process_queue();
        Response::Kicked(kicked)
    }

    fn cmd_kick_job(&mut self, id: u64) -> Response {
        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        let state = job.state;
        let tube_name = job.tube_name.clone();

        match state {
            JobState::Buried => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.buried.retain(|&jid| jid != id);
                    tube.stat.buried_ct = tube.stat.buried_ct.saturating_sub(1);
                }
                self.stats.buried_ct = self.stats.buried_ct.saturating_sub(1);

                // Group tracking: un-bury decrements buried count
                if let Some(job) = self.jobs.get(&id)
                    && let Some(ref grp) = job.group
                    && let Some(gs) = self.groups.get_mut(grp)
                {
                    gs.buried = gs.buried.saturating_sub(1);
                }
            }
            JobState::Delayed => {
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.delay.remove_by_id(id);
                }
            }
            _ => return Response::NotFound,
        }

        if let Some(job) = self.jobs.get_mut(&id) {
            job.state = JobState::Ready;
            job.deadline_at = None;
            job.kick_ct += 1;
            let key = job.ready_key();
            if let Some(tube) = self.tubes.get_mut(&tube_name) {
                tube.ready.insert(key, id);
            }
            self.ready_ct += 1;
            if key.0 < URGENT_THRESHOLD {
                self.stats.urgent_ct += 1;
                if let Some(tube) = self.tubes.get_mut(&tube_name) {
                    tube.stat.urgent_ct += 1;
                }
            }
        }

        // WAL: write kick state change
        let pri = self.job_pri(id);
        self.wal_write_state_change(
            id,
            Some(JobState::Ready),
            pri,
            Duration::ZERO,
            0,
            StateChangeReason::Kick,
        );

        self.process_queue();
        Response::KickedOne
    }

    fn cmd_pause_tube(&mut self, tube: &str, delay: u32) -> Response {
        let tube = match self.tubes.get_mut(tube) {
            Some(t) => t,
            None => return Response::NotFound,
        };

        let delay_dur = Duration::from_secs(delay.max(1) as u64);
        tube.pause = delay_dur;
        tube.unpause_at = Some(Instant::now() + delay_dur);
        tube.stat.pause_ct += 1;

        Response::Paused
    }

    fn cmd_stats_job(&self, id: u64) -> Response {
        let job = match self.jobs.get(&id) {
            Some(j) => j,
            None => return Response::NotFound,
        };

        let now = Instant::now();
        let age = now.duration_since(job.created_at).as_secs() as i64;
        let time_left = match job.state {
            JobState::Reserved | JobState::Delayed => job
                .deadline_at
                .map(|d| {
                    if d > now {
                        d.duration_since(now).as_secs() as i64
                    } else {
                        0
                    }
                })
                .unwrap_or(0),
            _ => 0,
        };

        let time_reserved = if job.state == JobState::Reserved {
            job.reserved_at
                .map(|ra| now.duration_since(ra).as_secs() as i64)
                .unwrap_or(0)
        } else {
            0
        };

        let yaml = format!(
            "---\n\
             id: {}\n\
             tube: \"{}\"\n\
             state: {}\n\
             pri: {}\n\
             age: {}\n\
             delay: {}\n\
             ttr: {}\n\
             time-left: {}\n\
             time-reserved: {}\n\
             file: {}\n\
             reserves: {}\n\
             timeouts: {}\n\
             releases: {}\n\
             buries: {}\n\
             kicks: {}\n\
             idempotency-key: {}\n\
             idempotency-ttl: {}\n\
             group: {}\n\
             after-group: {}\n\
             concurrency-key: {}\n\
             concurrency-limit: {}\n",
            job.id,
            job.tube_name,
            job.state.as_str(),
            job.priority,
            age,
            job.delay.as_secs(),
            job.ttr.as_secs(),
            time_left,
            time_reserved,
            job.wal_file_seq.unwrap_or(0),
            job.reserve_ct,
            job.timeout_ct,
            job.release_ct,
            job.bury_ct,
            job.kick_ct,
            job.idempotency_key
                .as_ref()
                .map(|(k, _)| k.as_str())
                .unwrap_or(""),
            job.idempotency_key
                .as_ref()
                .map(|(_, ttl)| *ttl)
                .unwrap_or(0),
            job.group.as_deref().unwrap_or(""),
            job.after_group.as_deref().unwrap_or(""),
            job.concurrency_key
                .as_ref()
                .map(|(k, _)| k.as_str())
                .unwrap_or(""),
            job.concurrency_key.as_ref().map(|(_, l)| *l).unwrap_or(0),
        );
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_stats_tube(&self, tube_name: &str) -> Response {
        let tube = match self.tubes.get(tube_name) {
            Some(t) => t,
            None => return Response::NotFound,
        };

        let pause_time_left = tube
            .unpause_at
            .map(|u| {
                let now = Instant::now();
                if u > now {
                    u.duration_since(now).as_secs() as i64
                } else {
                    0
                }
            })
            .unwrap_or(0);

        let (p50, p95, p99) = tube.stat.percentiles();

        let bury_rate = tube.stat.bury_rate();

        let yaml = format!(
            "---\n\
             name: \"{}\"\n\
             current-jobs-urgent: {}\n\
             current-jobs-ready: {}\n\
             current-jobs-reserved: {}\n\
             current-jobs-delayed: {}\n\
             current-jobs-buried: {}\n\
             total-jobs: {}\n\
             current-using: {}\n\
             current-watching: {}\n\
             current-waiting: {}\n\
             cmd-delete: {}\n\
             cmd-pause-tube: {}\n\
             pause: {}\n\
             pause-time-left: {}\n\
             total-reserves: {}\n\
             total-timeouts: {}\n\
             total-buries: {}\n\
             bury-rate: {:.6}\n\
             processing-time-ewma: {:.6}\n\
             processing-time-min: {:.6}\n\
             processing-time-max: {:.6}\n\
             processing-time-samples: {}\n\
             processing-time-fast-threshold: {:.6}\n\
             processing-time-ewma-fast: {:.6}\n\
             processing-time-samples-fast: {}\n\
             processing-time-ewma-slow: {:.6}\n\
             processing-time-samples-slow: {}\n\
             processing-time-p50: {:.6}\n\
             processing-time-p95: {:.6}\n\
             processing-time-p99: {:.6}\n\
             queue-time-ewma: {:.6}\n\
             queue-time-min: {:.6}\n\
             queue-time-max: {:.6}\n\
             queue-time-samples: {}\n",
            tube.name,
            tube.stat.urgent_ct,
            tube.ready.len(),
            tube.stat.reserved_ct,
            tube.delay.len(),
            tube.stat.buried_ct,
            tube.stat.total_jobs_ct,
            tube.using_ct,
            tube.watching_ct,
            tube.stat.waiting_ct,
            tube.stat.total_delete_ct,
            tube.stat.pause_ct,
            tube.pause.as_secs(),
            pause_time_left,
            tube.stat.total_reserve_ct,
            tube.stat.total_timeout_ct,
            tube.stat.total_bury_ct,
            bury_rate,
            tube.stat.processing_time_ewma,
            tube.stat.processing_time_min.unwrap_or(0.0),
            tube.stat.processing_time_max.unwrap_or(0.0),
            tube.stat.processing_time_samples,
            FAST_THRESHOLD,
            tube.stat.processing_time_ewma_fast,
            tube.stat.processing_time_samples_fast,
            tube.stat.processing_time_ewma_slow,
            tube.stat.processing_time_samples_slow,
            p50,
            p95,
            p99,
            tube.stat.queue_time_ewma,
            tube.stat.queue_time_min.unwrap_or(0.0),
            tube.stat.queue_time_max.unwrap_or(0.0),
            tube.stat.queue_time_samples,
        );
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_stats_group(&self, group_name: &str) -> Response {
        let gs = match self.groups.get(group_name) {
            Some(gs) => gs,
            None => return Response::NotFound,
        };

        let yaml = format!(
            "---\n\
             name: \"{}\"\n\
             pending: {}\n\
             buried: {}\n\
             complete: {}\n\
             waiting-jobs: {}\n",
            group_name,
            gs.pending,
            gs.buried,
            gs.is_complete(),
            gs.waiting_jobs.len(),
        );
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_stats(&self) -> Response {
        let delayed_ct: usize = self.tubes.values().map(|t| t.delay.len()).sum();
        let uptime = Instant::now().duration_since(self.started_at).as_secs();

        // rusage stats
        let (rusage_utime, rusage_stime, rusage_maxrss) = {
            let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
            unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
            let utime = format!("{}.{:06}", usage.ru_utime.tv_sec, usage.ru_utime.tv_usec);
            let stime = format!("{}.{:06}", usage.ru_stime.tv_sec, usage.ru_stime.tv_usec);
            // On macOS ru_maxrss is in bytes; on Linux it's in kilobytes
            let maxrss = if cfg!(target_os = "linux") {
                usage.ru_maxrss * 1024
            } else {
                usage.ru_maxrss
            };
            (utime, stime, maxrss)
        };

        // WAL stats
        let (binlog_oldest, binlog_current, binlog_max_size, binlog_file_count, binlog_total_bytes) =
            match &self.wal {
                Some(wal) => (
                    wal.oldest_seq(),
                    wal.current_seq(),
                    wal.max_file_size(),
                    wal.file_count(),
                    wal.total_disk_bytes(),
                ),
                None => (0, 0, 0, 0, 0),
            };

        // TOAST stats. All zero when persistence is off, mirroring how the
        // binlog-* fields behave so the YAML schema stays stable.
        let (
            toast_total_bytes,
            toast_live_bytes,
            toast_segments,
            toast_compactions_total,
            toast_bodies_migrated_total,
            toast_bodies_dropped_corrupted,
        ) = match &self.body_store {
            Some(bs) => (
                bs.total_bytes(),
                bs.live_bytes(),
                bs.segment_count() as u64,
                bs.compactions_total(),
                bs.bodies_migrated_total(),
                bs.bodies_dropped_corrupted(),
            ),
            None => (0, 0, 0, 0, 0, 0),
        };

        let yaml = format!(
            "---\n\
             current-jobs-urgent: {}\n\
             current-jobs-ready: {}\n\
             current-jobs-reserved: {}\n\
             current-jobs-delayed: {}\n\
             current-jobs-buried: {}\n\
             cmd-put: {}\n\
             cmd-peek: {}\n\
             cmd-peek-ready: {}\n\
             cmd-peek-delayed: {}\n\
             cmd-peek-buried: {}\n\
             cmd-peek-reserved: {}\n\
             cmd-reserve: {}\n\
             cmd-reserve-with-timeout: {}\n\
             cmd-delete: {}\n\
             cmd-release: {}\n\
             cmd-use: {}\n\
             cmd-watch: {}\n\
             cmd-ignore: {}\n\
             cmd-bury: {}\n\
             cmd-kick: {}\n\
             cmd-touch: {}\n\
             cmd-stats: {}\n\
             cmd-stats-job: {}\n\
             cmd-stats-tube: {}\n\
             cmd-list-tubes: {}\n\
             cmd-list-tube-used: {}\n\
             cmd-list-tubes-watched: {}\n\
             cmd-pause-tube: {}\n\
             cmd-reserve-mode: {}\n\
             job-timeouts: {}\n\
             total-jobs: {}\n\
             max-job-size: {}\n\
             current-jobs-size: {}\n\
             max-jobs-size: {}\n\
             accounting-drift-events: {}\n\
             current-tubes: {}\n\
             current-connections: {}\n\
             current-producers: {}\n\
             current-workers: {}\n\
             current-waiting: {}\n\
             total-connections: {}\n\
             pid: {}\n\
             version: \"tuber {}\"\n\
             rusage-utime: {}\n\
             rusage-stime: {}\n\
             rusage-maxrss: {}\n\
             uptime: {}\n\
             binlog-oldest-index: {}\n\
             binlog-current-index: {}\n\
             binlog-records-migrated: {}\n\
             binlog-records-written: 0\n\
             binlog-max-size: {}\n\
             binlog-enabled: {}\n\
             binlog-file-count: {}\n\
             binlog-total-bytes: {}\n\
             toast-total-bytes: {}\n\
             toast-live-bytes: {}\n\
             toast-segments: {}\n\
             toast-compactions-total: {}\n\
             toast-bodies-migrated-total: {}\n\
             toast-bodies-dropped-corrupted: {}\n\
             recovered-missing-bodies: {}\n\
             max-storage-bytes: {}\n\
             current-concurrency-keys: {}\n\
             draining: {}\n\
             id: {}\n\
             name: {}\n\
             hostname: {}\n\
             os: {}\n\
             platform: {}\n",
            self.stats.urgent_ct,
            self.ready_ct,
            self.stats.reserved_ct,
            delayed_ct,
            self.stats.buried_ct,
            self.stats.op_ct[OP_PUT],
            self.stats.op_ct[OP_PEEKJOB],
            self.stats.op_ct[OP_PEEK_READY],
            self.stats.op_ct[OP_PEEK_DELAYED],
            self.stats.op_ct[OP_PEEK_BURIED],
            self.stats.op_ct[OP_PEEK_RESERVED],
            self.stats.op_ct[OP_RESERVE],
            self.stats.op_ct[OP_RESERVE_TIMEOUT],
            self.stats.op_ct[OP_DELETE],
            self.stats.op_ct[OP_RELEASE],
            self.stats.op_ct[OP_USE],
            self.stats.op_ct[OP_WATCH],
            self.stats.op_ct[OP_IGNORE],
            self.stats.op_ct[OP_BURY],
            self.stats.op_ct[OP_KICK],
            self.stats.op_ct[OP_TOUCH],
            self.stats.op_ct[OP_STATS],
            self.stats.op_ct[OP_STATSJOB],
            self.stats.op_ct[OP_STATS_TUBE],
            self.stats.op_ct[OP_LIST_TUBES],
            self.stats.op_ct[OP_LIST_TUBE_USED],
            self.stats.op_ct[OP_LIST_TUBES_WATCHED],
            self.stats.op_ct[OP_PAUSE_TUBE],
            self.stats.op_ct[OP_RESERVE_MODE],
            self.stats.timeout_ct,
            self.stats.total_jobs_ct,
            self.max_job_size,
            self.total_job_bytes,
            self.max_job_bytes.unwrap_or(0),
            self.stats.accounting_drift_events,
            self.tubes.len(),
            self.conns.len(),
            self.conns.values().filter(|c| c.is_producer()).count(),
            self.conns.values().filter(|c| c.is_worker()).count(),
            self.stats.waiting_ct,
            self.stats.total_connections,
            std::process::id(),
            env!("CARGO_PKG_VERSION"),
            rusage_utime,
            rusage_stime,
            rusage_maxrss,
            uptime,
            binlog_oldest,
            binlog_current,
            self.wal.as_ref().map(|w| w.records_migrated()).unwrap_or(0),
            binlog_max_size,
            if self.wal.is_some() { "true" } else { "false" },
            binlog_file_count,
            binlog_total_bytes,
            toast_total_bytes,
            toast_live_bytes,
            toast_segments,
            toast_compactions_total,
            toast_bodies_migrated_total,
            toast_bodies_dropped_corrupted,
            self.stats.recovered_missing_bodies,
            self.max_storage_bytes.unwrap_or(0),
            self.concurrency_keys.len(),
            if self.drain_mode { "true" } else { "false" },
            self.instance_id,
            self.name.as_deref().unwrap_or(""),
            self.hostname,
            self.os,
            self.platform,
        );
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_list_tubes(&self) -> Response {
        let mut yaml = "---\n".to_string();
        for name in self.tubes.keys() {
            yaml.push_str(&format!("- {name}\n"));
        }
        Response::Ok(yaml.into_bytes())
    }

    fn cmd_list_tube_used(&self, conn_id: u64) -> Response {
        let tube = self
            .conns
            .get(&conn_id)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(|| "default".to_string());
        Response::Using(tube)
    }

    fn cmd_list_tubes_watched(&self, conn_id: u64) -> Response {
        let mut yaml = "---\n".to_string();
        if let Some(conn) = self.conns.get(&conn_id) {
            for w in &conn.watched {
                yaml.push_str(&format!("- {}\n", w.name));
            }
        }
        Response::Ok(yaml.into_bytes())
    }

    // --- Internal helpers ---

    fn conn_deadline_soon(&self, conn_id: u64, now: Instant) -> bool {
        let conn = match self.conns.get(&conn_id) {
            Some(c) => c,
            None => return false,
        };
        if conn.reserved_jobs.is_empty() {
            return false;
        }
        let margin = Duration::from_secs(1);
        for &job_id in &conn.reserved_jobs {
            if let Some(job) = self.jobs.get(&job_id)
                && let Some(deadline) = job.deadline_at
                && deadline <= now + margin
            {
                return true;
            }
        }
        false
    }

    fn conn_has_ready_job(&self, conn_id: u64) -> bool {
        let conn = match self.conns.get(&conn_id) {
            Some(c) => c,
            None => return false,
        };
        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() {
                    continue;
                }
                if tube.has_ready() {
                    return true;
                }
            }
        }
        false
    }

    fn find_ready_job_for_conn(&self, conn_id: u64) -> Option<u64> {
        let conn = self.conns.get(&conn_id)?;
        let mut best: Option<((u32, u64), u64)> = None; // (key, job_id)

        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() {
                    continue;
                }
                if let Some(entry) = self.find_best_unblocked_ready(tube) {
                    let (key, jid) = entry;
                    match &best {
                        None => best = Some((key, jid)),
                        Some((bk, _)) => {
                            if key < *bk {
                                best = Some((key, jid));
                            }
                        }
                    }
                }
            }
        }

        best.map(|(_, id)| id)
    }

    fn next_rng(&mut self) -> u64 {
        self.rng_state = self.rng_state.wrapping_add(1);
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        x
    }

    /// Weighted job selection with a caller-supplied weight function.
    /// For standard weighted mode, weight = raw tube weight.
    /// For weighted-fair mode, weight = raw weight / processing_time_ewma.
    fn select_weighted_job_with(
        &mut self,
        conn_id: u64,
        weight_fn: fn(&WatchedTube, &Tube) -> f64,
    ) -> Option<u64> {
        let conn = self.conns.get(&conn_id)?;

        let mut candidates: Vec<(u64, f64)> = Vec::new();
        let mut total_weight: f64 = 0.0;

        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() || !tube.has_ready() {
                    continue;
                }
                if let Some((_, job_id)) = self.find_best_unblocked_ready(tube) {
                    let effective = weight_fn(w, tube);
                    total_weight += effective;
                    candidates.push((job_id, effective));
                }
            }
        }

        if total_weight == 0.0 {
            return None;
        }

        let x = self.next_rng();
        let r = (x as f64 / u64::MAX as f64) * total_weight;
        let mut cumulative: f64 = 0.0;
        for (job_id, weight) in &candidates {
            cumulative += weight;
            if r < cumulative {
                return Some(*job_id);
            }
        }

        candidates.last().map(|(id, _)| *id)
    }

    fn select_weighted_job(&mut self, conn_id: u64) -> Option<u64> {
        self.select_weighted_job_with(conn_id, |w, _| w.weight as f64)
    }

    fn select_weighted_fair_job(&mut self, conn_id: u64) -> Option<u64> {
        self.select_weighted_job_with(conn_id, |w, tube| {
            let ewma = tube.stat.processing_time_ewma;
            if ewma > 0.0 && tube.stat.processing_time_samples >= FAIR_MIN_SAMPLES {
                w.weight as f64 / ewma.max(FAIR_EWMA_FLOOR)
            } else {
                w.weight as f64
            }
        })
    }

    /// Check if a group is complete and promote any waiting after-jobs to ready.
    fn check_group_completion(&mut self, group_name: &str) {
        let is_complete = self
            .groups
            .get(group_name)
            .map(|gs| gs.is_complete())
            .unwrap_or(false);

        if !is_complete {
            return;
        }

        // Take waiting jobs out of the group
        let waiting_jobs = self
            .groups
            .get_mut(group_name)
            .map(|gs| std::mem::take(&mut gs.waiting_jobs))
            .unwrap_or_default();

        // Promote each waiting after-job to ready
        for job_id in &waiting_jobs {
            if let Some(job) = self.jobs.get_mut(job_id)
                && job.state == JobState::Delayed
                && job.deadline_at.is_none()
            {
                job.state = JobState::Ready;
                let key = job.ready_key();
                let tn = job.tube_name.clone();
                if let Some(tube) = self.tubes.get_mut(&tn) {
                    tube.ready.insert(key, *job_id);
                }
                self.ready_ct += 1;
                if key.0 < URGENT_THRESHOLD {
                    self.stats.urgent_ct += 1;
                    if let Some(tube) = self.tubes.get_mut(&tn) {
                        tube.stat.urgent_ct += 1;
                    }
                }
            }
        }

        // Clean up idle groups
        if self
            .groups
            .get(group_name)
            .map(|gs| gs.is_idle())
            .unwrap_or(false)
        {
            self.groups.remove(group_name);
        }

        if !waiting_jobs.is_empty() {
            self.process_queue();
        }
    }

    /// Try to match waiting connections with ready jobs.
    fn process_queue(&mut self) {
        let now = Instant::now();

        // First pass: collect indices of timed-out waiters
        let mut timed_out_indices = Vec::new();

        for (i, waiter) in self.waiters.iter().enumerate() {
            if let Some(deadline) = waiter.deadline
                && now >= deadline
            {
                timed_out_indices.push(i);
            }
        }

        let mut timed_out = Vec::new();
        for &i in timed_out_indices.iter().rev() {
            timed_out.push(self.remove_waiter_at(i));
        }

        for waiter in timed_out {
            if self.conn_deadline_soon(waiter.conn_id, now) {
                let _ = waiter.reply_tx.send(Response::DeadlineSoon);
            } else {
                let _ = waiter.reply_tx.send(Response::TimedOut);
            }
        }

        // Second pass: fulfill waiters. Recomputing the per-tube unblocked
        // top once per outer pass keeps the per-waiter check O(watches)
        // instead of the previous O(N) heap scan per waiter.
        loop {
            let mut tube_top: HashMap<String, ((u32, u64), u64)> = HashMap::new();
            for (name, tube) in &self.tubes {
                if tube.is_paused() {
                    continue;
                }
                if let Some(top) = self.find_best_unblocked_ready(tube) {
                    tube_top.insert(name.clone(), top);
                }
            }
            if tube_top.is_empty() {
                break;
            }

            let fulfilled_idx = self.waiters.iter().position(|waiter| {
                self.conns
                    .get(&waiter.conn_id)
                    .is_some_and(|conn| conn.watched.iter().any(|w| tube_top.contains_key(&w.name)))
            });

            let Some(i) = fulfilled_idx else { break };
            let conn_id = self.waiters[i].conn_id;
            // If find_job_for_waiting_conn disagrees with the cache (e.g., a
            // weighted RNG hits a zero-weight tube), break to avoid spinning.
            let Some(job_id) = self.find_job_for_waiting_conn(conn_id) else {
                break;
            };
            let waiter = self.remove_waiter_at(i);
            let resp = self.do_reserve(waiter.conn_id, job_id);
            let _ = waiter.reply_tx.send(resp);
        }
    }

    /// Same as find_ready_job_for_conn but uses inner state directly.
    fn find_ready_job_for_conn_inner(&self, conn_id: u64) -> Option<u64> {
        let conn = self.conns.get(&conn_id)?;
        let mut best: Option<((u32, u64), u64)> = None;

        for w in &conn.watched {
            if let Some(tube) = self.tubes.get(&w.name) {
                if tube.is_paused() {
                    continue;
                }
                if let Some(entry) = self.find_best_unblocked_ready(tube) {
                    let (key, jid) = entry;
                    match &best {
                        None => best = Some((key, jid)),
                        Some((bk, _)) => {
                            if key < *bk {
                                best = Some((key, jid));
                            }
                        }
                    }
                }
            }
        }

        best.map(|(_, id)| id)
    }

    /// Find a job for a waiting connection, respecting its reserve mode.
    fn find_job_for_waiting_conn(&mut self, conn_id: u64) -> Option<u64> {
        let mode = self.conns.get(&conn_id)?.reserve_mode;
        match mode {
            ReserveMode::Weighted => self.select_weighted_job(conn_id),
            ReserveMode::WeightedFair => self.select_weighted_fair_job(conn_id),
            ReserveMode::Fifo => self.find_ready_job_for_conn_inner(conn_id),
        }
    }

    fn add_waiter(&mut self, conn_id: u64, reply_tx: oneshot::Sender<Response>, deadline: Option<Instant>) {
        self.stats.waiting_ct += 1;
        if let Some(conn) = self.conns.get(&conn_id) {
            for w in &conn.watched {
                if let Some(tube) = self.tubes.get_mut(&w.name) {
                    tube.stat.waiting_ct += 1;
                }
            }
        }
        self.waiters.push(WaitingReserve { conn_id, reply_tx, deadline });
    }

    /// Pop the waiter at `i` and decrement both global and per-tube waiting
    /// counters. Keeps the `waiters` vec and stats strictly in sync.
    fn remove_waiter_at(&mut self, i: usize) -> WaitingReserve {
        let waiter = self.waiters.remove(i);
        self.stats.waiting_ct = self.stats.waiting_ct.saturating_sub(1);
        if let Some(conn) = self.conns.get(&waiter.conn_id) {
            for w in &conn.watched {
                if let Some(tube) = self.tubes.get_mut(&w.name) {
                    tube.stat.waiting_ct = tube.stat.waiting_ct.saturating_sub(1);
                }
            }
        }
        waiter
    }

    fn remove_waiter(&mut self, conn_id: u64) {
        let mut i = 0;
        while i < self.waiters.len() {
            if self.waiters[i].conn_id == conn_id {
                let waiter = self.remove_waiter_at(i);
                let _ = waiter.reply_tx.send(Response::TimedOut);
            } else {
                i += 1;
            }
        }
    }

    // --- WAL helpers ---

    /// Current priority of a job, or 0 if the job is gone.
    /// Used by WAL state-change writes where the priority is not being changed.
    fn job_pri(&self, id: u64) -> u32 {
        self.jobs.get(&id).map_or(0, |j| j.priority)
    }

    /// True iff the WAL has buffered writes since its last successful sync.
    /// Drives the engine's group-commit decisions; see the `serve` loop banner.
    fn wal_is_dirty(&self) -> bool {
        self.wal.as_ref().is_some_and(|w| w.is_dirty())
    }

    /// True iff the WAL is configured for strict durability (sync_interval
    /// == 0). Computed once per recv arm; threaded into `dispatch_command`
    /// to decide Pending vs Immediate without re-reading state.
    fn wal_is_strict(&self) -> bool {
        self.wal
            .as_ref()
            .is_some_and(|w| w.sync_interval().is_zero())
    }

    /// fsync TOAST then WAL. Disables the WAL on error (logged) so the
    /// rest of the engine keeps serving in-memory state — same failure
    /// shape as the existing `wal_write_*` helpers.
    fn sync_wal(&mut self) {
        if let Some(wal) = self.wal.as_mut()
            && let Err(e) = wal.sync()
        {
            tracing::error!("WAL sync error: {}, disabling WAL", e);
            self.wal = None;
        }
    }

    /// True iff the WAL is dirty AND its sync-staleness SLA has elapsed.
    /// Tick-branch backstop predicate; reads the WAL exactly once.
    fn wal_sync_due(&self) -> bool {
        let Some(wal) = self.wal.as_ref() else { return false };
        if !wal.is_dirty() {
            return false;
        }
        let interval = wal.sync_interval();
        interval.is_zero() || wal.last_sync_elapsed() >= interval
    }

    fn wal_write_put(&mut self, job_id: u64) {
        if self.wal.is_none() {
            return;
        }
        // Temporarily take the job out to satisfy the borrow checker while
        // calling into the WAL. Accounting is net-zero across the pair because
        // `wal.write_put` does not change `job.body`, only bookkeeping fields.
        if let Some(mut job) = self.take_job(job_id) {
            if let Some(wal) = self.wal.as_mut()
                && let Err(e) = wal.write_put(&mut job)
            {
                tracing::error!("WAL write_put error: {}, disabling WAL", e);
                self.wal = None;
            }
            self.insert_job(job_id, job);
        }
    }

    fn wal_write_state_change(
        &mut self,
        job_id: u64,
        state: Option<JobState>,
        pri: u32,
        delay: Duration,
        expiry_epoch_secs: u64,
        reason: StateChangeReason,
    ) {
        if self.wal.is_none() {
            return;
        }
        if let Some(mut job) = self.take_job(job_id) {
            if let Some(wal) = self.wal.as_mut()
                && let Err(e) =
                    wal.write_state_change(&mut job, state, pri, delay, expiry_epoch_secs, reason)
            {
                tracing::error!("WAL write_state_change error: {}, disabling WAL", e);
                self.wal = None;
            }
            self.insert_job(job_id, job);
        }
    }

}

/// Parse a human-readable byte count (`1g`, `500M`, `100k`, or raw bytes) into u64.
/// Case-insensitive. Trailing `B` accepted (`2GB`, `500MB`). No decimals.
pub fn parse_bytes(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty byte count".to_string());
    }
    // Strip an optional trailing "B" / "b" (e.g. "2GB").
    let s_no_b = s.strip_suffix(['B', 'b']).unwrap_or(s);
    let (num_part, multiplier): (&str, u64) = match s_no_b.chars().last() {
        Some('k' | 'K') => (&s_no_b[..s_no_b.len() - 1], 1024),
        Some('m' | 'M') => (&s_no_b[..s_no_b.len() - 1], 1024 * 1024),
        Some('g' | 'G') => (&s_no_b[..s_no_b.len() - 1], 1024 * 1024 * 1024),
        Some('t' | 'T') => (&s_no_b[..s_no_b.len() - 1], 1024u64.pow(4)),
        _ => (s_no_b, 1),
    };
    let n: u64 = num_part
        .trim()
        .parse()
        .map_err(|e| format!("invalid byte count {s:?}: {e}"))?;
    n.checked_mul(multiplier)
        .ok_or_else(|| format!("byte count {s:?} overflows u64"))
}

/// Build ServerState, replaying the WAL if `wal_dir` is set.
///
/// On a large binlog this can take seconds to minutes and allocate
/// proportional RAM. Callers that care about TCP readiness (e.g. [`run`])
/// must complete this step *before* binding the listener, so the accept
/// port only becomes reachable once the server is genuinely able to serve
/// commands.
///
/// May return `io::ErrorKind::OutOfMemory` if, after replaying the WAL,
/// the actual in-memory job set exceeds `max_job_bytes`.
fn build_state(
    max_job_size: u32,
    max_job_bytes: Option<u64>,
    max_storage_bytes: Option<u64>,
    wal_dir: Option<&Path>,
    sync_interval: Duration,
    migrate_wal: bool,
    name: Option<String>,
) -> io::Result<ServerState> {
    let mut state = ServerState::new(max_job_size, max_job_bytes, max_storage_bytes, name);

    if let Some(dir) = wal_dir {
        // Persistence requires an explicit disk budget. Without one, TOAST
        // grows unbounded and the operator typically realises only after
        // the disk fills — exactly the footgun the design doc set out to
        // avoid. Refuse to start with a clear pointer at the missing flag.
        if max_storage_bytes.is_none() {
            return Err(io::Error::other(
                "--binlog-dir requires --max-storage-bytes (env TUBER_MAX_STORAGE_BYTES). \
                 Pick a number you'd be comfortable having entirely consumed by the queue \
                 — Tuber will return OUT_OF_STORAGE on puts once the projected WAL+TOAST \
                 footprint would exceed it. Suggested starting point: 80% of the volume.",
            ));
        }

        // Open the WAL first so we can peek at file format versions before
        // doing any TOAST/body-store work — that way a refusal returns
        // cleanly with no on-disk side effects.
        let mut wal = Wal::open(
            dir,
            crate::wal::WalConfig {
                max_file_size: None,
                sync_interval,
            },
        )?;

        // Legacy-format gate: pre-v5 records carry inline bodies that the
        // replay path lifts into the body store. We refuse to migrate
        // silently — operators who haven't reasoned about the upgrade
        // should hit a clear stop sign before any data is touched.
        let legacy_version = wal
            .min_format_version()?
            .filter(|v| *v < crate::wal::WAL_VERSION);
        if let Some(min_version) = legacy_version {
            if !migrate_wal {
                return Err(io::Error::other(format!(
                    "WAL contains v{min_version} records, but this binary writes \
                     v{}. Re-run with --migrate-wal to convert (inline bodies are \
                     promoted into TOAST). Back up {} first if you want a \
                     rollback option.",
                    crate::wal::WAL_VERSION,
                    dir.display(),
                )));
            }
            tracing::info!(
                "WAL legacy format detected (v{} → v{}); migrating via --migrate-wal",
                min_version,
                crate::wal::WAL_VERSION,
            );
        }

        // Body store sits next to the WAL in `<dir>/toast/`. Open it after
        // the version check so any v5 `BodyId` references in WAL records
        // can resolve during replay. The directory is created on demand —
        // existing operators upgrading without TOAST data see an empty
        // index, which is correct.
        let toast_dir = dir.join("toast");
        let body_store = Arc::new(BodyStore::open(
            &toast_dir,
            crate::body_store::DEFAULT_SEGMENT_SIZE,
        )?);

        let on_disk = wal.total_disk_bytes();

        tracing::info!("WAL: replaying {} bytes from {:?}", on_disk, dir);
        let (mut jobs, next_id, tombstones, orphan_bodies) = wal.replay()?;
        let job_count = jobs.len();
        let tombstone_count = tombstones.len();

        // Reclaim TOAST bodies whose owning job was deleted but whose
        // pre-WAL-fsync `BodyStore::delete` never landed (server crashed
        // between WAL fsync and TOAST cleanup). Without this, orphans
        // accumulate on disk indefinitely.
        if !orphan_bodies.is_empty() {
            body_store.delete_many(&orphan_bodies);
            tracing::info!(
                "WAL replay: reclaimed {} orphan TOAST bodies",
                orphan_bodies.len()
            );
        }

        // Migrate inline bodies recovered from pre-v5 WAL records into the
        // body store. v5 records already arrive as External — left alone.
        let mut migrated = 0u64;
        for job in jobs.values_mut() {
            if let Some(bytes) = job.body.take_inline() {
                let body_id = body_store.write_body(&bytes)?;
                job.body = BodyRef::External(body_id);
                migrated += 1;
            }
        }
        if migrated > 0 {
            // Force the body store to disk before the WAL replay completes
            // — bodies must outlive their referencing FullJob records.
            body_store.fsync()?;
            tracing::info!("WAL→TOAST: migrated {} inline bodies into the body store", migrated);
        }

        // Integrity check: WAL records that reference TOAST bodies which
        // no longer exist (segment corruption, manual `rm`, or the
        // CRC-failure drop in `compact_segment`). The TOAST-then-WAL
        // fsync ordering means a clean crash should never produce this,
        // but disk faults and operator mistakes can. Reap before
        // `restore_jobs` so we don't have to unwind heaps + stats — the
        // broken jobs never enter the live set. Each reap is journalled
        // as a state-change-delete so the next restart sees them gone.
        //
        // Operational consequence: wiping `<wal-dir>/toast/` while the
        // WAL survives reaps every job. There's nothing to serve their
        // bodies from. Operators who want a clean restart should move
        // the WAL aside too.
        let mut reaped: Vec<Job> = Vec::new();
        let kept: HashMap<u64, Job> = jobs
            .into_iter()
            .filter_map(|(id, job)| {
                if let BodyRef::External(body_id) = &job.body
                    && !body_store.contains_body(*body_id)
                {
                    tracing::error!(
                        job_id = id,
                        tube = %job.tube_name,
                        body_id = body_id.0,
                        state = job.state.as_str(),
                        "WAL references body missing from TOAST: reaping job (lost data)",
                    );
                    reaped.push(job);
                    None
                } else {
                    Some((id, job))
                }
            })
            .collect();
        let recovered_missing_bodies = reaped.len() as u64;
        for mut job in reaped {
            // Best-effort: journal the delete so the warning doesn't
            // re-fire on every restart. WAL errors here aren't fatal —
            // in-memory state is already correct (the job is gone), and
            // the next restart will just re-reap.
            if let Err(e) = wal.write_state_change(
                &mut job,
                None,
                0,
                Duration::ZERO,
                0,
                StateChangeReason::None,
            ) {
                tracing::error!(
                    job_id = job.id,
                    "WAL delete journal failed for reaped job: {}", e,
                );
            }
        }
        state.stats.recovered_missing_bodies = recovered_missing_bodies;

        state.restore_jobs(kept, next_id, tombstones);

        // Enforce the in-memory budget after replay. The WAL on-disk size
        // is not a reliable proxy (tombstones, superseded records, and format
        // overhead can make it 1.4x+ larger than the live set), so we measure
        // the actual in-memory cost.
        if let Some(max) = max_job_bytes
            && state.total_job_bytes > max
        {
            let actual = state.total_job_bytes;
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "WAL replay produced {actual} in-memory bytes, exceeds --max-jobs-size \
                     ({max} bytes). Raise --max-jobs-size or move the binlog aside and restart."
                ),
            ));
        }

        // Hand the body store to the WAL so every WAL fsync is preceded
        // by a TOAST fsync — the durability invariant for v5 records.
        wal.set_body_store(Arc::clone(&body_store));
        state.wal = Some(wal);
        state.body_store = Some(body_store);
        tracing::info!(
            "WAL: replayed {} jobs and {} idempotency tombstones from {:?}",
            job_count,
            tombstone_count,
            dir
        );
    }

    Ok(state)
}

/// Start the beanstalkd server.
///
/// WAL replay runs *before* the TCP listener is bound, so the accept port
/// only becomes reachable once the server is ready to handle commands. This
/// makes TCP-level health checks (e.g. `nc -z host 11300`) an honest signal of
/// readiness — during replay there is nothing to connect to, rather than a
/// listening socket that silently never calls `accept()`.
pub async fn run(
    addr: &str,
    port: u16,
    max_job_size: u32,
    max_job_bytes: Option<u64>,
    max_storage_bytes: Option<u64>,
    wal_dir: Option<&str>,
    sync_interval: Duration,
    migrate_wal: bool,
    metrics_port: Option<u16>,
    name: Option<String>,
) -> io::Result<()> {
    let wal_path = wal_dir.map(Path::new);
    let state = build_state(
        max_job_size,
        max_job_bytes,
        max_storage_bytes,
        wal_path,
        sync_interval,
        migrate_wal,
        name.clone(),
    )?;

    let listener = TcpListener::bind((addr, port)).await?;
    let mut opts = format!(" max-job-size={max_job_size}");
    if let Some(b) = max_job_bytes {
        opts.push_str(&format!(" max-jobs-size={b}"));
    }
    if let Some(b) = max_storage_bytes {
        opts.push_str(&format!(" max-storage-bytes={b}"));
    }
    if let Some(ref dir) = wal_dir {
        opts.push_str(&format!(" binlog={dir}"));
        if sync_interval.is_zero() {
            opts.push_str(" sync=every-write");
        } else {
            opts.push_str(&format!(" sync={}ms", sync_interval.as_millis()));
        }
    }
    if let Some(mp) = metrics_port {
        opts.push_str(&format!(" metrics={}:{mp}", listener.local_addr()?.ip()));
    }
    if let Some(ref n) = name {
        tracing::info!(
            "tuber v{} [{}] listening on {}:{}{opts}",
            env!("CARGO_PKG_VERSION"),
            n,
            addr,
            port
        );
    } else {
        tracing::info!(
            "tuber v{} listening on {}:{}{opts}",
            env!("CARGO_PKG_VERSION"),
            addr,
            port
        );
    }

    if let Some(mp) = metrics_port {
        let listen_addr = listener.local_addr()?.ip();
        let beanstalk_addr = format!("{listen_addr}:{port}");
        tokio::spawn(async move {
            if let Err(e) = crate::metrics::serve(listen_addr, mp, beanstalk_addr).await {
                tracing::error!("metrics server error: {e}");
            }
        });
    }

    serve(listener, state, max_job_size).await
}

/// Run the server with a pre-bound listener. Used by tests and benches that
/// need to bind port 0 and learn the ephemeral port before starting the
/// server. Production startup goes through [`run`] instead, which replays
/// the WAL before binding.
pub async fn run_with_listener(
    listener: TcpListener,
    max_job_size: u32,
    wal_dir: Option<&Path>,
    name: Option<String>,
) -> io::Result<()> {
    // Test-only entry point: enable migration so legacy fixtures work
    // without the integration-test harness gaining a flag of its own.
    // When a WAL is requested, supply a generous default disk budget so
    // tests don't have to thread one through — production callers must
    // set --max-storage-bytes explicitly (enforced in `build_state`).
    let default_budget: Option<u64> = wal_dir.map(|_| 1024 * 1024 * 1024);
    run_with_listener_limited(
        listener,
        max_job_size,
        None,
        default_budget,
        wal_dir,
        true,
        name,
    )
    .await
}

/// Like [`run_with_listener`] but with explicit memory and storage budgets.
/// Exists so integration tests can exercise the budget enforcement paths
/// without going through the `run` CLI path.
pub async fn run_with_listener_limited(
    listener: TcpListener,
    max_job_size: u32,
    max_job_bytes: Option<u64>,
    max_storage_bytes: Option<u64>,
    wal_dir: Option<&Path>,
    migrate_wal: bool,
    name: Option<String>,
) -> io::Result<()> {
    let state = build_state(
        max_job_size,
        max_job_bytes,
        max_storage_bytes,
        wal_dir,
        crate::wal::DEFAULT_SYNC_INTERVAL,
        migrate_wal,
        name,
    )?;
    serve(listener, state, max_job_size).await
}

/// Test-only: same as [`run_with_listener`] but with `--sync-interval 0`,
/// so the group-commit hot path runs at strictest durability. Used by the
/// throughput test to verify that batching keeps the ceiling well above
/// the per-put-fsync ceiling we'd see without it. Same generous default
/// disk budget as `run_with_listener` — production callers must pick one.
pub async fn run_with_listener_sync_zero(
    listener: TcpListener,
    max_job_size: u32,
    wal_dir: &Path,
) -> io::Result<()> {
    let state = build_state(
        max_job_size,
        None,
        Some(1024 * 1024 * 1024),
        Some(wal_dir),
        Duration::ZERO,
        true,
        None,
    )?;
    serve(listener, state, max_job_size).await
}

/// Maximum number of acks the engine will buffer before forcing a sync.
/// Channel is 1024; cap at half so the drain loop doesn't monopolise the
/// task and the tail of a fully-loaded batch sees ≤ one fsync of latency.
const MAX_BATCH: usize = 512;

/// Result of dispatching a single command. Drives the engine's group-commit
/// decision: should the response go out now, defer until the next sync, or
/// disappear entirely (because a reserve queued onto the waiter list)?
enum DispatchOutcome {
    /// Command dirtied the WAL — defer the ack into `pending` so a single
    /// fsync at the end of the drain covers every write in the batch.
    Pending(oneshot::Sender<Response>, Response),
    /// Read-only command; ack immediately.
    Immediate(oneshot::Sender<Response>, Response),
    /// Reserve with no available job parked onto the waiter list. Reply
    /// will be sent later by `process_queue` (post-sync) or the tick.
    Deferred,
}

#[derive(PartialEq, Eq)]
enum ProcessResult {
    Continue,
    Shutdown,
}

/// Run a single Command through `state.handle_command` and classify the
/// outcome. Reserve-with-timeout that returns `TimedOut` is bounced onto
/// the waiter list (reply held by the waiter, returns `Deferred`).
///
/// Durability mode is controlled by `--sync-interval`:
/// - `sync_interval == 0`: **strict.** Defer the ack (Pending) so the
///   engine's drain-then-fsync loop covers the whole batch with one fsync
///   before any client hears `INSERTED`. This is the group-commit hot path.
/// - `sync_interval > 0`: **relaxed.** Ack immediately even though the
///   WAL has buffered bytes; the tick branch will fsync within
///   `sync_interval` of every write. Up to `sync_interval` of acked work
///   can be lost on a process kill or power failure. This is the
///   pre-Phase-8 default behaviour and the throughput operators expect.
fn dispatch_command(
    state: &mut ServerState,
    strict: bool,
    conn_id: u64,
    cmd: Command,
    body: Option<Vec<u8>>,
    reply_tx: oneshot::Sender<Response>,
) -> DispatchOutcome {
    // Single match decides both is-this-a-reserve and what timeout was set.
    // `Some(None)` = infinite-wait Reserve; `Some(Some(t))` = with timeout;
    // `None` = not a reserve at all.
    let timeout = match &cmd {
        Command::Reserve => Some(None),
        Command::ReserveWithTimeout { timeout } => Some(Some(*timeout)),
        _ => None,
    };

    let resp = state.handle_command(conn_id, cmd, body);

    // Reserve with no available job — park onto the waiter list rather
    // than ack TimedOut. The reply_tx travels with the waiter; it'll fire
    // when a put wakes the waiter (after that put's sync) or on timeout.
    if let Some(t) = timeout
        && matches!(resp, Response::TimedOut)
        && t != Some(0)
    {
        let deadline = t.map(|secs| Instant::now() + Duration::from_secs(secs as u64));
        state.add_waiter(conn_id, reply_tx, deadline);
        return DispatchOutcome::Deferred;
    }

    if strict && state.wal_is_dirty() {
        DispatchOutcome::Pending(reply_tx, resp)
    } else {
        DispatchOutcome::Immediate(reply_tx, resp)
    }
}

/// Process one engine message; classify into pending/immediate/deferred or
/// signal Shutdown. Disconnects update connection state but never produce
/// a reply.
fn process_message(
    state: &mut ServerState,
    strict: bool,
    msg: EngineMsg,
    pending: &mut Vec<(oneshot::Sender<Response>, Response)>,
) -> ProcessResult {
    match msg.payload {
        EnginePayload::Command { cmd, body, reply_tx } => {
            match dispatch_command(state, strict, msg.conn_id, cmd, body, reply_tx) {
                DispatchOutcome::Pending(tx, r) => pending.push((tx, r)),
                DispatchOutcome::Immediate(tx, r) => {
                    let _ = tx.send(r);
                }
                DispatchOutcome::Deferred => {}
            }
            ProcessResult::Continue
        }
        EnginePayload::Disconnect => {
            state.unregister_conn(msg.conn_id);
            ProcessResult::Continue
        }
        EnginePayload::Shutdown => ProcessResult::Shutdown,
    }
}

/// Drain every deferred ack. Disconnected clients silently ignored —
/// the WAL write is durable regardless of whether the client hears about it.
fn drain_pending(pending: &mut Vec<(oneshot::Sender<Response>, Response)>) {
    for (tx, r) in pending.drain(..) {
        let _ = tx.send(r);
    }
}

/// Run the engine task and accept loop with a fully-built [`ServerState`].
async fn serve(listener: TcpListener, mut state: ServerState, max_job_size: u32) -> io::Result<()> {
    let (engine_tx, mut engine_rx) = mpsc::channel::<EngineMsg>(1024);

    // TOAST compaction lives in its own task: when a sealed segment's live
    // ratio drops below the threshold, copy its surviving bodies into the
    // current write segment and unlink the old file. The engine task
    // remains the sole writer of `ServerState`; compaction only touches
    // `BodyStore`, which is internally synchronized.
    let compaction_shutdown = Arc::new(tokio::sync::Notify::new());
    let compaction_handle: Option<tokio::task::JoinHandle<()>> =
        if let Some(bs) = state.body_store.as_ref() {
            let bs = Arc::clone(bs);
            let shutdown = Arc::clone(&compaction_shutdown);
            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(TOAST_COMPACTION_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.notified() => break,
                        _ = interval.tick() => {}
                    }
                    let Some(seq) = bs.compaction_candidate(
                        crate::body_store::COMPACTION_LIVE_RATIO_THRESHOLD,
                    ) else {
                        continue;
                    };
                    // Compaction is synchronous file IO: pwrite + fsync per
                    // body, plus an unlink at the end. On fast SSD with
                    // typical bodies it's well under 100 ms, but on slow
                    // storage (HDD/NAS), large bodies, or under CPU pressure
                    // it can stall the runtime. Run on the blocking pool so
                    // a slow compaction can't starve other tokio tasks.
                    let bs_for_blocking = Arc::clone(&bs);
                    let result = tokio::task::spawn_blocking(move || {
                        bs_for_blocking.compact_segment(seq)
                    })
                    .await;
                    match result {
                        Ok(Ok(n)) if n > 0 => {
                            tracing::info!(
                                "TOAST compacted segment {}: migrated {} bodies",
                                seq, n
                            );
                        }
                        Ok(Ok(_)) => {
                            tracing::debug!("TOAST compacted segment {}: empty", seq);
                        }
                        Ok(Err(e)) => {
                            tracing::error!(
                                "TOAST compaction failed for segment {}: {}",
                                seq, e
                            );
                        }
                        Err(je) => {
                            tracing::error!(
                                "TOAST compaction task panicked on segment {}: {}",
                                seq, je
                            );
                        }
                    }
                }
            }))
        } else {
            None
        };

    // Shrink the engine tick when the WAL fsync interval is tighter than the
    // default 100 ms tick, so fsync cadence isn't rate-limited by the tick.
    // `sync_interval == 0` (per-write fsync) stays on the 100 ms tick — the
    // tick only drives GC/maintenance, not the per-write sync.
    let default_tick = Duration::from_millis(100);
    let tick_period = state
        .wal
        .as_ref()
        .map(|w| w.sync_interval())
        .filter(|d| !d.is_zero() && *d < default_tick)
        .unwrap_or(default_tick);

    // Engine task. Two durability modes governed by `--sync-interval`:
    //
    //   sync_interval == 0  → strict, group-commit hot path:
    //     1. Receive a message; dispatch it (Pending if dirty).
    //     2. Drain the channel non-blocking up to MAX_BATCH.
    //     3. fsync once covering the whole batch.
    //     4. Send every deferred ack — clients see their `INSERTED` only
    //        AFTER sync, so the durability invariant "ack ⇒ on disk" holds.
    //
    //   sync_interval >  0  → relaxed:
    //     dispatch acks Immediately even on dirty writes (Pending is never
    //     produced). The tick branch fsyncs once per `sync_interval`.
    //     Up to `sync_interval` of acked work can be lost on crash — same
    //     contract as pre-Phase-8.
    let engine_handle = tokio::spawn(async move {
        let mut tick_interval = tokio::time::interval(tick_period);
        let mut sigusr1 =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
                .expect("failed to register SIGUSR1 handler");
        let mut pending: Vec<(oneshot::Sender<Response>, Response)> =
            Vec::with_capacity(MAX_BATCH);

        loop {
            tokio::select! {
                msg = engine_rx.recv() => {
                    let Some(msg) = msg else { break };

                    // Computed once per recv arm and threaded through so
                    // dispatch_command doesn't have to re-read the WAL.
                    let strict = state.wal_is_strict();

                    let mut shutdown_seen =
                        process_message(&mut state, strict, msg, &mut pending) == ProcessResult::Shutdown;

                    if strict {
                        // Group commit: drain non-blocking up to MAX_BATCH
                        // so one fsync amortises across the whole burst.
                        // Relaxed mode (sync_interval > 0) skips this —
                        // process_message already sent the ack as Immediate,
                        // and a drain loop here would starve per-connection
                        // tasks waiting to deliver those acks. (Removing
                        // this gate causes a 50× throughput regression at
                        // sync_interval > 0; see docs/architecture.md.)
                        while !shutdown_seen && pending.len() < MAX_BATCH {
                            let next = match engine_rx.try_recv() {
                                Ok(m) => m,
                                Err(_) => break,
                            };
                            shutdown_seen =
                                process_message(&mut state, strict, next, &mut pending)
                                    == ProcessResult::Shutdown;
                        }
                        // pending non-empty implies the WAL is dirty (we
                        // only push Pending when it is). sync_wal() is also
                        // a no-op on a clean WAL, so an extra check would
                        // be redundant.
                        if !pending.is_empty() {
                            state.sync_wal();
                            drain_pending(&mut pending);
                        }
                    }

                    if shutdown_seen {
                        tracing::info!("engine shutting down, flushing WAL");
                        if let Some(wal) = &mut state.wal {
                            wal.flush_and_sync();
                        }
                        break;
                    }
                }
                _ = tick_interval.tick() => {
                    state.tick();
                    // SLA backstop. state.tick() can dirty the WAL via TTR
                    // expiry; pending is always empty here because the recv
                    // arm flushes its own before yielding.
                    if state.wal_sync_due() {
                        state.sync_wal();
                    }
                }
                _ = sigusr1.recv() => {
                    tracing::info!("received SIGUSR1, entering drain mode");
                    state.drain_mode = true;
                }
                else => break,
            }
        }
        tracing::info!("engine stopped");
    });

    // Accept loop with graceful shutdown on SIGINT/SIGTERM
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("failed to register SIGINT handler");
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (socket, peer) = result?;
                let _ = socket.set_nodelay(true);
                tracing::debug!("accepted connection from {}", peer);

                let tx = engine_tx.clone();
                tokio::spawn(async move {
                    handle_connection(socket, tx, max_job_size).await;
                });
            }
            _ = sigint.recv() => {
                tracing::info!("received SIGINT, shutting down gracefully");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM, shutting down gracefully");
                break;
            }
        }
    }

    let _ = engine_tx
        .send(EngineMsg {
            conn_id: 0,
            payload: EnginePayload::Shutdown,
        })
        .await;

    // Drop the sender so the engine loop exits after processing Shutdown,
    // then await the task so the WAL is fully flushed before we return.
    drop(engine_tx);
    let _ = engine_handle.await;

    // Stop compaction after the engine has drained: an in-flight migration
    // can take the BodyStore mutex briefly, so the engine's final fsync
    // shouldn't race against it. notify_one + await joins cleanly.
    if let Some(handle) = compaction_handle {
        compaction_shutdown.notify_one();
        let _ = handle.await;
    }

    Ok(())
}

/// Atomic counter for connection IDs (simpler than engine round-trip).
static NEXT_CONN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Maximum digits in a u64 value (18446744073709551615).
const MAX_U64_DIGITS: usize = 20;

/// Maximum command line length in bytes, including the trailing \r\n.
/// The longest possible command is delete-batch with MAX_DELETE_BATCH (1000) u64 IDs:
///   "delete-batch "                                  =  13
///   1000 × (u64 + space)                             = 21000  (1000 × (20 + 1))
///   "\r\n"                                           =   2
///   Total                                            = 21015
///
/// For reference, put with all extensions is 891 bytes (well under this limit).
const MAX_LINE_LEN: u64 = (13 + MAX_DELETE_BATCH * (MAX_U64_DIGITS + 1) + 2) as u64;

async fn handle_connection(
    socket: tokio::net::TcpStream,
    engine_tx: mpsc::Sender<EngineMsg>,
    max_job_size: u32,
) {
    let conn_id = NEXT_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line_buf = String::new();
    let mut resp_buf = Vec::with_capacity(256);

    loop {
        line_buf.clear();
        // Limit read to MAX_LINE_LEN to prevent unbounded memory growth from
        // clients that send data without a newline. Matches beanstalkd behavior.
        let _n = match (&mut reader)
            .take(MAX_LINE_LEN)
            .read_line(&mut line_buf)
            .await
        {
            Ok(0) => break, // EOF
            Ok(n) => {
                if !line_buf.ends_with('\n') {
                    // Line exceeded MAX_LINE_LEN without a newline — bad client
                    let _ = writer.write_all(b"BAD_FORMAT\r\n").await;
                    break;
                }
                n
            }
            Err(e) => {
                tracing::debug!("read error for conn {}: {}", conn_id, e);
                break;
            }
        };

        // Strip trailing \r\n
        let cmd_str = line_buf.trim_end_matches('\n').trim_end_matches('\r');

        // Parse command
        let cmd = match protocol::parse_command(cmd_str) {
            Ok(cmd) => cmd,
            Err(resp) => {
                resp_buf.clear();
                resp.serialize_into(&mut resp_buf);
                let _ = writer.write_all(&resp_buf).await;
                continue;
            }
        };

        // Handle quit
        if matches!(cmd, Command::Quit) {
            break;
        }

        // If it's a put command, read the body
        let body = if let Command::Put { bytes, .. } = &cmd {
            let body_size = *bytes as usize;

            // Check size BEFORE allocating to prevent OOM from malicious clients
            if body_size > max_job_size as usize {
                let _ = writer.write_all(b"JOB_TOO_BIG\r\n").await;
                // Drain the body + \r\n so the connection stays usable
                let to_drain = body_size as u64 + 2;
                if tokio::io::copy(&mut (&mut reader).take(to_drain), &mut tokio::io::sink())
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            }

            let mut body_buf = vec![0u8; body_size + 2]; // +2 for \r\n
            match reader.read_exact(&mut body_buf).await {
                Ok(_) => {
                    // Verify trailing \r\n
                    if body_buf[body_size] != b'\r' || body_buf[body_size + 1] != b'\n' {
                        let _ = writer.write_all(b"EXPECTED_CRLF\r\n").await;
                        continue;
                    }
                    body_buf.truncate(body_size);

                    Some(body_buf)
                }
                Err(e) => {
                    tracing::debug!("body read error for conn {}: {}", conn_id, e);
                    break;
                }
            }
        } else {
            None
        };

        // Send to engine and await response
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = engine_tx
            .send(EngineMsg {
                conn_id,
                payload: EnginePayload::Command {
                    cmd,
                    body,
                    reply_tx,
                },
            })
            .await;

        match reply_rx.await {
            Ok(resp) => {
                resp_buf.clear();
                resp.serialize_into(&mut resp_buf);
                if writer.write_all(&resp_buf).await.is_err() {
                    break;
                }
            }
            Err(_) => break, // engine dropped
        }
    }

    // Disconnect
    let _ = engine_tx
        .send(EngineMsg {
            conn_id,
            payload: EnginePayload::Disconnect,
        })
        .await;
}

mod tick;

#[cfg(test)]
mod tests;
