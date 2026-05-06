use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

pub const MAX_TUBE_NAME_LEN: usize = 200;
pub const URGENT_THRESHOLD: u32 = 1024;
pub const JOB_DATA_SIZE_LIMIT_DEFAULT: u32 = 65535;
pub const JOB_DATA_SIZE_LIMIT_MAX: u32 = 1_073_741_824;

/// Logical identifier for a body stored in the (future) external body store.
/// Monotonic and never reused. Phase 2 placeholder — only `BodyRef::External`
/// will carry one, and that variant is not constructed yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BodyId(pub u64);

/// Where a job's body lives.
///
/// In the current single-store design every body is `Inline`. The `External`
/// variant exists so that future TOAST work can swap in body-store references
/// without changing this enum's shape — for now any code path encountering
/// `External` panics via `unreachable!`.
#[derive(Debug, Clone)]
pub enum BodyRef {
    Inline(Vec<u8>),
    External(BodyId),
}

impl BodyRef {
    /// Borrow the inline body bytes. Panics on `External` — Phase 2 invariant
    /// is that `External` is never constructed.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            BodyRef::Inline(v) => v,
            BodyRef::External(_) => unreachable!("body store not yet enabled"),
        }
    }

    /// Number of body bytes resident in RAM. `Inline` returns the actual
    /// body length; `External` returns 0 because those bytes live in the
    /// body store, not in this `Job`. Drives in-memory budget accounting.
    pub fn len(&self) -> usize {
        match self {
            BodyRef::Inline(v) => v.len(),
            BodyRef::External(_) => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Ready,
    Reserved,
    Delayed,
    Buried,
}

impl JobState {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobState::Ready => "ready",
            JobState::Reserved => "reserved",
            JobState::Delayed => "delayed",
            JobState::Buried => "buried",
        }
    }

    pub fn as_protocol_str(&self) -> &'static str {
        match self {
            JobState::Ready => "READY",
            JobState::Reserved => "RESERVED",
            JobState::Delayed => "DELAYED",
            JobState::Buried => "BURIED",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: u64,
    pub priority: u32,
    pub delay: Duration,
    pub ttr: Duration,
    pub body: BodyRef,
    pub state: JobState,
    pub tube_name: String,

    // Future feature extension fields
    pub idempotency_key: Option<(String, u32)>,
    pub group: Option<String>,
    pub after_group: Option<String>,
    pub concurrency_key: Option<(String, u32)>,

    pub created_at: Instant,
    /// For delayed jobs: when it becomes ready.
    /// For reserved jobs: when TTR expires.
    pub deadline_at: Option<Instant>,

    /// Connection ID of the reserver, if reserved.
    pub reserver_id: Option<u64>,

    /// When the job was last reserved (for processing time tracking).
    pub reserved_at: Option<Instant>,

    // Counters
    pub reserve_ct: u32,
    pub timeout_ct: u32,
    pub release_ct: u32,
    pub bury_ct: u32,
    pub kick_ct: u32,

    // WAL persistence fields
    pub wal_file_seq: Option<u64>,
    pub wal_used: usize,
    pub created_at_epoch: u64,
}

impl Job {
    pub fn new(
        id: u64,
        priority: u32,
        delay: Duration,
        ttr: Duration,
        body: Vec<u8>,
        tube_name: String,
    ) -> Self {
        let now = Instant::now();
        let (state, deadline_at) = if delay.is_zero() {
            (JobState::Ready, None)
        } else {
            (JobState::Delayed, Some(now + delay))
        };

        Job {
            id,
            priority,
            delay,
            ttr,
            body: BodyRef::Inline(body),
            state,
            tube_name,
            idempotency_key: None,
            group: None,
            after_group: None,
            concurrency_key: None,
            created_at: now,
            deadline_at,
            reserver_id: None,
            reserved_at: None,
            reserve_ct: 0,
            timeout_ct: 0,
            release_ct: 0,
            bury_ct: 0,
            kick_ct: 0,
            wal_file_seq: None,
            wal_used: 0,
            created_at_epoch: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Returns the priority sort key for the ready heap: (priority, id).
    /// Lower priority number = higher urgency. Ties broken by lower id (FIFO).
    pub fn ready_key(&self) -> (u32, u64) {
        (self.priority, self.id)
    }

    pub fn is_urgent(&self) -> bool {
        self.priority < URGENT_THRESHOLD
    }

    pub fn body_size(&self) -> usize {
        self.body.len()
    }

    /// Borrow the body bytes. Panics on `External` (Phase 2 invariant).
    pub fn body_bytes(&self) -> &[u8] {
        self.body.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_job(id: u64, pri: u32) -> Job {
        Job::new(
            id,
            pri,
            Duration::ZERO,
            Duration::from_secs(1),
            vec![],
            "default".into(),
        )
    }

    // Mirrors cttest_job_creation
    #[test]
    fn test_job_creation() {
        let j = make_test_job(1, 10);
        assert_eq!(j.id, 1);
        assert_eq!(j.priority, 10);
        assert_eq!(j.state, JobState::Ready);
        assert_eq!(j.tube_name, "default");
    }

    // Mirrors cttest_job_cmp_pris
    #[test]
    fn test_job_cmp_priorities() {
        let j1 = make_test_job(1, 1);
        let j2 = make_test_job(2, 2);
        // Lower priority number is "less" (higher urgency)
        assert!(j1.ready_key() < j2.ready_key());
    }

    // Mirrors cttest_job_cmp_ids
    #[test]
    fn test_job_cmp_ids() {
        let j1 = make_test_job(1, 1);
        let j2 = make_test_job(2, 1);
        // Same priority, lower ID wins
        assert!(j1.ready_key() < j2.ready_key());
    }

    // Mirrors cttest_job_large_pris
    #[test]
    fn test_job_large_priorities() {
        let j1 = make_test_job(1, 0);
        let j2 = make_test_job(2, u32::MAX);
        assert!(j1.ready_key() < j2.ready_key());
    }

    // Mirrors cttest_job_hash_free -- in Rust this is just HashMap remove
    #[test]
    fn test_job_hash_free() {
        use std::collections::HashMap;
        let mut jobs: HashMap<u64, Job> = HashMap::new();
        let j = make_test_job(1, 1);
        jobs.insert(j.id, j);
        assert_eq!(jobs.len(), 1);
        jobs.remove(&1);
        assert_eq!(jobs.len(), 0);
    }

    // Mirrors cttest_job_hash_free_next
    #[test]
    fn test_job_hash_free_chain() {
        use std::collections::HashMap;
        let mut jobs: HashMap<u64, Job> = HashMap::new();
        let j1 = make_test_job(1, 1);
        let j2 = make_test_job(2, 1);
        jobs.insert(j1.id, j1);
        jobs.insert(j2.id, j2);
        assert_eq!(jobs.len(), 2);

        jobs.remove(&1);
        assert_eq!(jobs.len(), 1);
        assert!(jobs.contains_key(&2));

        jobs.remove(&2);
        assert_eq!(jobs.len(), 0);
    }

    // Mirrors cttest_job_all_jobs_used
    #[test]
    fn test_job_count() {
        use std::collections::HashMap;
        let mut jobs: HashMap<u64, Job> = HashMap::new();
        for i in 1..=5 {
            jobs.insert(i, make_test_job(i, 1));
        }
        assert_eq!(jobs.len(), 5);
        jobs.remove(&3);
        assert_eq!(jobs.len(), 4);
    }

    // Mirrors cttest_job_100_000_jobs
    #[test]
    fn test_job_100k_jobs() {
        use std::collections::HashMap;
        let mut jobs: HashMap<u64, Job> = HashMap::new();
        let n = 100_000u64;
        for i in 1..=n {
            jobs.insert(i, make_test_job(i, 1));
        }
        assert_eq!(jobs.len(), n as usize);

        for i in 1..=n {
            jobs.remove(&i);
        }
        assert_eq!(jobs.len(), 0);
    }

    #[test]
    fn test_job_state_str() {
        assert_eq!(JobState::Ready.as_str(), "ready");
        assert_eq!(JobState::Reserved.as_str(), "reserved");
        assert_eq!(JobState::Delayed.as_str(), "delayed");
        assert_eq!(JobState::Buried.as_str(), "buried");
    }

    #[test]
    fn test_job_reserved_at_none_on_creation() {
        let j = make_test_job(1, 10);
        assert!(j.reserved_at.is_none());
    }

    #[test]
    fn test_body_ref_inline_round_trip() {
        let job = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(1),
            b"hello-toast".to_vec(),
            "default".into(),
        );
        // Constructor wraps incoming bytes in BodyRef::Inline.
        assert!(matches!(job.body, BodyRef::Inline(_)));
        assert_eq!(job.body_bytes(), b"hello-toast");
        assert_eq!(job.body_size(), 11);
    }

    #[test]
    #[should_panic(expected = "body store not yet enabled")]
    fn test_body_ref_external_unreachable() {
        // External is defined but no code path constructs it in Phase 2.
        // Touching its bytes panics with the documented message.
        let body = BodyRef::External(BodyId(123));
        let _ = body.as_bytes();
    }

    #[test]
    fn test_job_urgent() {
        let urgent = make_test_job(1, 0);
        assert!(urgent.is_urgent());

        let urgent2 = make_test_job(2, 1023);
        assert!(urgent2.is_urgent());

        let not_urgent = make_test_job(3, 1024);
        assert!(!not_urgent.is_urgent());
    }
}
