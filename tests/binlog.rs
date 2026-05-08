use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

struct TestServer {
    port: u16,
    handle: JoinHandle<()>,
    wal_dir: PathBuf,
}

impl TestServer {
    async fn start_with_wal(dir: &Path) -> Self {
        Self::start_with_wal_and_max_job_size(dir, 65535).await
    }

    async fn start_with_wal_and_max_job_size(dir: &Path, max_job_size: u32) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let wal_dir = dir.to_path_buf();
        let wal_dir_clone = wal_dir.clone();

        let handle = tokio::spawn(async move {
            tuber::server::run_with_listener(
                listener,
                max_job_size,
                Some(wal_dir_clone.as_path()),
                None,
            )
            .await
            .ok();
        });

        // Give the server a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        TestServer {
            port,
            handle,
            wal_dir,
        }
    }

    /// Start with an explicit `--max-storage-bytes` budget. Used to
    /// exercise the disk-budget enforcement (`OUT_OF_STORAGE`).
    async fn start_with_wal_and_storage_budget(dir: &Path, max_storage_bytes: u64) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let wal_dir = dir.to_path_buf();
        let wal_dir_clone = wal_dir.clone();

        let handle = tokio::spawn(async move {
            tuber::server::run_with_listener_limited(
                listener,
                65535,
                None,
                Some(max_storage_bytes),
                Some(wal_dir_clone.as_path()),
                true, // migrate legacy WALs in tests by default
                None,
            )
            .await
            .ok();
        });

        // Give the server a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        TestServer {
            port,
            handle,
            wal_dir,
        }
    }

    /// Start with an explicit `max_jobs_size` budget. Used to exercise the
    /// replay pre-check in src/server.rs:build_state.
    async fn try_start_with_wal_and_max_jobs_size(
        dir: &Path,
        max_jobs_size: u64,
    ) -> Result<Self, std::io::Error> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let wal_dir = dir.to_path_buf();
        let wal_dir_clone = wal_dir.clone();

        // Startup error (e.g. replay over budget) is delivered via the oneshot;
        // the spawned task reports it before we even try to connect.
        let (err_tx, err_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let result = tuber::server::run_with_listener_limited(
                listener,
                65535,
                Some(max_jobs_size),
                Some(1024 * 1024 * 1024), // generous default budget; this test exercises max_jobs_size, not storage
                Some(wal_dir_clone.as_path()),
                true, // tests can carry pre-v5 fixtures; allow migration
                None,
            )
            .await;
            let _ = err_tx.send(result);
        });

        // Give the server a moment to either come up or fail replay.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // If the task has already finished with an error, surface it.
        if handle.is_finished() {
            match err_rx.await {
                Ok(Err(e)) => return Err(e),
                Ok(Ok(())) => {}
                Err(_) => {}
            }
        }

        Ok(TestServer {
            port,
            handle,
            wal_dir,
        })
    }

    /// Start with `--sync-interval 0` (strictest durability). Used by the
    /// group-commit throughput test — verifies batching keeps the ceiling
    /// well above the per-put-fsync floor.
    async fn start_with_wal_sync_zero(dir: &Path) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let wal_dir = dir.to_path_buf();
        let wal_dir_clone = wal_dir.clone();

        let handle = tokio::spawn(async move {
            tuber::server::run_with_listener_sync_zero(listener, 65535, wal_dir_clone.as_path())
                .await
                .ok();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        TestServer {
            port,
            handle,
            wal_dir,
        }
    }

    /// Try to start with a specific `migrate_wal` setting, surfacing the
    /// startup error if any. Used by the legacy-WAL refusal tests.
    async fn try_start_with_migrate_wal(
        dir: &Path,
        migrate_wal: bool,
    ) -> Result<Self, std::io::Error> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let wal_dir = dir.to_path_buf();
        let wal_dir_clone = wal_dir.clone();

        let (err_tx, err_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let result = tuber::server::run_with_listener_limited(
                listener,
                65535,
                None,
                Some(1024 * 1024 * 1024), // generous default budget; legacy-WAL tests don't exercise storage
                Some(wal_dir_clone.as_path()),
                migrate_wal,
                None,
            )
            .await;
            let _ = err_tx.send(result);
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        if handle.is_finished() {
            match err_rx.await {
                Ok(Err(e)) => return Err(e),
                Ok(Ok(())) => {}
                Err(_) => {}
            }
        }

        Ok(TestServer {
            port,
            handle,
            wal_dir,
        })
    }

    async fn connect(&self) -> TestConn {
        // Retry connection a few times (server may still be starting)
        let mut last_err = None;
        for _ in 0..20 {
            match TcpStream::connect(("127.0.0.1", self.port)).await {
                Ok(stream) => {
                    stream.set_nodelay(true).unwrap();
                    let (reader, writer) = stream.into_split();
                    return TestConn {
                        reader: BufReader::new(reader),
                        writer,
                    };
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        panic!("could not connect after retries: {:?}", last_err);
    }

    fn shutdown(self) -> PathBuf {
        self.handle.abort();
        self.wal_dir.clone()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

struct TestConn {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestConn {
    async fn mustsend(&mut self, s: &str) {
        self.writer.write_all(s.as_bytes()).await.unwrap();
    }

    async fn ckresp(&mut self, expected: &str) {
        let line = self.readline().await;
        assert_eq!(expected, line, "expected {:?}, got {:?}", expected, line);
    }

    async fn readline(&mut self) -> String {
        let mut buf = String::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                self.reader.read_line(&mut buf).await.unwrap();
                if buf.ends_with("\r\n") {
                    break;
                }
            }
        })
        .await
        .expect("readline timed out after 5s");
        buf
    }

    async fn read_ok_body(&mut self) -> String {
        let header = self.readline().await;
        assert!(header.starts_with("OK "), "expected OK, got {:?}", header);
        let len: usize = header
            .trim_end()
            .strip_prefix("OK ")
            .unwrap()
            .parse()
            .unwrap();
        let mut body_buf = vec![0u8; len + 2];
        tokio::time::timeout(
            Duration::from_secs(5),
            self.reader.read_exact(&mut body_buf),
        )
        .await
        .expect("read_ok_body timed out")
        .unwrap();
        String::from_utf8_lossy(&body_buf[..len]).to_string()
    }
}

// ---------------------------------------------------------------------------
// WAL integration tests
// ---------------------------------------------------------------------------

/// Test 1: Basic put, restart, peek finds job, delete works
#[tokio::test]
async fn test_binlog_basic() {
    let dir = tempfile::tempdir().unwrap();

    // Start server, put a job
    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 10 0 60 5\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Shutdown
    drop(c);
    let wal_dir = srv.shutdown();

    // Wait for shutdown
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart
    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    // Job should still exist
    c2.mustsend("peek 1\r\n").await;
    c2.ckresp("FOUND 1 5\r\n").await;
    c2.ckresp("hello\r\n").await;

    // Delete should work
    c2.mustsend("delete 1\r\n").await;
    c2.ckresp("DELETED\r\n").await;

    // After delete, peek should not find it
    c2.mustsend("peek 1\r\n").await;
    c2.ckresp("NOT_FOUND\r\n").await;
}

/// Test 2: Put, reserve, bury, restart, peek-buried finds it
#[tokio::test]
async fn test_binlog_bury() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 10 0 60 5\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    c.mustsend("bury 1 10\r\n").await;
    c.ckresp("BURIED\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("peek-buried\r\n").await;
    c2.ckresp("FOUND 1 5\r\n").await;
    c2.ckresp("hello\r\n").await;
}

/// Test 3: Put to named tube, release with new pri, restart, verify state
#[tokio::test]
async fn test_binlog_read() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("use mytube\r\n").await;
    c.ckresp("USING mytube\r\n").await;

    c.mustsend("put 100 0 60 4\r\n").await;
    c.mustsend("data\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("watch mytube\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 4\r\n").await;
    c.ckresp("data\r\n").await;

    // Release with new priority
    c.mustsend("release 1 50 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("state: ready"), "body: {}", body);
    assert!(body.contains("pri: 50"), "body: {}", body);
    assert!(body.contains("tube: \"mytube\""), "body: {}", body);
}

/// Test 4: Small max_file_size, put many jobs, verify multiple files created
#[tokio::test]
async fn test_binlog_size_limit() {
    let dir = tempfile::tempdir().unwrap();

    // Use a very small max_file_size (we can't set it via the public API directly,
    // but we can verify WAL files are created by checking the directory)
    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    // Put several jobs
    for i in 0..20 {
        let body = format!("job-body-{:04}", i);
        c.mustsend(&format!("put 0 0 60 {}\r\n", body.len())).await;
        c.mustsend(&format!("{}\r\n", body)).await;
        c.ckresp(&format!("INSERTED {}\r\n", i + 1)).await;
    }

    // Verify WAL files exist
    let wal_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("binlog."))
        .collect();
    assert!(
        !wal_files.is_empty(),
        "expected WAL files in {:?}",
        dir.path()
    );

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart and verify all jobs survived
    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    for i in 0..20 {
        c2.mustsend(&format!("peek {}\r\n", i + 1)).await;
        let line = c2.readline().await;
        assert!(
            line.starts_with(&format!("FOUND {}", i + 1)),
            "expected FOUND for job {}, got {:?}",
            i + 1,
            line
        );
        let _ = c2.readline().await; // body
    }
}

/// Test 5: Put 96 jobs, delete all, verify no crash
#[tokio::test]
async fn test_binlog_allocation() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    for i in 0..96 {
        c.mustsend("put 0 0 60 1\r\n").await;
        c.mustsend("x\r\n").await;
        c.ckresp(&format!("INSERTED {}\r\n", i + 1)).await;
    }

    for i in 0..96 {
        c.mustsend(&format!("delete {}\r\n", i + 1)).await;
        c.ckresp("DELETED\r\n").await;
    }

    // Server should still work
    c.mustsend("put 0 0 60 1\r\n").await;
    c.mustsend("y\r\n").await;
    c.ckresp("INSERTED 97\r\n").await;
}

/// Test 6: Reserved job replays as ready
#[tokio::test]
async fn test_binlog_reserved_replayed_as_ready() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 4\r\n").await;
    c.mustsend("data\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 4\r\n").await;
    c.ckresp("data\r\n").await;

    // Job is now reserved. Restart without releasing.
    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    // Job should be replayed as ready (not reserved)
    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("state: ready"), "body: {}", body);
}

/// Test 7: Delayed job survives restart
#[tokio::test]
async fn test_binlog_delayed_job() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 3600 60 4\r\n").await;
    c.mustsend("data\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("state: delayed"), "body: {}", body);
}

/// Test 8: Put with extension fields, restart, stats-job shows them
#[tokio::test]
async fn test_binlog_extension_fields() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 4 idp:mykey grp:g1 aft:g0 con:c1\r\n")
        .await;
    c.mustsend("data\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("idempotency-key: mykey"), "body: {}", body);
    assert!(body.contains("group: g1"), "body: {}", body);
    assert!(body.contains("after-group: g0"), "body: {}", body);
    assert!(body.contains("concurrency-key: c1"), "body: {}", body);
}

/// Test 9: Multiple puts and deletes, restart preserves only surviving jobs
#[tokio::test]
async fn test_binlog_put_delete_restart() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    // Put 5 jobs
    for i in 1..=5 {
        let body = format!("body{}", i);
        c.mustsend(&format!("put 0 0 60 {}\r\n", body.len())).await;
        c.mustsend(&format!("{}\r\n", body)).await;
        c.ckresp(&format!("INSERTED {}\r\n", i)).await;
    }

    // Delete jobs 2 and 4
    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;
    c.mustsend("delete 4\r\n").await;
    c.ckresp("DELETED\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    // Jobs 1, 3, 5 should exist
    for &id in &[1, 3, 5] {
        c2.mustsend(&format!("peek {}\r\n", id)).await;
        let line = c2.readline().await;
        assert!(
            line.starts_with(&format!("FOUND {}", id)),
            "expected FOUND for job {}, got {:?}",
            id,
            line
        );
        let _ = c2.readline().await;
    }

    // Jobs 2, 4 should not exist
    for &id in &[2, 4] {
        c2.mustsend(&format!("peek {}\r\n", id)).await;
        c2.ckresp("NOT_FOUND\r\n").await;
    }

    // Next job ID should be 6
    c2.mustsend("put 0 0 60 1\r\n").await;
    c2.mustsend("x\r\n").await;
    c2.ckresp("INSERTED 6\r\n").await;
}

/// Test 10: Kick survives restart
#[tokio::test]
async fn test_binlog_kick_restart() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    // Put, reserve, bury
    c.mustsend("put 10 0 60 5\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    c.mustsend("bury 1 10\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // Kick it back to ready
    c.mustsend("kick-job 1\r\n").await;
    c.ckresp("KICKED\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    // Should be ready after restart
    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("state: ready"), "body: {}", body);
}

/// Test 11: Idempotency key survives WAL replay
#[tokio::test]
async fn test_wal_replay_idempotency() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart server — idempotency index should be rebuilt from WAL
    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    // Same key should return same ID with state
    c2.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c2.mustsend("world\r\n").await;
    c2.ckresp("INSERTED 1 READY\r\n").await;

    // Different key should get new ID
    c2.mustsend("put 0 0 60 5 idp:key2\r\n").await;
    c2.mustsend("other\r\n").await;
    c2.ckresp("INSERTED 2\r\n").await;
}

/// Test 12: Concurrency limit >1 survives WAL replay
///
/// Before the fix, restore_jobs() never populated concurrency_limits,
/// so after restart is_concurrency_blocked() would default to limit 1
/// instead of the configured limit (e.g. 3).
///
/// The bug: cmd_put() registers the limit at put-time, but restore_jobs()
/// doesn't. After restart, acquire_concurrency_key() lazily sets the limit
/// on the first reserve — but is_concurrency_blocked() runs BEFORE acquire,
/// so once 1 job is reserved (count=1), the next check sees count(1) >= limit(1)
/// and blocks. The limit never gets a chance to update to 3.
#[tokio::test]
async fn test_wal_replay_concurrency_limit() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    // Put 3 jobs with con:api:3 (limit 3)
    for i in 1..=3 {
        let body = format!("{}", i);
        c.mustsend(&format!("put 0 0 60 {} con:api:3\r\n", body.len()))
            .await;
        c.mustsend(&format!("{}\r\n", body)).await;
        c.ckresp(&format!("INSERTED {}\r\n", i)).await;
    }

    // Before restart: reserve 1 job so count=1, then verify 2nd is still allowed
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("2\r\n").await;

    // Release both so all 3 are ready for restart
    c.mustsend("release 1 0 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;
    c.mustsend("release 2 0 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart — concurrency_limits is now empty
    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    // Reserve first job — this always works (count=0 < default limit=1)
    // acquire_concurrency_key sets count=1, limit=3
    c2.mustsend("reserve-with-timeout 0\r\n").await;
    let line = c2.readline().await;
    assert!(
        line.starts_with("RESERVED"),
        "expected RESERVED, got {:?}",
        line
    );
    let _ = c2.readline().await;

    // Reserve second job — BUG: without the fix, is_concurrency_blocked sees
    // count=1 >= limit=1 (default) and blocks, even though limit should be 3.
    // With the fix, limit is correctly restored to 3 so count=1 < 3 passes.
    c2.mustsend("reserve-with-timeout 0\r\n").await;
    let line = c2.readline().await;
    assert!(
        line.starts_with("RESERVED"),
        "expected second RESERVED after restart, got {:?} (concurrency limit not restored)",
        line
    );
    let _ = c2.readline().await;

    // Reserve third — should also succeed (count=2 < limit=3)
    c2.mustsend("reserve-with-timeout 0\r\n").await;
    let line = c2.readline().await;
    assert!(
        line.starts_with("RESERVED"),
        "expected third RESERVED after restart, got {:?}",
        line
    );
    let _ = c2.readline().await;
}

/// Replay aborts with a diagnostic error if the on-disk binlog is larger than
/// --max-jobs-size. This is the operator-facing safety net for the migration
/// case (upgrading from an unlimited previous run to a newly-bounded one).
#[tokio::test]
async fn test_binlog_replay_aborts_when_over_budget() {
    let dir = tempfile::tempdir().unwrap();

    // First run: no limit, put several jobs so the binlog is non-trivial.
    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;
    for i in 0..5 {
        c.mustsend("put 0 0 60 100\r\n").await;
        c.mustsend(&format!("{}\r\n", "x".repeat(100))).await;
        c.ckresp(&format!("INSERTED {}\r\n", i + 1)).await;
    }
    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Second run: tight --max-jobs-size (100 bytes) must fail replay loudly.
    let result = TestServer::try_start_with_wal_and_max_jobs_size(&wal_dir, 100).await;
    let err = result
        .err()
        .expect("expected replay to abort with an error");
    assert_eq!(err.kind(), std::io::ErrorKind::OutOfMemory);
    let msg = err.to_string();
    assert!(
        msg.contains("--max-jobs-size"),
        "error message should mention --max-jobs-size: {msg}"
    );
    assert!(
        msg.contains("in-memory"),
        "error message should mention in-memory size: {msg}"
    );

    // Third run: generous budget — all five jobs replay successfully.
    let srv3 = TestServer::try_start_with_wal_and_max_jobs_size(&wal_dir, 10 * 1024 * 1024)
        .await
        .expect("generous budget should allow replay");
    let mut c3 = srv3.connect().await;
    c3.mustsend("stats\r\n").await;
    let body = c3.read_ok_body().await;
    assert!(
        body.contains("current-jobs-ready: 5"),
        "expected 5 ready jobs after replay: {body}"
    );
}

/// Replay aborts when a small-body workload blows the in-memory budget.
/// The WAL on-disk size can be 3–5× under the actual in-memory cost when
/// bodies are tiny, because per-job overhead (HashMap bucket, String headers,
/// allocator slack) is not represented on disk.
#[tokio::test]
async fn test_binlog_replay_aborts_when_in_memory_exceeds_budget() {
    let dir = tempfile::tempdir().unwrap();

    // First run: no limit. Many tiny jobs — the regime where on-disk size
    // (~105 B/job) drastically understates RAM cost (~522 B/job for a 10-byte
    // body in the "default" tube).
    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;
    for i in 0..50 {
        c.mustsend("put 0 0 60 10\r\n").await;
        c.mustsend(&format!("{}\r\n", "x".repeat(10))).await;
        c.ckresp(&format!("INSERTED {}\r\n", i + 1)).await;
    }
    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Second run: budget chosen between disk size (~5.3 KB) and in-memory
    // size (~26 KB). Pre-flight passes; post-replay check must fire.
    let result = TestServer::try_start_with_wal_and_max_jobs_size(&wal_dir, 10_000).await;
    let err = result
        .err()
        .expect("expected post-replay check to abort with an error");
    assert_eq!(err.kind(), std::io::ErrorKind::OutOfMemory);
    let msg = err.to_string();
    assert!(
        msg.contains("--max-jobs-size"),
        "error message should mention --max-jobs-size: {msg}"
    );
    assert!(
        msg.contains("in-memory"),
        "error message should mention in-memory size: {msg}"
    );

    // Third run: generous budget — all 50 jobs replay successfully.
    let srv3 = TestServer::try_start_with_wal_and_max_jobs_size(&wal_dir, 1024 * 1024)
        .await
        .expect("generous budget should allow replay");
    let mut c3 = srv3.connect().await;
    c3.mustsend("stats\r\n").await;
    let body = c3.read_ok_body().await;
    assert!(
        body.contains("current-jobs-ready: 50"),
        "expected 50 ready jobs after replay: {body}"
    );
}

/// Test: Kicked job preserves its priority through WAL replay.
#[tokio::test]
async fn test_binlog_kick_preserves_priority() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 100 0 60 5\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    // Bury with a *different* priority (50)
    c.mustsend("bury 1 50\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // Kick it back to ready
    c.mustsend("kick-job 1\r\n").await;
    c.ckresp("KICKED\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("state: ready"), "body: {}", body);
    assert!(
        body.contains("pri: 50"),
        "kicked job should preserve its priority (50): {}",
        body
    );
}

/// Test: Auto-released (timed-out) job preserves its priority through WAL replay.
#[tokio::test]
async fn test_binlog_timeout_preserves_priority() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    // ttr = 1 second
    c.mustsend("put 100 0 1 5\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    // Wait for TTR to expire
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Send another command to trigger process_queue (and thus the timeout)
    c.mustsend("put 0 0 60 1\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("stats-job 1\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(body.contains("state: ready"), "body: {}", body);
    assert!(
        body.contains("pri: 100"),
        "timed-out job should preserve its original priority (100): {}",
        body
    );
}

/// Test: graceful SIGTERM flushes WAL before exit.
///
/// Runs the server as a child process so we can deliver a real SIGTERM
/// (in-process tests can't — tokio's signal handler is shared with the
/// test harness).
#[tokio::test]
async fn test_binlog_sigterm_flushes_wal() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().to_path_buf();

    let tmp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tmp_listener.local_addr().unwrap().port();
    drop(tmp_listener);

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_tuber"))
        .args([
            "server",
            "-l",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-b",
            wal_path.to_str().unwrap(),
            "--max-storage-bytes",
            "1g",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start tuber subprocess");

    let mut connected = false;
    for _ in 0..40 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(connected, "server did not start accepting connections");

    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let (read_half, write_half) = stream.into_split();
    let mut c = TestConn {
        reader: BufReader::new(read_half),
        writer: write_half,
    };
    c.mustsend("put 10 0 60 12\r\n").await;
    c.mustsend("sigterm-test\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    drop(c);

    let pid = child.id().expect("no child pid");
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }

    // SIGTERM exits with a signal status, not 0 — we only assert it exited.
    let _ = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("server did not exit within 5s after SIGTERM")
        .expect("failed to wait on child");

    let srv2 = TestServer::start_with_wal(&wal_path).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("peek 1\r\n").await;
    c2.ckresp("FOUND 1 12\r\n").await;
    c2.ckresp("sigterm-test\r\n").await;
}

/// TOAST: when -b is set, a `toast/` subdirectory exists and bodies land in
/// segment files. Confirms the body-store integration is wired into the
/// startup path, not just the in-memory tests.
#[tokio::test]
async fn test_binlog_toast_directory_created() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    drop(c);
    let _ = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let toast_dir = dir.path().join("toast");
    assert!(toast_dir.is_dir(), "toast/ subdirectory must exist");

    // Exactly one segment, named body.000000.
    let entries: Vec<_> = std::fs::read_dir(&toast_dir).unwrap().collect();
    assert_eq!(entries.len(), 1, "expected one segment file");
    let name = entries[0].as_ref().unwrap().file_name();
    assert_eq!(name.to_string_lossy(), "body.000000");

    // 16 (file header) + 20 (body header) + 5 (body) = 41 bytes.
    let len = std::fs::metadata(toast_dir.join("body.000000")).unwrap().len();
    assert_eq!(len, 41, "segment size should match header(16) + body_hdr(20) + body(5)");
}

/// TOAST: many puts span multiple segments and all bodies survive a
/// restart (header-scan recovery rebuilds the BodyId index).
#[tokio::test]
async fn test_binlog_toast_many_bodies_restart() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    // 50 puts of varying-size bodies. With the default 64 MiB segment
    // size we won't trigger rotation, but we exercise the index path
    // for many distinct BodyIds.
    let n = 50;
    for i in 0..n {
        let body = format!("body-{:03}-payload", i);
        let len = body.len();
        c.mustsend(&format!("put 0 0 60 {}\r\n", len)).await;
        c.mustsend(&format!("{}\r\n", body)).await;
        c.ckresp(&format!("INSERTED {}\r\n", i + 1)).await;
    }

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart and confirm every body still peekable with original bytes.
    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    for i in 0..n {
        let expected = format!("body-{:03}-payload", i);
        let len = expected.len();
        c2.mustsend(&format!("peek {}\r\n", i + 1)).await;
        c2.ckresp(&format!("FOUND {} {}\r\n", i + 1, len)).await;
        c2.ckresp(&format!("{}\r\n", expected)).await;
    }
}

/// TOAST: an 8 KiB printable body round-trips through put → restart →
/// reserve, exercising the body-store read on a payload bigger than a
/// kernel readahead might serve in a single page.
#[tokio::test]
async fn test_binlog_toast_large_body_restart() {
    let dir = tempfile::tempdir().unwrap();

    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;

    // Repeating ASCII pattern so the readline-based test helper still works.
    // The pattern itself doesn't repeat at small periods, so a substring
    // mismatch would surface immediately.
    let pattern = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@";
    let body: String = pattern.repeat(128); // 64 * 128 = 8192 bytes
    let len = body.len();

    c.mustsend(&format!("put 0 0 60 {}\r\n", len)).await;
    c.mustsend(&format!("{}\r\n", body)).await;
    c.ckresp("INSERTED 1\r\n").await;

    drop(c);
    let wal_dir = srv.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let srv2 = TestServer::start_with_wal(&wal_dir).await;
    let mut c2 = srv2.connect().await;

    c2.mustsend("reserve-with-timeout 1\r\n").await;
    c2.ckresp(&format!("RESERVED 1 {}\r\n", len)).await;
    c2.ckresp(&format!("{}\r\n", body)).await;
}

/// `--max-storage-bytes`: once the combined WAL+TOAST footprint plus a
/// one-segment WAL reserve (≈10 MB) would exceed the budget, new puts
/// are refused with `OUT_OF_STORAGE`. State changes (delete) always
/// succeed regardless of the budget — they're how the operator unsticks
/// a wedged queue.
#[tokio::test]
async fn test_max_storage_bytes_returns_out_of_storage() {
    let dir = tempfile::tempdir().unwrap();

    // 13 MB budget: ~10 MB reserved for the WAL, ~3 MB headroom for
    // bodies and WAL data. ~50 puts of 60 KiB should trip the cap.
    let srv = TestServer::start_with_wal_and_storage_budget(dir.path(), 13 * 1024 * 1024).await;
    let mut c = srv.connect().await;

    let body = "x".repeat(60 * 1024); // 60 KiB — fits within default max-job-size
    let len = body.len();
    let mut accepted = 0u32;
    let mut hit = false;
    for _ in 0..200 {
        c.mustsend(&format!("put 0 0 60 {}\r\n", len)).await;
        c.mustsend(&format!("{}\r\n", body)).await;
        let resp = c.readline().await;
        if resp == "OUT_OF_STORAGE\r\n" {
            hit = true;
            break;
        }
        assert!(
            resp.starts_with("INSERTED "),
            "unexpected response: {:?}",
            resp
        );
        accepted += 1;
    }
    assert!(
        hit,
        "expected OUT_OF_STORAGE within 200 puts, got {} accepted",
        accepted
    );
    assert!(accepted >= 1, "at least one put should land before the cap");

    // State changes never refused: delete an inserted job and confirm
    // the response is DELETED, not OUT_OF_STORAGE.
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;
}

/// `--max-storage-bytes` is mandatory whenever `-b` is set. Trying to
/// start a server with a WAL directory but no storage budget must fail
/// with a clear error pointing at the missing flag.
#[tokio::test]
async fn test_storage_budget_required_with_binlog() {
    use std::path::Path;
    let dir = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let result = tuber::server::run_with_listener_limited(
        listener,
        65535,
        None,
        None, // no storage budget
        Some(Path::new(dir.path())),
        true,
        None,
    )
    .await;
    let err = result.expect_err("must reject -b without --max-storage-bytes");
    let msg = format!("{err}");
    assert!(
        msg.contains("--max-storage-bytes"),
        "error message must name the missing flag, got: {msg}"
    );
}

/// Hand-craft a v4 WAL file containing one inline-body FullJob record. We
/// can't go through `serialize_full_job` because that path now requires
/// External; this mirrors the v4 layout byte-for-byte so the migration
/// path has something realistic to chew on.
fn write_legacy_v4_wal(dir: &Path, job_id: u64, body: &[u8]) {
    let mut payload: Vec<u8> = Vec::new();
    payload.extend_from_slice(&0u32.to_le_bytes()); // priority
    payload.extend_from_slice(&0u64.to_le_bytes()); // delay_nanos
    payload.extend_from_slice(&60_000_000_000u64.to_le_bytes()); // ttr 60s
    payload.extend_from_slice(&0u64.to_le_bytes()); // created_at_epoch
    payload.push(0); // state = Ready
    for _ in 0..5 {
        payload.extend_from_slice(&0u32.to_le_bytes()); // counters
    }
    let tube = b"default";
    payload.extend_from_slice(&(tube.len() as u16).to_le_bytes());
    payload.extend_from_slice(tube);
    payload.extend_from_slice(&0u16.to_le_bytes()); // idp_key None
    payload.extend_from_slice(&0u32.to_le_bytes()); // idp_ttl
    payload.extend_from_slice(&0u16.to_le_bytes()); // group None
    payload.extend_from_slice(&0u16.to_le_bytes()); // after_group None
    payload.extend_from_slice(&0u16.to_le_bytes()); // concurrency_key None
    payload.extend_from_slice(&0u32.to_le_bytes()); // concurrency_limit
    payload.extend_from_slice(&(body.len() as u32).to_le_bytes()); // body_len
    payload.extend_from_slice(body); // body bytes (v4 inline)

    let mut record: Vec<u8> = Vec::new();
    record.push(0x01); // FULL_JOB
    record.extend_from_slice(&job_id.to_le_bytes());
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&payload);
    let crc = crc32fast::hash(&record);
    record.extend_from_slice(&crc.to_le_bytes());

    let mut file: Vec<u8> = Vec::new();
    file.extend_from_slice(b"TWAL");
    file.extend_from_slice(&4u32.to_le_bytes()); // legacy version
    file.extend_from_slice(&0u32.to_le_bytes()); // flags
    file.extend_from_slice(&record);

    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join("binlog.000001"), file).unwrap();
}

/// Without `--migrate-wal`, a server pointed at a directory containing a
/// pre-v5 WAL must refuse to start. The error message mentions the legacy
/// version and the flag the operator should pass.
#[tokio::test]
async fn test_legacy_wal_refuses_start_without_migrate_flag() {
    let dir = tempfile::tempdir().unwrap();
    write_legacy_v4_wal(dir.path(), 42, b"legacy-body");

    let result = TestServer::try_start_with_migrate_wal(dir.path(), false).await;
    let err = match result {
        Ok(_) => panic!("server must refuse a v4 WAL without --migrate-wal"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("v4") && msg.contains("--migrate-wal"),
        "error must explain the legacy version and the required flag, got: {msg}"
    );

    // Side-effect-free: the toast/ subdirectory should not have been created.
    assert!(
        !dir.path().join("toast").exists(),
        "refusing should not create the body store"
    );
}

/// With `--migrate-wal`, the same v4 WAL replays cleanly — its inline
/// body lifts into the body store and the job is reservable.
#[tokio::test]
async fn test_legacy_wal_migrates_when_flag_set() {
    let dir = tempfile::tempdir().unwrap();
    write_legacy_v4_wal(dir.path(), 42, b"legacy-body");

    let srv = TestServer::try_start_with_migrate_wal(dir.path(), true)
        .await
        .expect("migration must succeed");

    let mut c = srv.connect().await;

    // The replayed job is reservable; its body is what we wrote to the
    // legacy WAL, now served from the body store.
    c.mustsend("reserve-with-timeout 1\r\n").await;
    c.ckresp("RESERVED 42 11\r\n").await;
    c.ckresp("legacy-body\r\n").await;

    // The body store now has the migrated body on disk.
    let toast_dir = dir.path().join("toast");
    assert!(toast_dir.is_dir(), "migration must populate toast/");
    let entries: Vec<_> = std::fs::read_dir(&toast_dir).unwrap().collect();
    assert!(!entries.is_empty(), "toast/ must contain at least one segment");
}

/// `--migrate-wal` against an already-migrated (v5) WAL is a no-op: the
/// server starts cleanly without complaint.
#[tokio::test]
async fn test_migrate_flag_is_noop_on_current_format_wal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let srv = TestServer::start_with_wal(dir.path()).await;
        let mut c = srv.connect().await;
        c.mustsend("put 0 0 60 5\r\n").await;
        c.mustsend("hello\r\n").await;
        c.ckresp("INSERTED 1\r\n").await;
        drop(c);
        let _ = srv.shutdown();
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // Restart with --migrate-wal; nothing to migrate, but no error either.
    let srv = TestServer::try_start_with_migrate_wal(dir.path(), true)
        .await
        .expect("migrate-wal must be a no-op on a v5 WAL");
    let mut c = srv.connect().await;
    c.mustsend("peek 1\r\n").await;
    c.ckresp("FOUND 1 5\r\n").await;
    c.ckresp("hello\r\n").await;
}

/// Group commit: at `--sync-interval 0`, 8 concurrent producers each
/// issuing 50 puts must complete well above the per-put-fsync floor.
///
/// Pre-change baseline (per-put fsync): ~75 puts/sec ceiling regardless
/// of producer count, since the engine task serialises and each put pays
/// its own fsync. Post-change one fsync per drained batch covers all 8
/// producers' in-flight puts → hundreds of puts/sec on isolated runs.
///
/// Threshold of 90 ops/sec is set with `cargo test`'s parallelism in mind
/// — the full suite shares SSD fsync bandwidth across ~30 tests, so the
/// observed rate drops well below the standalone number. 90 still sits
/// meaningfully above the per-put-fsync ceiling and would catch a
/// regression that broke batching (which would then run *below* baseline
/// because it's competing with the rest of the suite for fsync slots).
#[tokio::test]
async fn test_group_commit_throughput_at_sync_zero() {
    let dir = tempfile::tempdir().unwrap();
    let srv = TestServer::start_with_wal_sync_zero(dir.path()).await;

    const PRODUCERS: usize = 8;
    const PUTS_EACH: usize = 50;
    const TOTAL: usize = PRODUCERS * PUTS_EACH;

    let port = srv.port;
    let started = std::time::Instant::now();

    let mut handles = Vec::with_capacity(PRODUCERS);
    for _ in 0..PRODUCERS {
        handles.push(tokio::spawn(async move {
            let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            stream.set_nodelay(true).unwrap();
            let (reader, writer) = stream.into_split();
            let mut c = TestConn {
                reader: BufReader::new(reader),
                writer,
            };
            for _ in 0..PUTS_EACH {
                c.mustsend("put 0 0 60 5\r\nhello\r\n").await;
                let line = c.readline().await;
                assert!(
                    line.starts_with("INSERTED "),
                    "unexpected response: {:?}",
                    line
                );
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let elapsed = started.elapsed();
    let ops_per_sec = TOTAL as f64 / elapsed.as_secs_f64();
    eprintln!(
        "group-commit throughput: {} puts in {:?} ({:.1} ops/sec)",
        TOTAL, elapsed, ops_per_sec
    );
    assert!(
        ops_per_sec >= 90.0,
        "throughput regressed below the group-commit floor: \
         {:.1} ops/sec (≥ 90 expected); per-put-fsync baseline is ~75 \
         on a single producer and falls further under suite parallelism",
        ops_per_sec
    );

    // The whole batch must round-trip: after the producers finish, the
    // queue holds exactly TOTAL ready jobs, and they're all reservable.
    let mut c = srv.connect().await;
    c.mustsend("stats-tube default\r\n").await;
    let header = c.readline().await;
    assert!(header.starts_with("OK "));
    // Drain the OK body — we don't parse it, the assertion below covers it.
    let body_len: usize = header
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap();
    let mut body = vec![0u8; body_len];
    use tokio::io::AsyncReadExt;
    c.reader.read_exact(&mut body).await.unwrap();
    let mut crlf = [0u8; 2];
    c.reader.read_exact(&mut crlf).await.unwrap();
    let yaml = String::from_utf8(body).unwrap();
    let ready_line = yaml
        .lines()
        .find(|l| l.starts_with("current-jobs-ready:"))
        .expect("ready count present");
    let ready: usize = ready_line
        .split(':')
        .nth(1)
        .and_then(|s| s.trim().parse().ok())
        .unwrap();
    assert_eq!(ready, TOTAL, "every put must be persisted and ready");
}

/// Group commit must preserve durability through restart: every job that
/// got an `INSERTED` ack at sync_interval=0 survives a clean shutdown.
/// Crash-recovery under `kill -9` is covered at the integration level by
/// `tuber-pressure-testing/ruby/bin/crash_recover.rb`; this test locks in
/// the in-process clean-shutdown invariant.
#[tokio::test]
async fn test_group_commit_durability_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();

    let acked: Vec<u64> = {
        let srv = TestServer::start_with_wal_sync_zero(dir.path()).await;
        let mut c = srv.connect().await;
        let mut ids = Vec::with_capacity(40);
        for _ in 0..40 {
            c.mustsend("put 0 0 60 7\r\npayload\r\n").await;
            let line = c.readline().await;
            // Parse "INSERTED <id>\r\n"
            let id: u64 = line
                .strip_prefix("INSERTED ")
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or_else(|| panic!("unexpected: {:?}", line));
            ids.push(id);
        }
        drop(c);
        let _ = srv.shutdown();
        // Allow the engine to flush+sync on shutdown.
        tokio::time::sleep(Duration::from_millis(200)).await;
        ids
    };

    // Restart against the same dir; every acked id must replay.
    let srv = TestServer::start_with_wal(dir.path()).await;
    let mut c = srv.connect().await;
    for id in &acked {
        c.mustsend(&format!("peek {}\r\n", id)).await;
        c.ckresp(&format!("FOUND {} 7\r\n", id)).await;
        c.ckresp("payload\r\n").await;
    }
}
