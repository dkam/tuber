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
}

impl TestServer {
    async fn start() -> Self {
        Self::start_with_max_job_size(65535).await
    }

    async fn start_with_max_job_size(max_job_size: u32) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let handle = tokio::spawn(async move {
            tuber::server::run_with_listener(listener, max_job_size, None, None)
                .await
                .ok();
        });

        TestServer { port, handle }
    }

    async fn start_with_max_jobs_size(max_jobs_size: u64) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let handle = tokio::spawn(async move {
            tuber::server::run_with_listener_limited(
                listener,
                65535,
                Some(max_jobs_size),
                None,
                None,
            )
            .await
            .ok();
        });

        TestServer { port, handle }
    }

    async fn connect(&self) -> TestConn {
        let stream = TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.into_split();
        TestConn {
            reader: BufReader::new(reader),
            writer,
        }
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

    /// Read a full line (ending with \r\n) and assert it equals `expected`.
    async fn ckresp(&mut self, expected: &str) {
        let line = self.readline().await;
        assert_eq!(expected, line, "expected {:?}, got {:?}", expected, line);
    }

    /// Read a full line and assert it contains `sub`.
    async fn ckrespsub(&mut self, sub: &str) {
        let line = self.readline().await;
        assert!(line.contains(sub), "{:?} not found in {:?}", sub, line);
    }

    /// Read a line terminated by \r\n, with a 5-second timeout.
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

    /// Read an OK <len>\r\n<body>\r\n response and return the full response
    /// (both lines concatenated).
    async fn read_ok_body(&mut self) -> String {
        let header = self.readline().await;
        assert!(header.starts_with("OK "), "expected OK, got {:?}", header);
        let len: usize = header
            .trim_end()
            .strip_prefix("OK ")
            .unwrap()
            .parse()
            .unwrap();
        let mut body_buf = vec![0u8; len + 2]; // +2 for trailing \r\n
        tokio::time::timeout(
            Duration::from_secs(5),
            self.reader.read_exact(&mut body_buf),
        )
        .await
        .expect("read_ok_body timed out")
        .unwrap();
        String::from_utf8_lossy(&body_buf[..len]).to_string()
    }

    /// Convenience: put a job and return the inserted ID.
    async fn put_job(&mut self, pri: u32, delay: u32, ttr: u32, body: &str) -> u64 {
        let cmd = format!("put {} {} {} {}\r\n", pri, delay, ttr, body.len());
        self.mustsend(&cmd).await;
        self.mustsend(&format!("{}\r\n", body)).await;
        let line = self.readline().await;
        let after = line
            .strip_prefix("INSERTED ")
            .unwrap_or_else(|| panic!("expected INSERTED, got {:?}", line))
            .trim();
        after.split_whitespace().next().unwrap().parse().unwrap()
    }

    /// Convenience: put a job, reserve it, and return the job ID.
    async fn put_and_reserve(&mut self, pri: u32, delay: u32, ttr: u32, body: &str) -> u64 {
        let id = self.put_job(pri, delay, ttr, body).await;
        self.mustsend("reserve-with-timeout 0\r\n").await;
        let line = self.readline().await;
        assert!(
            line.starts_with("RESERVED "),
            "expected RESERVED, got {:?}",
            line
        );
        self.readline().await; // consume body line
        id
    }

    /// Read a RESERVED_BATCH response and return (id, body_string) pairs.
    async fn read_reserve_batch(&mut self) -> Vec<(u64, String)> {
        let header = self.readline().await;
        let n: usize = header
            .trim_end()
            .strip_prefix("RESERVED_BATCH ")
            .unwrap_or_else(|| panic!("expected RESERVED_BATCH, got {:?}", header))
            .parse()
            .unwrap();
        let mut jobs = Vec::with_capacity(n);
        for _ in 0..n {
            let rline = self.readline().await;
            let parts: Vec<&str> = rline.trim_end().split_whitespace().collect();
            assert_eq!(parts[0], "RESERVED", "expected RESERVED, got {:?}", rline);
            let id: u64 = parts[1].parse().unwrap();
            let bytes: usize = parts[2].parse().unwrap();
            let mut buf = vec![0u8; bytes + 2];
            tokio::time::timeout(Duration::from_secs(5), self.reader.read_exact(&mut buf))
                .await
                .expect("read body timed out")
                .unwrap();
            buf.truncate(bytes);
            let body = String::from_utf8(buf).unwrap();
            jobs.push((id, body));
        }
        jobs
    }
}

// ---------------------------------------------------------------------------
// Batch 1: Basic protocol tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_unknown_command() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("nont10knowncommand\r\n").await;
    c.ckresp("UNKNOWN_COMMAND\r\n").await;
}

#[tokio::test]
async fn test_peek_ok() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("peek 1\r\n").await;
    c.ckresp("FOUND 1 1\r\n").await;
    c.ckresp("a\r\n").await;
}

#[tokio::test]
async fn test_peek_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("peek 2\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("peek 18446744073709551615\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_peek_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("peek 18446744073709551616\r\n").await; // UINT64_MAX+1
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("peek 184467440737095516160000000000000000000000000000\r\n")
        .await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("peek foo111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("peek 111foo\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_touch_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("touch a111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("touch 111a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("touch !@#!@#\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_touch_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("touch 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("touch 100000000000000\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_bury_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("bury 111abc 2\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("bury 111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("bury 111 222abc\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_kickjob_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("kick-job a111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("kick-job 111a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("kick-job !@#!@#\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_delete_ready_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 0 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;
}

#[tokio::test]
async fn test_delete_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("delete 18446744073709551616\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("delete 184467440737095516160000000000000000000000000000\r\n")
        .await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("delete foo111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("delete 111foo\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_release_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // bad id
    c.mustsend("release 18446744073709551616 1 1\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("release 184467440737095516160000000000000000000000000000 1 1\r\n")
        .await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("release foo111\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("release 111foo\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;

    // bad priority
    c.mustsend("release 18446744073709551615 abc 1\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;

    // bad duration
    c.mustsend("release 18446744073709551615 1 abc\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_release_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("release 1 1 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_underscore_tube() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("use x_y\r\n").await;
    c.ckresp("USING x_y\r\n").await;
}

#[tokio::test]
async fn test_two_commands_one_packet() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("use a\r\nuse b\r\n").await;
    c.ckresp("USING a\r\n").await;
    c.ckresp("USING b\r\n").await;
}

#[tokio::test]
async fn test_too_big_job() {
    let srv = TestServer::start_with_max_job_size(10).await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 0 11\r\n").await;
    c.mustsend("delete 9999\r\n").await;
    c.mustsend("put 0 0 0 1\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("JOB_TOO_BIG\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_job_size_invalid() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 0 4294967296\r\n").await;
    c.mustsend("put 0 0 0 10b\r\n").await;
    c.mustsend("put 0 0 0 --!@#$%^&&**()0b\r\n").await;
    c.mustsend("put 0 0 0 1\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_negative_delay() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 512 -1 100 0\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_garbage_priority() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put -1kkdj9djjkd9 0 100 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_negative_priority() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put -1 0 100 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_max_priority() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 4294967295 0 100 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_too_big_priority() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 4294967296 0 100 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_zero_delay() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_list_tubes() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("watch w\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;

    c.mustsend("use u\r\n").await;
    c.ckresp("USING u\r\n").await;

    c.mustsend("list-tubes\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("default"), "expected 'default' in list-tubes");
    assert!(body.contains("w"), "expected 'w' in list-tubes");
    assert!(body.contains("u"), "expected 'u' in list-tubes");

    c.mustsend("list-tube-used\r\n").await;
    c.ckresp("USING u\r\n").await;

    c.mustsend("list-tubes-watched\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("default"));
    assert!(body.contains("w"));

    c.mustsend("ignore default\r\n").await;
    c.ckresp("WATCHING 1\r\n").await;

    c.mustsend("list-tubes-watched\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("w"));
    assert!(!body.contains("default"));

    c.mustsend("ignore w\r\n").await;
    c.ckresp("NOT_IGNORED\r\n").await;
}

#[tokio::test]
async fn test_use_tube_long_name() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    let name200 = "0".repeat(200);
    // 200 chars is okay
    c.mustsend(&format!("use {}\r\n", name200)).await;
    c.ckresp(&format!("USING {}\r\n", name200)).await;
    // 201 chars is too much
    c.mustsend(&format!("use {}Z\r\n", name200)).await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_reserve_mode_fifo() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("reserve-mode fifo\r\n").await;
    c.ckresp("USING fifo\r\n").await;
}

#[tokio::test]
async fn test_reserve_mode_weighted() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("reserve-mode weighted\r\n").await;
    c.ckresp("USING weighted\r\n").await;
}

#[tokio::test]
async fn test_reserve_mode_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("reserve-mode blah\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("reserve-mode \r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_watch_with_weight() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("watch foo 4\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
    // Re-watch updates weight (no new tube)
    c.mustsend("watch foo 2\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
}

#[tokio::test]
async fn test_watch_weight_default() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("watch bar\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
}

#[tokio::test]
async fn test_watch_weight_zero() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("watch foo 0\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_watch_weight_too_large() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("watch foo 10000\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

// ---------------------------------------------------------------------------
// Batch 2: Job lifecycle tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_peek_delayed() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("peek-delayed\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;

    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("put 0 99 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
    c.mustsend("put 0 1 1 1\r\n").await;
    c.mustsend("C\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    c.mustsend("peek-delayed\r\n").await;
    c.ckresp("FOUND 3 1\r\n").await;
    c.ckresp("C\r\n").await;

    c.mustsend("delete 3\r\n").await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("peek-delayed\r\n").await;
    c.ckresp("FOUND 2 1\r\n").await;
    c.ckresp("B\r\n").await;

    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("peek-delayed\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_peek_buried_kick() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // cannot bury unreserved job
    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("peek-buried\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("A\r\n").await;

    // now we can bury
    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("BURIED\r\n").await;
    c.mustsend("peek-buried\r\n").await;
    c.ckresp("FOUND 1 1\r\n").await;
    c.ckresp("A\r\n").await;

    // kick and verify the job is ready
    c.mustsend("kick 1\r\n").await;
    c.ckresp("KICKED 1\r\n").await;
    c.mustsend("peek-buried\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("peek-ready\r\n").await;
    c.ckresp("FOUND 1 1\r\n").await;
    c.ckresp("A\r\n").await;

    // nothing is left to kick
    c.mustsend("kick 1\r\n").await;
    c.ckresp("KICKED 0\r\n").await;
}

#[tokio::test]
async fn test_kickjob_buried() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("A\r\n").await;
    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("BURIED\r\n").await;

    c.mustsend("kick-job 100\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("kick-job 1\r\n").await;
    c.ckresp("KICKED\r\n").await;
    c.mustsend("kick-job 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_kickjob_delayed() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("put 0 10 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    c.mustsend("kick-job 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
    c.mustsend("kick-job 2\r\n").await;
    c.ckresp("KICKED\r\n").await;
    c.mustsend("kick-job 2\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_stats_job_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("stats-job 111ABC\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("stats-job 111 222\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
    c.mustsend("stats-job 111\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_stats_tube_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("use tubea\r\n").await;
    c.ckresp("USING tubea\r\n").await;
    c.mustsend("put 0 0 0 1\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats-tube tubea\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("name: \"tubea\""),
        "name missing from stats-tube"
    );
    assert!(
        body.contains("current-jobs-ready: 0"),
        "current-jobs-ready missing"
    );
    assert!(body.contains("total-jobs: 1"), "total-jobs missing");
}

#[tokio::test]
async fn test_ttr_large() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    let id1 = c.put_job(0, 0, 120, "a").await;
    let id2 = c.put_job(0, 0, 5000, "a").await;

    c.mustsend(&format!("stats-job {}\r\n", id1)).await;
    let body1 = c.read_ok_body().await;
    assert!(body1.contains("ttr: 120"));

    c.mustsend(&format!("stats-job {}\r\n", id2)).await;
    let body2 = c.read_ok_body().await;
    assert!(body2.contains("ttr: 5000"));
}

#[tokio::test]
async fn test_ttr_small() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    let id = c.put_job(0, 0, 0, "a").await;
    c.mustsend(&format!("stats-job {}\r\n", id)).await;
    let body = c.read_ok_body().await;
    assert!(body.contains("ttr: 1"), "ttr should be 1, got: {}", body);
}

#[tokio::test]
async fn test_reserve_job_ready() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    c.mustsend("reserve-job 2\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("B\r\n").await;

    // Non-existing job
    c.mustsend("reserve-job 3\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;

    // id=1 was not reserved
    c.mustsend("release 1 1 0\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;

    c.mustsend("release 2 1 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;
}

#[tokio::test]
async fn test_reserve_job_delayed() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 100 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("put 0 100 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
    c.mustsend("put 0 100 1 1\r\n").await;
    c.mustsend("C\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    c.mustsend("reserve-job 2\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("B\r\n").await;

    c.mustsend("release 2 1 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;

    // verify that job was released in ready state
    c.mustsend("stats-job 2\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: ready"));
}

#[tokio::test]
async fn test_reserve_job_buried() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // put, reserve and bury
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("reserve-job 1\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("A\r\n").await;
    c.mustsend("bury 1 1\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // put, reserve and bury
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
    c.mustsend("reserve-job 2\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("B\r\n").await;
    c.mustsend("bury 2 1\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // reserve by ids
    c.mustsend("reserve-job 2\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("B\r\n").await;
    c.mustsend("reserve-job 1\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("A\r\n").await;

    // release back and check if jobs are ready
    c.mustsend("release 1 1 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;
    c.mustsend("release 2 1 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;
    c.mustsend("stats-job 1\r\n").await;
    let body1 = c.read_ok_body().await;
    assert!(body1.contains("state: ready"));
    c.mustsend("stats-job 2\r\n").await;
    let body2 = c.read_ok_body().await;
    assert!(body2.contains("state: ready"));
}

#[tokio::test]
async fn test_reserve_job_already_reserved() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-job 1\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("A\r\n").await;

    // Job should not be reserved twice
    c.mustsend("reserve-job 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

// ---------------------------------------------------------------------------
// Batch 3: Multi-connection & timing tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multi_tube() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("use abc\r\n").await;
    c.ckresp("USING abc\r\n").await;
    c.mustsend("put 999999 0 0 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("use def\r\n").await;
    c.ckresp("USING def\r\n").await;
    c.mustsend("put 99 0 0 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
    c.mustsend("watch abc\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
    c.mustsend("watch def\r\n").await;
    c.ckresp("WATCHING 3\r\n").await;
    c.mustsend("reserve\r\n").await;
    c.ckresp("RESERVED 2 0\r\n").await;
}

#[tokio::test]
async fn test_reserve_timeout_two_conns() {
    let srv = TestServer::start().await;
    let mut c0 = srv.connect().await;
    let mut c1 = srv.connect().await;
    c0.mustsend("watch foo\r\n").await;
    c0.ckresp("WATCHING 2\r\n").await;
    c0.mustsend("reserve-with-timeout 1\r\n").await;
    c1.mustsend("watch foo\r\n").await;
    c1.ckresp("WATCHING 2\r\n").await;
    // c0 should time out after ~1s
    c0.ckresp("TIMED_OUT\r\n").await;
}

#[tokio::test]
async fn test_close_releases_job() {
    let srv = TestServer::start().await;
    let mut cons = srv.connect().await;
    let mut prod = srv.connect().await;
    cons.mustsend("reserve-with-timeout 1\r\n").await;

    prod.mustsend("put 0 0 100 1\r\n").await;
    prod.mustsend("a\r\n").await;
    prod.ckresp("INSERTED 1\r\n").await;

    cons.ckresp("RESERVED 1 1\r\n").await;
    cons.ckresp("a\r\n").await;

    prod.mustsend("stats-job 1\r\n").await;
    let body = prod.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    // Drop the consumer connection - should release the job
    drop(cons);

    // Job should be released quickly (< 1s)
    prod.mustsend("reserve-with-timeout 1\r\n").await;
    prod.ckresp("RESERVED 1 1\r\n").await;
    prod.ckresp("a\r\n").await;
}

#[tokio::test]
async fn test_quit_releases_job() {
    let srv = TestServer::start().await;
    let mut cons = srv.connect().await;
    let mut prod = srv.connect().await;
    cons.mustsend("reserve-with-timeout 1\r\n").await;

    prod.mustsend("put 0 0 100 1\r\n").await;
    prod.mustsend("a\r\n").await;
    prod.ckresp("INSERTED 1\r\n").await;

    cons.ckresp("RESERVED 1 1\r\n").await;
    cons.ckresp("a\r\n").await;

    prod.mustsend("stats-job 1\r\n").await;
    let body = prod.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    // Quit consumer - should release the job
    cons.mustsend("quit\r\n").await;

    // Job should be released quickly (< 1s)
    prod.mustsend("reserve-with-timeout 1\r\n").await;
    prod.ckresp("RESERVED 1 1\r\n").await;
    prod.ckresp("a\r\n").await;
}

#[tokio::test]
async fn test_delete_reserved_by_other() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    let mut o = srv.connect().await;
    o.mustsend("reserve\r\n").await;
    o.ckresp("RESERVED 1 1\r\n").await;
    o.ckresp("a\r\n").await;

    c.mustsend("delete 1\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_small_delay() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 1 1 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
}

#[tokio::test]
async fn test_delayed_to_ready() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 1 1 0\r\n").await;
    c.mustsend("\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("current-jobs-ready: 0"));
    assert!(body.contains("current-jobs-delayed: 1"));
    assert!(body.contains("total-jobs: 1"));

    // Wait for the delay to expire
    tokio::time::sleep(Duration::from_millis(1100)).await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("current-jobs-ready: 1"),
        "delayed job should be ready now: {}",
        body
    );
    assert!(body.contains("current-jobs-delayed: 0"));
}

#[tokio::test]
async fn test_pause_tube() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 0 1\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    let start = std::time::Instant::now();
    c.mustsend("pause-tube default 1\r\n").await;
    c.ckresp("PAUSED\r\n").await;
    c.mustsend("reserve\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("x\r\n").await;
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_secs(1),
        "pause-tube should delay reserve by 1s, elapsed {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_unpause_tube() {
    let srv = TestServer::start().await;
    let mut c0 = srv.connect().await;
    let mut c1 = srv.connect().await;

    c0.mustsend("put 0 0 0 0\r\n").await;
    c0.mustsend("\r\n").await;
    c0.ckresp("INSERTED 1\r\n").await;

    c0.mustsend("pause-tube default 86400\r\n").await;
    c0.ckresp("PAUSED\r\n").await;

    c1.mustsend("reserve\r\n").await;

    c0.mustsend("pause-tube default 0\r\n").await;
    c0.ckresp("PAUSED\r\n").await;

    // c1 should get the job quickly after unpause
    c1.ckresp("RESERVED 1 0\r\n").await;
    c1.ckresp("\r\n").await;
}

#[tokio::test]
async fn test_reserve_ttr_deadline_soon() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 1\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // After 0.2s the job should still be reserved
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    c.mustsend("reserve-with-timeout 1\r\n").await;
    c.ckresp("DEADLINE_SOON\r\n").await;

    // Job should still be reserved
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    // Release and check it's ready
    c.mustsend("release 1 0 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: ready"));
}

#[tokio::test]
async fn test_reserve_job_ttr_deadline_soon() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 5 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: delayed"));

    c.mustsend("reserve-job 1\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // After 0.1s the job should still be reserved
    tokio::time::sleep(Duration::from_millis(100)).await;
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    c.mustsend("reserve-with-timeout 1\r\n").await;
    c.ckresp("DEADLINE_SOON\r\n").await;

    // Job should still be reserved
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: reserved"));

    // Wait for TTR to expire and check auto-release
    tokio::time::sleep(Duration::from_millis(1000)).await;
    // put a dummy job to trigger tick processing
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
    // check that ID=1 gets released
    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: ready"));
}

#[tokio::test]
async fn test_weighted_reserve_empty() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("reserve-mode weighted\r\n").await;
    c.ckresp("USING weighted\r\n").await;

    c.mustsend("watch empty 1\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;

    // Put a job in default tube
    c.mustsend("put 0 0 120 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Reserve should come from default (only tube with jobs)
    c.mustsend("reserve\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;
}

#[tokio::test]
async fn test_weighted_reserve_distribution() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("reserve-mode weighted\r\n").await;
    c.ckresp("USING weighted\r\n").await;

    c.mustsend("use high\r\n").await;
    c.ckresp("USING high\r\n").await;

    // Put many jobs in "high" tube
    for i in 0..100 {
        c.mustsend("put 0 0 120 1\r\n").await;
        c.mustsend("h\r\n").await;
        c.ckresp(&format!("INSERTED {}\r\n", i + 1)).await;
    }

    c.mustsend("use low\r\n").await;
    c.ckresp("USING low\r\n").await;

    for i in 0..100 {
        c.mustsend("put 0 0 120 1\r\n").await;
        c.mustsend("l\r\n").await;
        c.ckresp(&format!("INSERTED {}\r\n", 100 + i + 1)).await;
    }

    // Watch high:4 low:1, then ignore default
    c.mustsend("watch high 4\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
    c.mustsend("watch low 1\r\n").await;
    c.ckresp("WATCHING 3\r\n").await;
    c.mustsend("ignore default\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;

    let mut high_ct = 0;
    let mut low_ct = 0;
    for _ in 0..100 {
        c.mustsend("reserve\r\n").await;
        let _header = c.readline().await; // "RESERVED <id> 1\r\n"
        let body = c.readline().await; // "h\r\n" or "l\r\n"
        if body.starts_with('h') {
            high_ct += 1;
        } else if body.starts_with('l') {
            low_ct += 1;
        }
    }
    // With 4:1 ratio, expect ~80 high, ~20 low. Use wide bounds.
    assert!(high_ct > 50, "high_ct={} should be > 50", high_ct);
    assert!(low_ct > 2, "low_ct={} should be > 2", low_ct);
}

// ---------------------------------------------------------------------------
// Batch 4: Put extension fields (idp, grp, aft, con)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_put_with_idempotency_key() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1 idp:abc123\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("idempotency-key: abc123"), "body: {}", body);
    assert!(
        body.contains("group: \n"),
        "group should be empty, body: {}",
        body
    );
}

#[tokio::test]
async fn test_put_with_all_extensions() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1 idp:k1 grp:g1 aft:g0 con:c1\r\n")
        .await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("idempotency-key: k1"), "body: {}", body);
    assert!(body.contains("group: g1"), "body: {}", body);
    assert!(body.contains("after-group: g0"), "body: {}", body);
    assert!(body.contains("concurrency-key: c1"), "body: {}", body);
}

#[tokio::test]
async fn test_put_with_extensions_order_independent() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1 con:c1 idp:k1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("idempotency-key: k1"), "body: {}", body);
    assert!(body.contains("concurrency-key: c1"), "body: {}", body);
}

#[tokio::test]
async fn test_put_with_unknown_tag_rejected() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1 foo:bar\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_put_with_invalid_key_rejected() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1 idp:-bad\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_put_without_extensions_still_works() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("idempotency-key: \n"), "body: {}", body);
}

// ---------------------------------------------------------------------------
// Phase 5: Stats format, SIGUSR1, verbose flag
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stats_format_complete() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;

    // All C-expected fields must be present
    let expected_fields = [
        "current-jobs-urgent:",
        "current-jobs-ready:",
        "current-jobs-reserved:",
        "current-jobs-delayed:",
        "current-jobs-buried:",
        "cmd-put:",
        "cmd-peek:",
        "cmd-peek-ready:",
        "cmd-peek-delayed:",
        "cmd-peek-buried:",
        "cmd-peek-reserved:",
        "cmd-reserve:",
        "cmd-reserve-with-timeout:",
        "cmd-delete:",
        "cmd-release:",
        "cmd-use:",
        "cmd-watch:",
        "cmd-ignore:",
        "cmd-bury:",
        "cmd-kick:",
        "cmd-touch:",
        "cmd-stats:",
        "cmd-stats-job:",
        "cmd-stats-tube:",
        "cmd-list-tubes:",
        "cmd-list-tube-used:",
        "cmd-list-tubes-watched:",
        "cmd-pause-tube:",
        "job-timeouts:",
        "total-jobs:",
        "max-job-size:",
        "current-tubes:",
        "current-connections:",
        "current-producers:",
        "current-workers:",
        "current-waiting:",
        "total-connections:",
        "pid:",
        "version:",
        "rusage-utime:",
        "rusage-stime:",
        "uptime:",
        "binlog-oldest-index:",
        "binlog-current-index:",
        "binlog-records-migrated:",
        "binlog-records-written:",
        "binlog-max-size:",
        "binlog-enabled:",
        "binlog-file-count:",
        "binlog-total-bytes:",
        "rusage-maxrss:",
        "draining:",
        "id:",
        "hostname:",
        "os:",
        "platform:",
    ];

    for field in &expected_fields {
        assert!(
            body.contains(field),
            "missing field '{}' in stats:\n{}",
            field,
            body
        );
    }

    // rusage-utime/stime should match N.NNNNNN pattern
    for line in body.lines() {
        if line.starts_with("rusage-utime:") || line.starts_with("rusage-stime:") {
            let val = line.split(':').nth(1).unwrap().trim();
            let parts: Vec<&str> = val.split('.').collect();
            assert_eq!(
                parts.len(),
                2,
                "rusage value should be N.NNNNNN, got: {}",
                val
            );
            assert_eq!(
                parts[1].len(),
                6,
                "rusage fractional should be 6 digits, got: {}",
                val
            );
        }
    }

    // id should be 16-char hex
    for line in body.lines() {
        if line.starts_with("id:") {
            let val = line.split(':').nth(1).unwrap().trim();
            assert_eq!(val.len(), 16, "id should be 16 chars, got: '{}'", val);
            assert!(
                val.chars().all(|c| c.is_ascii_hexdigit()),
                "id should be hex, got: '{}'",
                val
            );
        }
    }

    // hostname, os, platform should be non-empty
    for field in &["hostname:", "os:", "platform:"] {
        for line in body.lines() {
            if line.starts_with(field) {
                let val = line.split(':').nth(1).unwrap().trim();
                assert!(!val.is_empty(), "{} should be non-empty", field);
            }
        }
    }

    // binlog-* fields should be 0 when WAL disabled
    assert!(
        body.contains("binlog-oldest-index: 0"),
        "binlog-oldest-index should be 0"
    );
    assert!(
        body.contains("binlog-current-index: 0"),
        "binlog-current-index should be 0"
    );
    assert!(
        body.contains("binlog-max-size: 0"),
        "binlog-max-size should be 0"
    );
    assert!(
        body.contains("binlog-file-count: 0"),
        "binlog-file-count should be 0 when WAL disabled"
    );
    assert!(
        body.contains("binlog-total-bytes: 0"),
        "binlog-total-bytes should be 0 when WAL disabled"
    );

    // rusage-maxrss should be a non-negative integer
    for line in body.lines() {
        if line.starts_with("rusage-maxrss:") {
            let val: i64 = line
                .split(':')
                .nth(1)
                .unwrap()
                .trim()
                .parse()
                .expect("rusage-maxrss should be an integer");
            assert!(
                val >= 0,
                "rusage-maxrss should be non-negative, got: {}",
                val
            );
        }
    }
}

#[tokio::test]
async fn test_stats_field_ordering() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;

    // Verify key fields appear in C-compatible order by checking line positions
    let ordered_fields = [
        "\ncurrent-jobs-urgent:",
        "\npid:",
        "\nversion:",
        "\nrusage-utime:",
        "\nrusage-stime:",
        "\nuptime:",
        "\nbinlog-oldest-index:",
        "\ndraining:",
        "\nid:",
        "\nhostname:",
        "\nos:",
        "\nplatform:",
    ];

    let mut last_pos = 0;
    for field in &ordered_fields {
        let pos = body
            .find(field)
            .unwrap_or_else(|| panic!("field '{}' not found", field));
        assert!(
            pos >= last_pos,
            "field '{}' (pos {}) should come after previous field (pos {})",
            field,
            pos,
            last_pos
        );
        last_pos = pos;
    }
}

/// SIGUSR1 drain mode test. This test sends SIGUSR1 to the current process,
/// which affects all engine tasks running in the same process. Run in isolation.
#[tokio::test]
#[ignore] // Run with: cargo test test_sigusr1_drain_mode -- --ignored
async fn test_sigusr1_drain_mode() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Get the server PID from stats
    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;
    let pid: i32 = body
        .lines()
        .find(|l| l.starts_with("pid:"))
        .unwrap()
        .split(':')
        .nth(1)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    // Verify not draining initially
    assert!(
        body.contains("draining: false"),
        "should not be draining initially"
    );

    // Send SIGUSR1
    unsafe { libc::kill(pid, libc::SIGUSR1) };

    // Give signal time to be processed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // put should return DRAINING
    c.mustsend("put 0 0 1 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("DRAINING\r\n").await;

    // stats should show draining: true
    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("draining: true"),
        "should be draining after SIGUSR1"
    );
}

#[tokio::test]
async fn test_stats_job_file_field() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_job(0, 0, 1, "a").await;

    c.mustsend(&format!("stats-job {}\r\n", id)).await;
    let body = c.read_ok_body().await;
    // Without WAL, file should be 0
    assert!(
        body.contains("file: 0"),
        "file should be 0 without WAL, got: {}",
        body
    );
}

// ---------------------------------------------------------------------------
// flush-tube tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_flush_tube_basic() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put 3 ready jobs
    c.put_job(0, 0, 1, "a").await;
    c.put_job(0, 0, 1, "b").await;
    c.put_job(0, 0, 1, "c").await;

    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 3\r\n").await;

    // Verify tube is empty
    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("current-jobs-ready: 0"), "ready should be 0");
}

#[tokio::test]
async fn test_flush_tube_mixed_states() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // 1 buried job: put, reserve, bury
    let id = c.put_and_reserve(0, 0, 120, "bury_me").await;
    c.mustsend(&format!("bury {} 0\r\n", id)).await;
    c.ckresp("BURIED\r\n").await;

    // 1 ready job
    c.put_job(0, 0, 1, "ready").await;

    // 1 delayed job
    c.put_job(0, 3600, 1, "delayed").await;

    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 3\r\n").await;

    // Verify all queues empty
    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("current-jobs-ready: 0"), "ready should be 0");
    assert!(
        body.contains("current-jobs-delayed: 0"),
        "delayed should be 0"
    );
    assert!(
        body.contains("current-jobs-buried: 0"),
        "buried should be 0"
    );
}

#[tokio::test]
async fn test_flush_tube_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("flush-tube nonexistent\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_flush_tube_empty() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Use the tube to create it
    c.mustsend("use default\r\n").await;
    c.ckresp("USING default\r\n").await;

    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 0\r\n").await;
}

#[tokio::test]
async fn test_flush_tube_with_reserved() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put 2 jobs, reserve 1
    c.put_job(0, 0, 120, "job1").await;
    c.put_job(0, 0, 120, "job2").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.readline().await; // RESERVED line
    c.readline().await; // body line

    // Now we have 1 reserved + 1 ready
    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 2\r\n").await;

    // Verify tube is empty
    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("current-jobs-ready: 0"), "ready should be 0");
    assert!(
        body.contains("current-jobs-reserved: 0"),
        "reserved should be 0"
    );
}

// ---------------------------------------------------------------------------
// Idempotency key tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_idempotency_basic() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // First put with idempotency key
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Second put with same key on same tube → same ID with state
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 1 READY\r\n").await;
}

#[tokio::test]
async fn test_idempotency_different_keys() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 5 idp:key2\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
}

#[tokio::test]
async fn test_idempotency_different_tubes() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put on default tube
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Switch tube and put with same key → different ID
    c.mustsend("use other\r\n").await;
    c.ckresp("USING other\r\n").await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
}

#[tokio::test]
async fn test_idempotency_after_delete() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // After delete, same key should create new job
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
}

#[tokio::test]
async fn test_idempotency_buried_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Reserve and bury
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // Dedup still works for buried job
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 1 BURIED\r\n").await;
}

#[tokio::test]
async fn test_idempotency_delayed_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 3600 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Dedup works for delayed job
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 1 DELAYED\r\n").await;
}

#[tokio::test]
async fn test_idempotency_reserved_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Reserve the job
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    // Dedup still works for reserved job
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 1 RESERVED\r\n").await;
}

#[tokio::test]
async fn test_idempotency_flush_clears_keys() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Flush the tube
    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 1\r\n").await;

    // Key should be freed, new job gets new ID
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
}

// ---------------------------------------------------------------------------
// Job processing stats tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stats_tube_processing_reserve_delete() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put, reserve, then delete a job
    let id = c.put_and_reserve(0, 0, 60, "x").await;

    c.mustsend(&format!("delete {}\r\n", id)).await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("total-reserves: 1"),
        "total-reserves missing: {}",
        body
    );
    assert!(
        body.contains("processing-time-samples: 1"),
        "processing-time-samples missing: {}",
        body
    );
    // EWMA should be non-zero (job was reserved for some duration)
    assert!(
        !body.contains("processing-time-ewma: 0.000000"),
        "ewma should be non-zero: {}",
        body
    );
}

#[tokio::test]
async fn test_stats_tube_delete_ready_no_ewma() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put and immediately delete (from ready state, never reserved)
    let id = c.put_job(0, 0, 60, "x").await;

    c.mustsend(&format!("delete {}\r\n", id)).await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("total-reserves: 0"),
        "total-reserves should be 0: {}",
        body
    );
    assert!(
        body.contains("processing-time-samples: 0"),
        "samples should be 0: {}",
        body
    );
    assert!(
        body.contains("processing-time-ewma: 0.000000"),
        "ewma should be 0: {}",
        body
    );
}

#[tokio::test]
async fn test_stats_tube_bury_counter() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put, reserve, then bury
    let id = c.put_and_reserve(0, 0, 60, "x").await;

    c.mustsend(&format!("bury {} 0\r\n", id)).await;
    c.ckresp("BURIED\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("total-buries: 1"),
        "total-buries missing: {}",
        body
    );
    // EWMA unchanged (bury doesn't count as successful completion)
    assert!(
        body.contains("processing-time-samples: 0"),
        "samples should be 0 after bury: {}",
        body
    );
}

#[tokio::test]
async fn test_stats_job_time_reserved() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_and_reserve(0, 0, 60, "x").await;

    c.mustsend(&format!("stats-job {}\r\n", id)).await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("time-reserved: 0"),
        "time-reserved missing: {}",
        body
    );
}

// ---------------------------------------------------------------------------
// Job group (grp:/aft:) tests
// ---------------------------------------------------------------------------

/// After-job fires immediately when group has no pending members (empty group).
#[tokio::test]
async fn test_group_after_fires_immediately_empty_group() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put an after-job for a group that has no members
    c.mustsend("put 0 0 60 1 aft:batch-1\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Should be immediately ready since there are no pending group members
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("x\r\n").await;
}

/// After-job is held until all group members are deleted.
#[tokio::test]
async fn test_group_after_held_until_complete() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put two child jobs in the group
    c.mustsend("put 0 0 60 1 grp:batch-1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 grp:batch-1\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Put after-job (should be held)
    c.mustsend("put 0 0 60 1 aft:batch-1\r\n").await;
    c.mustsend("z\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    // Reserve and delete first child
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // After-job should still not be available (one child remains)
    // The only reservable job should be child 2
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("b\r\n").await;
    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Now the after-job should be ready
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 3 1\r\n").await;
    c.ckresp("z\r\n").await;
}

/// Buried job blocks group completion.
#[tokio::test]
async fn test_group_buried_blocks_completion() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put a child job
    c.mustsend("put 0 0 60 1 grp:batch-2\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Put after-job
    c.mustsend("put 0 0 60 1 aft:batch-2\r\n").await;
    c.mustsend("z\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Reserve and bury the child
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;
    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // After-job should not be available (child is buried)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;

    // Kick the buried job back to ready
    c.mustsend("kick 1\r\n").await;
    c.ckresp("KICKED 1\r\n").await;

    // After-job still not ready (child is back to ready, not deleted)
    // Reserve child first
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // Delete the child — now group should complete
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // After-job should now be ready
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("z\r\n").await;
}

/// Multiple after-jobs per group.
#[tokio::test]
async fn test_group_multiple_after_jobs() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // One child
    c.mustsend("put 0 0 60 1 grp:batch-3\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Two after-jobs
    c.mustsend("put 0 0 60 1 aft:batch-3\r\n").await;
    c.mustsend("y\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    c.mustsend("put 0 0 60 1 aft:batch-3\r\n").await;
    c.mustsend("z\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    // Reserve and delete child
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Both after-jobs should now be ready
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("y\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 3 1\r\n").await;
    c.ckresp("z\r\n").await;
}

/// Chaining: after-job for group A is also a member of group B.
#[tokio::test]
async fn test_group_chaining() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Group A child
    c.mustsend("put 0 0 60 1 grp:grpA\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // After grpA, member of grpB (chaining)
    c.mustsend("put 0 0 60 1 aft:grpA grp:grpB\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // After grpB
    c.mustsend("put 0 0 60 1 aft:grpB\r\n").await;
    c.mustsend("c\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    // Delete child of grpA — should release job 2 (aft:grpA)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Job 2 should now be ready (aft:grpA completed)
    // But job 3 should still be held (grpB has pending member: job 2)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("b\r\n").await;

    // Job 3 not yet available
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;

    // Delete job 2 — grpB completes, job 3 should be released
    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 3 1\r\n").await;
    c.ckresp("c\r\n").await;
}

/// Adding children to a group after an after-job is already waiting.
#[tokio::test]
async fn test_group_add_children_after_after_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put one child
    c.mustsend("put 0 0 60 1 grp:batch-4\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Put after-job (held: 1 pending)
    c.mustsend("put 0 0 60 1 aft:batch-4\r\n").await;
    c.mustsend("z\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Add another child to the group
    c.mustsend("put 0 0 60 1 grp:batch-4\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    // Delete first child — after-job still held (second child pending)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Reserve second child
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 3 1\r\n").await;
    c.ckresp("b\r\n").await;
    c.mustsend("delete 3\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Now after-job should be ready
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("z\r\n").await;
}

/// Stats-job shows group and after-group fields.
#[tokio::test]
async fn test_group_stats_job_fields() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 1 grp:mygrp aft:othergrp\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("group: mygrp"), "group missing: {}", body);
    assert!(
        body.contains("after-group: othergrp"),
        "after-group missing: {}",
        body
    );
}

/// Deleting a held after-job must remove its ID from GroupState::waiting_jobs
/// so the group can be cleaned up. Without the fix, the group leaks forever.
#[tokio::test]
async fn test_delete_held_after_job_cleans_waiting_jobs() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Create a group job so the group exists with pending=1
    c.mustsend("put 0 0 60 1 grp:g1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Create an after-job for g1 — it should be held (delayed, no deadline)
    c.mustsend("put 0 0 60 1 aft:g1\r\n").await;
    c.mustsend("z\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // The after-job should NOT be reservable yet
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // Delete the held after-job (job 2) — this is the bug scenario
    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Delete the group job (job 1) to complete the group
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Now create a NEW after-job for the same group.
    // If the bug is present, g1's waiting_jobs still contains the deleted job 2,
    // so is_idle() returns false and the group is never cleaned up — meaning
    // the new after-job would be held forever instead of firing immediately.
    c.mustsend("put 0 0 60 1 aft:g1\r\n").await;
    c.mustsend("y\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    // The new after-job should fire immediately (group g1 is complete and idle)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 3 1\r\n").await;
    c.ckresp("y\r\n").await;
}

// ---------------------------------------------------------------------------
// Concurrency key (con:) tests
// ---------------------------------------------------------------------------

/// Basic concurrency enforcement: reserve first con:key job succeeds, second is hidden.
#[tokio::test]
async fn test_concurrency_basic_enforcement() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put two jobs with same concurrency key
    c.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Reserve first succeeds
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // Second is blocked — no ready jobs visible
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;

    // Delete first, second becomes available
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("b\r\n").await;
}

/// Different concurrency keys don't interfere with each other.
#[tokio::test]
async fn test_concurrency_different_keys() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 1 con:keyA\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 con:keyB\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Both can be reserved simultaneously
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("b\r\n").await;
}

/// Release frees the concurrency slot.
#[tokio::test]
async fn test_concurrency_release_frees_slot() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Reserve first
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // Release first (back to ready with no delay)
    c.mustsend("release 1 0 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;

    // Now another con:mykey job is reservable (job 1 is back to ready but not reserved)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;
}

/// Bury frees the concurrency slot.
#[tokio::test]
async fn test_concurrency_bury_frees_slot() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Reserve and bury first
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // Second becomes reservable
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("b\r\n").await;
}

/// reserve-job respects concurrency: returns NOT_FOUND when key is taken.
#[tokio::test]
async fn test_concurrency_reserve_job_blocked() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Reserve first
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // Try to reserve-job the second one by ID — blocked
    c.mustsend("reserve-job 2\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

/// Jobs without con: are unaffected by concurrency enforcement.
#[tokio::test]
async fn test_concurrency_no_key_unaffected() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put a con: job and a plain job
    c.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    c.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c.mustsend("c\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    // Reserve con:mykey job
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // Plain job (no con:) is still reservable despite con:mykey being taken
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("b\r\n").await;

    // Third job (con:mykey) is blocked
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;
}

/// Stats-job shows concurrency-key field.
#[tokio::test]
async fn test_concurrency_stats_job_field() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 1 con:testkey\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("stats-job 1\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("concurrency-key: testkey"),
        "concurrency-key missing: {}",
        body
    );
}

/// Global stats shows current-concurrency-keys count.
#[tokio::test]
async fn test_concurrency_stats_global() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Initially 0 concurrency keys active
    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("current-concurrency-keys: 0"),
        "expected 0 keys: {}",
        body
    );

    // Put and reserve a con: job
    c.mustsend("put 0 0 60 1 con:k1\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("x\r\n").await;

    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("current-concurrency-keys: 1"),
        "expected 1 key: {}",
        body
    );

    // Delete the job, key should be freed
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("current-concurrency-keys: 0"),
        "expected 0 keys after delete: {}",
        body
    );
}

/// Disconnect frees concurrency slot.
#[tokio::test]
async fn test_concurrency_disconnect_frees_slot() {
    let srv = TestServer::start().await;

    // Put two con:mykey jobs
    let mut c1 = srv.connect().await;
    c1.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c1.mustsend("a\r\n").await;
    c1.ckresp("INSERTED 1\r\n").await;

    c1.mustsend("put 0 0 60 1 con:mykey\r\n").await;
    c1.mustsend("b\r\n").await;
    c1.ckresp("INSERTED 2\r\n").await;

    // Reserve on c1
    c1.mustsend("reserve-with-timeout 0\r\n").await;
    c1.ckresp("RESERVED 1 1\r\n").await;
    c1.ckresp("a\r\n").await;

    // Drop c1 (disconnect) — should release the slot
    drop(c1);
    // Small delay to let the server process the disconnect
    tokio::time::sleep(Duration::from_millis(200)).await;

    // New connection should be able to reserve
    let mut c2 = srv.connect().await;
    c2.mustsend("reserve-with-timeout 0\r\n").await;
    // Either job 1 (re-enqueued) or job 2 could be returned
    let line = c2.readline().await;
    assert!(
        line.starts_with("RESERVED "),
        "expected RESERVED after disconnect, got: {}",
        line
    );
}

// ---------------------------------------------------------------------------
// Cross-tube group tests
// ---------------------------------------------------------------------------

/// Group members can span multiple tubes; after-job fires when all are deleted.
#[tokio::test]
async fn test_group_cross_tube() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put child on tube "alpha"
    c.mustsend("use alpha\r\n").await;
    c.ckresp("USING alpha\r\n").await;
    c.mustsend("put 0 0 60 1 grp:cross\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Put child on tube "beta"
    c.mustsend("use beta\r\n").await;
    c.ckresp("USING beta\r\n").await;
    c.mustsend("put 0 0 60 1 grp:cross\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Put after-job on tube "gamma"
    c.mustsend("use gamma\r\n").await;
    c.ckresp("USING gamma\r\n").await;
    c.mustsend("put 0 0 60 1 aft:cross\r\n").await;
    c.mustsend("z\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    // Watch all tubes
    c.mustsend("watch alpha\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
    c.mustsend("watch beta\r\n").await;
    c.ckresp("WATCHING 3\r\n").await;
    c.mustsend("watch gamma\r\n").await;
    c.ckresp("WATCHING 4\r\n").await;

    // Reserve and delete child from alpha
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // After-job should still be held (child on beta remains)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("b\r\n").await;
    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Now the after-job on gamma should be ready
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 3 1\r\n").await;
    c.ckresp("z\r\n").await;
}

/// Cyclic group dependencies hold jobs indefinitely without crashing.
#[tokio::test]
async fn test_group_cycle_holds_indefinitely() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Seed both groups so they have pending members before adding the aft: jobs.
    // Without this, the first aft: would fire immediately (empty group = complete).
    c.mustsend("put 0 0 60 1 grp:cycleA\r\n").await;
    c.mustsend("x\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 grp:cycleB\r\n").await;
    c.mustsend("y\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Now create the cycle: each after-job depends on the other group
    c.mustsend("put 0 0 60 1 grp:cycleA aft:cycleB\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    c.mustsend("put 0 0 60 1 grp:cycleB aft:cycleA\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 4\r\n").await;

    // Reserve and delete the seed jobs — groups still won't complete
    // because jobs 3 and 4 are also group members (held, but still pending)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("x\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("y\r\n").await;
    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Neither cyclic job should be reservable — deadlocked
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;

    // Server is still functional — put and reserve a normal job
    c.mustsend("put 0 0 60 1\r\n").await;
    c.mustsend("c\r\n").await;
    c.ckresp("INSERTED 5\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 5 1\r\n").await;
    c.ckresp("c\r\n").await;

    // Cyclic jobs are still there (peek confirms they exist)
    c.mustsend("peek 3\r\n").await;
    c.ckresp("FOUND 3 1\r\n").await;
    c.ckresp("a\r\n").await;

    c.mustsend("peek 4\r\n").await;
    c.ckresp("FOUND 4 1\r\n").await;
    c.ckresp("b\r\n").await;
}

// ---------------------------------------------------------------------------
// Concurrency key: kick interaction
// ---------------------------------------------------------------------------

/// Kick on a buried con: job does not leave the concurrency slot taken.
#[tokio::test]
async fn test_concurrency_kick_frees_and_re_reserves() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 1 con:kkey\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 con:kkey\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Reserve and bury job 1
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;
    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // Job 2 should now be reservable (bury freed the slot)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("b\r\n").await;
    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Kick the buried job back to ready
    c.mustsend("kick 1\r\n").await;
    c.ckresp("KICKED 1\r\n").await;

    // Job 1 should be reservable again
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;
}

// ---------------------------------------------------------------------------
// Drain mode: reserve/delete still work
// ---------------------------------------------------------------------------

/// In drain mode, reserve and delete still work — only put is blocked.
#[tokio::test]
async fn test_drain_mode_reserve_and_delete_work() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put a job before draining
    c.mustsend("put 0 0 60 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Enter drain mode via protocol command
    c.mustsend("drain\r\n").await;
    c.ckresp("DRAINING\r\n").await;

    // put should fail
    c.mustsend("put 0 0 60 1\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("DRAINING\r\n").await;

    // reserve should still work
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // delete should still work
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // bury, kick, stats, etc. should all still work
    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("draining: true"));
}

/// undrain exits drain mode and resumes accepting puts.
#[tokio::test]
async fn test_undrain_mode() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Enter drain mode
    c.mustsend("drain\r\n").await;
    c.ckresp("DRAINING\r\n").await;

    // put should fail
    c.mustsend("put 0 0 60 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("DRAINING\r\n").await;

    // Exit drain mode
    c.mustsend("undrain\r\n").await;
    c.ckresp("NOT_DRAINING\r\n").await;

    // put should work again
    c.mustsend("put 0 0 60 1\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // stats should show draining: false
    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("draining: false"));
}

// ---------------------------------------------------------------------------
// reserve-job edge cases
// ---------------------------------------------------------------------------

/// reserve-job on nonexistent job returns NOT_FOUND.
#[tokio::test]
async fn test_reserve_job_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("reserve-job 999\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

/// reserve-job on a deleted job returns NOT_FOUND.
#[tokio::test]
async fn test_reserve_job_deleted() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_and_reserve(0, 0, 60, "a").await;

    c.mustsend(&format!("delete {}\r\n", id)).await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend(&format!("reserve-job {}\r\n", id)).await;
    c.ckresp("NOT_FOUND\r\n").await;
}

// ---------------------------------------------------------------------------
// flush-tube interaction with groups and concurrency
// ---------------------------------------------------------------------------

/// flush-tube clears group pending counts so after-jobs fire.
#[tokio::test]
async fn test_flush_tube_clears_group_state() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put group children on default tube
    c.mustsend("put 0 0 60 1 grp:flushgrp\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 grp:flushgrp\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Put after-job on a different tube so flush doesn't delete it
    c.mustsend("use other\r\n").await;
    c.ckresp("USING other\r\n").await;
    c.mustsend("put 0 0 60 1 aft:flushgrp\r\n").await;
    c.mustsend("z\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    // Flush the default tube (deletes the group children)
    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 2\r\n").await;

    // Watch the other tube — after-job should now be ready
    c.mustsend("watch other\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 3 1\r\n").await;
    c.ckresp("z\r\n").await;
}

/// flush-tube clears concurrency key reservations.
#[tokio::test]
async fn test_flush_tube_clears_concurrency_keys() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put two con: jobs
    c.mustsend("put 0 0 60 1 con:fkey\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 con:fkey\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Reserve first (blocks second)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // Flush — should clear everything including the concurrency state
    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 2\r\n").await;

    // Stats should show 0 concurrency keys
    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("current-concurrency-keys: 0"),
        "concurrency keys should be cleared after flush: {}",
        body
    );
}

// ---------------------------------------------------------------------------
// Weighted reserve: mode switching
// ---------------------------------------------------------------------------

/// Switching between weighted and FIFO modes within the same connection.
#[tokio::test]
async fn test_reserve_mode_switch() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Start in FIFO (default), switch to weighted, switch back
    c.mustsend("reserve-mode weighted\r\n").await;
    c.ckresp("USING weighted\r\n").await;

    c.mustsend("reserve-mode fifo\r\n").await;
    c.ckresp("USING fifo\r\n").await;

    // Put a job and verify FIFO reserve still works
    c.put_and_reserve(0, 0, 60, "a").await;
}

// ---------------------------------------------------------------------------
// Reserve-with-timeout: real waiting behavior
// ---------------------------------------------------------------------------

/// Waiter blocks, then receives a job when one is put.
#[tokio::test]
async fn test_reserve_timeout_waiter_wakes_on_put() {
    let srv = TestServer::start().await;
    let mut consumer = srv.connect().await;
    let mut producer = srv.connect().await;

    // Consumer starts waiting (3s timeout — plenty of room)
    consumer.mustsend("reserve-with-timeout 3\r\n").await;

    // Small delay to ensure the consumer is registered as a waiter
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Producer puts a job — should wake the consumer
    producer.mustsend("put 0 0 60 5\r\n").await;
    producer.mustsend("hello\r\n").await;
    producer.ckresp("INSERTED 1\r\n").await;

    // Consumer should receive the job (not TIMED_OUT)
    consumer.ckresp("RESERVED 1 5\r\n").await;
    consumer.ckresp("hello\r\n").await;
}

/// Waiter times out when no job arrives.
#[tokio::test]
async fn test_reserve_timeout_expires() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let start = tokio::time::Instant::now();
    c.mustsend("reserve-with-timeout 1\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;
    let elapsed = start.elapsed();

    // Should have waited ~1s, not returned instantly
    assert!(
        elapsed >= Duration::from_millis(900),
        "timed out too fast: {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(2000),
        "timed out too slow: {:?}",
        elapsed
    );
}

/// Multiple waiters: first-come-first-served when a job arrives.
#[tokio::test]
async fn test_reserve_timeout_multiple_waiters() {
    let srv = TestServer::start().await;
    let mut c1 = srv.connect().await;
    let mut c2 = srv.connect().await;
    let mut prod = srv.connect().await;

    // Both consumers start waiting
    c1.mustsend("reserve-with-timeout 5\r\n").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    c2.mustsend("reserve-with-timeout 5\r\n").await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Put two jobs back-to-back — each waiter should get one
    prod.mustsend("put 0 0 60 3\r\n").await;
    prod.mustsend("one\r\n").await;
    prod.ckresp("INSERTED 1\r\n").await;
    prod.mustsend("put 0 0 60 3\r\n").await;
    prod.mustsend("two\r\n").await;
    prod.ckresp("INSERTED 2\r\n").await;

    c1.ckresp("RESERVED 1 3\r\n").await;
    c1.ckresp("one\r\n").await;

    c2.ckresp("RESERVED 2 3\r\n").await;
    c2.ckresp("two\r\n").await;
}

/// Waiter on a specific tube gets woken only by jobs on that tube.
#[tokio::test]
async fn test_reserve_timeout_tube_specific() {
    let srv = TestServer::start().await;
    let mut consumer = srv.connect().await;
    let mut producer = srv.connect().await;

    // Consumer watches only "jobs" tube, ignores default
    consumer.mustsend("watch jobs\r\n").await;
    consumer.ckresp("WATCHING 2\r\n").await;
    consumer.mustsend("ignore default\r\n").await;
    consumer.ckresp("WATCHING 1\r\n").await;
    consumer.mustsend("reserve-with-timeout 2\r\n").await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Put on default tube — should NOT wake the consumer
    producer.mustsend("put 0 0 60 5\r\n").await;
    producer.mustsend("wrong\r\n").await;
    producer.ckresp("INSERTED 1\r\n").await;

    // Verify consumer is still waiting (no response yet)
    let no_response = tokio::time::timeout(Duration::from_millis(300), consumer.readline()).await;
    assert!(no_response.is_err(), "consumer should still be waiting");

    // Put on "jobs" tube — should wake the consumer
    producer.mustsend("use jobs\r\n").await;
    producer.ckresp("USING jobs\r\n").await;
    producer.mustsend("put 0 0 60 5\r\n").await;
    producer.mustsend("right\r\n").await;
    producer.ckresp("INSERTED 2\r\n").await;

    consumer.ckresp("RESERVED 2 5\r\n").await;
    consumer.ckresp("right\r\n").await;
}

/// Delayed job becomes ready while waiter is blocking — waiter should receive it.
#[tokio::test]
async fn test_reserve_timeout_delayed_job_wakes_waiter() {
    let srv = TestServer::start().await;
    let mut consumer = srv.connect().await;
    let mut producer = srv.connect().await;

    // Consumer starts waiting
    consumer.mustsend("reserve-with-timeout 3\r\n").await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Producer puts a job with 1s delay
    producer.mustsend("put 0 1 60 7\r\n").await;
    producer.mustsend("delayed\r\n").await;
    producer.ckresp("INSERTED 1\r\n").await;

    // Consumer should get it after ~1s when the delay expires
    let start = tokio::time::Instant::now();
    consumer.ckresp("RESERVED 1 7\r\n").await;
    consumer.ckresp("delayed\r\n").await;
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(800),
        "got delayed job too fast: {:?}",
        elapsed
    );
}

// ---------------------------------------------------------------------------
// Command error paths and missing coverage
// ---------------------------------------------------------------------------

// --- touch ---

#[tokio::test]
async fn test_touch_reserved_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_and_reserve(0, 0, 2, "hello").await;

    // Touch should succeed on our own reserved job
    c.mustsend(&format!("touch {}\r\n", id)).await;
    c.ckresp("TOUCHED\r\n").await;
}

#[tokio::test]
async fn test_touch_not_reserved_by_us() {
    let srv = TestServer::start().await;
    let mut c1 = srv.connect().await;
    let mut c2 = srv.connect().await;

    let id = c1.put_and_reserve(0, 0, 60, "a").await;

    // c2 tries to touch — should fail (not reserved by c2)
    c2.mustsend(&format!("touch {}\r\n", id)).await;
    c2.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_touch_ready_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_job(0, 0, 60, "a").await;

    // Touch a ready (not reserved) job — should fail
    c.mustsend(&format!("touch {}\r\n", id)).await;
    c.ckresp("NOT_FOUND\r\n").await;
}

// --- bury ---

#[tokio::test]
async fn test_bury_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Bury a non-existent job
    c.mustsend("bury 999 0\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_bury_ready_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_job(0, 0, 60, "a").await;

    // Bury a ready (not reserved) job — should fail
    c.mustsend(&format!("bury {} 0\r\n", id)).await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_bury_reserved_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_and_reserve(0, 0, 60, "data").await;

    c.mustsend(&format!("bury {} 100\r\n", id)).await;
    c.ckresp("BURIED\r\n").await;

    // Verify state via stats-job
    c.mustsend(&format!("stats-job {}\r\n", id)).await;
    let body = c.read_ok_body().await;
    assert!(body.contains("state: buried"), "expected buried: {}", body);
    assert!(body.contains("pri: 100"), "expected pri 100: {}", body);
}

#[tokio::test]
async fn test_bury_not_reserved_by_us() {
    let srv = TestServer::start().await;
    let mut c1 = srv.connect().await;
    let mut c2 = srv.connect().await;

    let id = c1.put_and_reserve(0, 0, 60, "a").await;

    // c2 tries to bury c1's job — should fail
    c2.mustsend(&format!("bury {} 0\r\n", id)).await;
    c2.ckresp("NOT_FOUND\r\n").await;
}

// --- release ---

#[tokio::test]
async fn test_release_reserved_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_and_reserve(0, 0, 60, "data").await;

    // Release with new priority and no delay
    c.mustsend(&format!("release {} 50 0\r\n", id)).await;
    c.ckresp("RELEASED\r\n").await;

    // Should be reservable again
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckrespsub("RESERVED").await;
    c.readline().await; // body
}

#[tokio::test]
async fn test_release_with_delay() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_and_reserve(0, 0, 60, "data").await;

    // Release with 1s delay
    c.mustsend(&format!("release {} 0 1\r\n", id)).await;
    c.ckresp("RELEASED\r\n").await;

    // Should be delayed — not immediately reservable
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;

    // Verify state
    c.mustsend(&format!("stats-job {}\r\n", id)).await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("state: delayed"),
        "expected delayed: {}",
        body
    );
}

#[tokio::test]
async fn test_release_not_reserved_by_us() {
    let srv = TestServer::start().await;
    let mut c1 = srv.connect().await;
    let mut c2 = srv.connect().await;

    let id = c1.put_and_reserve(0, 0, 60, "a").await;

    // c2 tries to release c1's job — should fail
    c2.mustsend(&format!("release {} 0 0\r\n", id)).await;
    c2.ckresp("NOT_FOUND\r\n").await;
}

// --- delete ---

#[tokio::test]
async fn test_delete_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("delete 999\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_delete_buried_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_and_reserve(0, 0, 60, "a").await;

    c.mustsend(&format!("bury {} 0\r\n", id)).await;
    c.ckresp("BURIED\r\n").await;

    // Delete a buried job — should succeed
    c.mustsend(&format!("delete {}\r\n", id)).await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend(&format!("stats-job {}\r\n", id)).await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_delete_delayed_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_job(0, 3600, 60, "a").await;

    // Delete a delayed job — should succeed
    c.mustsend(&format!("delete {}\r\n", id)).await;
    c.ckresp("DELETED\r\n").await;
}

// --- kick-job ---

#[tokio::test]
async fn test_kickjob_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("kick-job 999\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_kickjob_ready_job() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_job(0, 0, 60, "a").await;

    // Kick a ready job — should fail (already ready)
    c.mustsend(&format!("kick-job {}\r\n", id)).await;
    c.ckresp("NOT_FOUND\r\n").await;
}

// --- kick (bulk) ---

#[tokio::test]
async fn test_kick_buried_jobs() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put and bury 3 jobs
    for i in 0..3u8 {
        c.put_job(0, 0, 60, &i.to_string()).await;
    }
    for _ in 0..3 {
        c.mustsend("reserve-with-timeout 0\r\n").await;
        c.ckrespsub("RESERVED").await;
        c.readline().await; // body
    }
    // Bury the 3 reserved jobs
    for id in 1..=3 {
        c.mustsend(&format!("bury {} 0\r\n", id)).await;
        c.ckresp("BURIED\r\n").await;
    }

    // Kick 2 of the 3
    c.mustsend("kick 2\r\n").await;
    c.ckresp("KICKED 2\r\n").await;

    // One should still be buried
    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("current-jobs-buried: 1"),
        "expected 1 buried: {}",
        body
    );
}

// --- list-tube-used / list-tubes-watched ---

#[tokio::test]
async fn test_list_tube_used_default() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Default tube is "default"
    c.mustsend("list-tube-used\r\n").await;
    c.ckresp("USING default\r\n").await;

    // Switch tube
    c.mustsend("use mytube\r\n").await;
    c.ckresp("USING mytube\r\n").await;

    c.mustsend("list-tube-used\r\n").await;
    c.ckresp("USING mytube\r\n").await;
}

#[tokio::test]
async fn test_list_tubes_watched_default() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Default: watching only "default"
    c.mustsend("list-tubes-watched\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("default"), "expected default: {}", body);

    // Watch another tube
    c.mustsend("watch extra\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;

    c.mustsend("list-tubes-watched\r\n").await;
    let body = c.read_ok_body().await;
    assert!(body.contains("default"), "expected default: {}", body);
    assert!(body.contains("extra"), "expected extra: {}", body);
}

// --- stats-job error paths ---

#[tokio::test]
async fn test_stats_job_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("stats-job 999\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

// --- stats-tube error paths ---

#[tokio::test]
async fn test_stats_tube_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("stats-tube nonexistent\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

// --- pause-tube error paths ---

#[tokio::test]
async fn test_pause_tube_not_found() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("pause-tube nonexistent 5\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_pause_tube_bad_format() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("pause-tube\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;

    c.mustsend("pause-tube default\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;

    c.mustsend("pause-tube default abc\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

// --- peek error paths ---

#[tokio::test]
async fn test_peek_ready_empty() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("peek-ready\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_peek_delayed_empty() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("peek-delayed\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_peek_buried_empty() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("peek-buried\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

// ---------------------------------------------------------------------------
// peek-reserved tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_peek_reserved() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put a job, reserve it, then peek-reserved should find it
    c.mustsend("put 0 0 120 5\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    c.mustsend("peek-reserved\r\n").await;
    c.ckresp("FOUND 1 5\r\n").await;
    c.ckresp("hello\r\n").await;

    // Delete the job, peek-reserved should return NOT_FOUND
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("peek-reserved\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_peek_reserved_empty() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("peek-reserved\r\n").await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_peek_reserved_multi_tube() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put and reserve a job on tube "a"
    c.mustsend("use a\r\n").await;
    c.ckresp("USING a\r\n").await;
    c.mustsend("put 0 0 120 1\r\n").await;
    c.mustsend("A\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;
    c.mustsend("watch a\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("A\r\n").await;

    // Put and reserve a job on tube "b"
    c.mustsend("use b\r\n").await;
    c.ckresp("USING b\r\n").await;
    c.mustsend("put 0 0 120 1\r\n").await;
    c.mustsend("B\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
    c.mustsend("watch b\r\n").await;
    c.ckresp("WATCHING 3\r\n").await;
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("B\r\n").await;

    // peek-reserved while using tube "b" should find job 2, not job 1
    c.mustsend("peek-reserved\r\n").await;
    c.ckresp("FOUND 2 1\r\n").await;
    c.ckresp("B\r\n").await;

    // Switch to tube "a", should find job 1
    c.mustsend("use a\r\n").await;
    c.ckresp("USING a\r\n").await;
    c.mustsend("peek-reserved\r\n").await;
    c.ckresp("FOUND 1 1\r\n").await;
    c.ckresp("A\r\n").await;
}

// ---------------------------------------------------------------------------
// Idempotency TTL cooldown tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_idempotency_ttl_cooldown() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put with idp:key1:5 (5 second cooldown)
    c.mustsend("put 0 0 60 5 idp:key1:5\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Reserve and delete
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Re-put within cooldown → returns original ID with DELETED state
    c.mustsend("put 0 0 60 5 idp:key1:5\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 1 DELETED\r\n").await;
}

#[tokio::test]
async fn test_idempotency_ttl_expired() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put with idp:key1:1 (1 second cooldown)
    c.mustsend("put 0 0 60 5 idp:key1:1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Reserve and delete
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Wait for cooldown to expire
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Re-put after cooldown → new ID
    c.mustsend("put 0 0 60 5 idp:key1:1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
}

#[tokio::test]
async fn test_idempotency_no_ttl_unchanged() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put with idp:key1 (no TTL, backwards compat)
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Reserve and delete
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Re-put immediately → new ID (no cooldown)
    c.mustsend("put 0 0 60 5 idp:key1\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
}

#[tokio::test]
async fn test_idempotency_ttl_flush_clears() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put with cooldown
    c.mustsend("put 0 0 60 5 idp:key1:60\r\n").await;
    c.mustsend("hello\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Reserve, delete (starts cooldown)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("hello\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Flush tube clears cooldowns
    c.mustsend("flush-tube default\r\n").await;
    c.ckresp("FLUSHED 0\r\n").await;

    // Re-put → new ID (cooldown cleared by flush)
    c.mustsend("put 0 0 60 5 idp:key1:60\r\n").await;
    c.mustsend("world\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;
}

#[tokio::test]
async fn test_deadline_soon_proactive_wake() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put a job with TTR=2
    c.mustsend("put 0 0 2 1\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Reserve it immediately
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // Now start a long reserve-with-timeout. The server should proactively
    // wake us with DEADLINE_SOON when the reserved job enters the 1-second
    // safety margin (~1 second from now), NOT wait for the 10-second timeout.
    c.mustsend("reserve-with-timeout 10\r\n").await;

    // We should get DEADLINE_SOON well before 5 seconds (the readline timeout).
    // With TTR=2, deadline_soon triggers after ~1 second.
    let start = std::time::Instant::now();
    c.ckresp("DEADLINE_SOON\r\n").await;
    let elapsed = start.elapsed();

    // Should have been woken proactively in ~1-2 seconds, not 10
    assert!(
        elapsed < Duration::from_secs(5),
        "expected proactive wake within 5s, took {:?}",
        elapsed
    );
}

// ---------------------------------------------------------------------------
// reserve-batch tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_reserve_batch_basic() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    // Put 5 jobs with ascending priority
    for i in 0..5u32 {
        c.put_job(i, 0, 120, &format!("job{i}")).await;
    }

    c.mustsend("reserve-batch 5\r\n").await;
    let jobs = c.read_reserve_batch().await;
    assert_eq!(jobs.len(), 5);
    // Should be in priority order
    for (i, (_id, body)) in jobs.iter().enumerate() {
        assert_eq!(body, &format!("job{i}"));
    }
}

#[tokio::test]
async fn test_reserve_batch_partial() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    c.put_job(0, 0, 120, "a").await;
    c.put_job(0, 0, 120, "b").await;
    c.put_job(0, 0, 120, "c").await;

    c.mustsend("reserve-batch 10\r\n").await;
    let jobs = c.read_reserve_batch().await;
    assert_eq!(jobs.len(), 3);
}

#[tokio::test]
async fn test_reserve_batch_empty() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    c.mustsend("reserve-batch 5\r\n").await;
    let jobs = c.read_reserve_batch().await;
    assert_eq!(jobs.len(), 0);
}

#[tokio::test]
async fn test_reserve_batch_priority_order() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    // Insert in reverse priority order
    let id3 = c.put_job(30, 0, 120, "low").await;
    let id1 = c.put_job(10, 0, 120, "high").await;
    let id2 = c.put_job(20, 0, 120, "mid").await;

    c.mustsend("reserve-batch 3\r\n").await;
    let jobs = c.read_reserve_batch().await;
    assert_eq!(jobs.len(), 3);
    assert_eq!(jobs[0].0, id1);
    assert_eq!(jobs[1].0, id2);
    assert_eq!(jobs[2].0, id3);
}

#[tokio::test]
async fn test_reserve_batch_concurrency_key() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    // Put 5 jobs with concurrency limit of 2
    for i in 0..5u32 {
        let cmd = format!("put 0 0 120 3 con:key:2\r\n");
        c.mustsend(&cmd).await;
        c.mustsend(&format!("jb{i}\r\n")).await;
        c.ckrespsub("INSERTED").await;
    }

    c.mustsend("reserve-batch 5\r\n").await;
    let jobs = c.read_reserve_batch().await;
    assert_eq!(jobs.len(), 2, "concurrency limit should cap at 2");
}

#[tokio::test]
async fn test_reserve_batch_multi_tube() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    // Put jobs in two tubes
    c.mustsend("use tube1\r\n").await;
    c.ckresp("USING tube1\r\n").await;
    c.put_job(10, 0, 120, "t1").await;

    c.mustsend("use tube2\r\n").await;
    c.ckresp("USING tube2\r\n").await;
    c.put_job(5, 0, 120, "t2").await;

    // Watch both tubes
    c.mustsend("watch tube1\r\n").await;
    c.ckrespsub("WATCHING").await;
    c.mustsend("watch tube2\r\n").await;
    c.ckrespsub("WATCHING").await;

    c.mustsend("reserve-batch 5\r\n").await;
    let jobs = c.read_reserve_batch().await;
    assert_eq!(jobs.len(), 2);
    // tube2 job has lower priority (5 < 10), should come first
    assert_eq!(jobs[0].1, "t2");
    assert_eq!(jobs[1].1, "t1");
}

#[tokio::test]
async fn test_reserve_batch_bad_format() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    c.mustsend("reserve-batch 0\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;

    c.mustsend("reserve-batch 1001\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;

    c.mustsend("reserve-batch abc\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_reserve_batch_count_one() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    let id = c.put_job(0, 0, 120, "single").await;

    c.mustsend("reserve-batch 1\r\n").await;
    let jobs = c.read_reserve_batch().await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].0, id);
    assert_eq!(jobs[0].1, "single");
}

// ---------------------------------------------------------------------------
// delete-batch tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_delete_batch_basic() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    // Put and reserve 3 jobs
    c.put_job(0, 0, 120, "a").await;
    c.put_job(0, 0, 120, "b").await;
    c.put_job(0, 0, 120, "c").await;

    c.mustsend("reserve-batch 3\r\n").await;
    let jobs = c.read_reserve_batch().await;
    let ids: Vec<u64> = jobs.iter().map(|(id, _)| *id).collect();

    let cmd = format!("delete-batch {} {} {}\r\n", ids[0], ids[1], ids[2]);
    c.mustsend(&cmd).await;
    c.ckresp("DELETED_BATCH 3 0\r\n").await;
}

#[tokio::test]
async fn test_delete_batch_mixed_found_not_found() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    let id = c.put_and_reserve(0, 0, 120, "x").await;

    let cmd = format!("delete-batch {} 99999 88888\r\n", id);
    c.mustsend(&cmd).await;
    c.ckresp("DELETED_BATCH 1 2\r\n").await;
}

#[tokio::test]
async fn test_delete_batch_all_not_found() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    c.mustsend("delete-batch 1 2 3\r\n").await;
    c.ckresp("DELETED_BATCH 0 3\r\n").await;
}

#[tokio::test]
async fn test_delete_batch_ready_jobs() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    let id1 = c.put_job(0, 0, 120, "a").await;
    let id2 = c.put_job(0, 0, 120, "b").await;

    // Delete ready jobs (no reserve needed)
    let cmd = format!("delete-batch {} {}\r\n", id1, id2);
    c.mustsend(&cmd).await;
    c.ckresp("DELETED_BATCH 2 0\r\n").await;

    // Verify they're gone
    c.mustsend(&format!("peek {}\r\n", id1)).await;
    c.ckresp("NOT_FOUND\r\n").await;
}

#[tokio::test]
async fn test_delete_batch_other_conn_reserved() {
    let s = TestServer::start().await;
    let mut c1 = s.connect().await;
    let mut c2 = s.connect().await;

    let id = c1.put_and_reserve(0, 0, 120, "mine").await;

    // c2 can't delete c1's reserved job
    let cmd = format!("delete-batch {}\r\n", id);
    c2.mustsend(&cmd).await;
    c2.ckresp("DELETED_BATCH 0 1\r\n").await;
}

#[tokio::test]
async fn test_delete_batch_bad_format() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    // No IDs
    c.mustsend("delete-batch \r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;

    // Non-numeric
    c.mustsend("delete-batch abc\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;

    // Negative
    c.mustsend("delete-batch -1\r\n").await;
    c.ckresp("BAD_FORMAT\r\n").await;
}

#[tokio::test]
async fn test_delete_batch_single_id() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    let id = c.put_and_reserve(0, 0, 120, "solo").await;

    let cmd = format!("delete-batch {}\r\n", id);
    c.mustsend(&cmd).await;
    c.ckresp("DELETED_BATCH 1 0\r\n").await;
}

#[tokio::test]
async fn test_delete_batch_after_reserve_batch() {
    let s = TestServer::start().await;
    let mut c = s.connect().await;

    // Put 5 jobs
    for i in 0..5u32 {
        c.put_job(i, 0, 120, &format!("job{i}")).await;
    }

    // Reserve all via batch
    c.mustsend("reserve-batch 5\r\n").await;
    let jobs = c.read_reserve_batch().await;
    assert_eq!(jobs.len(), 5);

    // Delete all via batch
    let id_strs: Vec<String> = jobs.iter().map(|(id, _)| id.to_string()).collect();
    let cmd = format!("delete-batch {}\r\n", id_strs.join(" "));
    c.mustsend(&cmd).await;
    c.ckresp("DELETED_BATCH 5 0\r\n").await;

    // Verify tube is empty
    c.mustsend("reserve-batch 1\r\n").await;
    let jobs = c.read_reserve_batch().await;
    assert_eq!(jobs.len(), 0);
}

// ---------------------------------------------------------------------------
// Fuzz / garbage input tests
// ---------------------------------------------------------------------------

/// Fire a bunch of garbage lines at the server and make sure it stays alive.
#[tokio::test]
async fn test_fuzz_garbage_commands() {
    let server = TestServer::start().await;

    let garbage_inputs: Vec<&[u8]> = vec![
        // Empty / whitespace
        b"\r\n",
        b"\n",
        b"   \r\n",
        b"\t\t\r\n",
        // Random bytes
        b"\xff\xfe\xfd\r\n",
        b"\x00\x00\x00\r\n",
        // Partial commands
        b"pu\r\n",
        b"reser\r\n",
        b"d\r\n",
        // Commands with absurd arguments
        b"put 99999999999999999999 0 0 0\r\n",
        b"delete 99999999999999999999999999999\r\n",
        b"reserve-with-timeout 99999999999999999\r\n",
        b"kick 99999999999999999999999\r\n",
        // Negative-looking numbers
        b"put -1 -1 -1 -1\r\n",
        b"delete -1\r\n",
        b"release -1 -1 -1\r\n",
        // Commands with too many args
        b"delete 1 2 3 4 5\r\n",
        b"reserve extra args here\r\n",
        b"stats extra\r\n",
        // Very long tube name
        b"use AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\r\n",
        // Special characters in tube names
        b"use \x01\x02\x03\r\n",
        b"use ../../../etc/passwd\r\n",
        b"watch '; DROP TABLE tubes;--\r\n",
        // Embedded NUL bytes
        b"put 0 0 10\x005\r\n",
        b"use \x00default\r\n",
        // Unicode / multi-byte
        b"use \xc3\xa9\xc3\xa0\xc3\xbc\r\n",
        b"put \xe2\x80\x8b 0 0 5\r\n",
        // Protocol confusion
        b"GET / HTTP/1.1\r\n",
        b"HELO smtp.example.com\r\n",
        b"EHLO\r\n",
        // Repeated commands mashed together (no newlines between)
        b"put 0 0 10 5put 0 0 10 5\r\n",
        // Just the command prefix with nothing else
        b"put \r\n",
        b"put\r\n",
        b"use \r\n",
        b"use\r\n",
        b"watch \r\n",
        b"watch\r\n",
        b"ignore \r\n",
        b"delete \r\n",
        b"release \r\n",
        b"bury \r\n",
        b"kick \r\n",
        b"touch \r\n",
        b"peek \r\n",
        b"stats-job \r\n",
        b"stats-tube \r\n",
        b"pause-tube \r\n",
        b"flush-tube \r\n",
        b"reserve-batch \r\n",
        // Extension tags with garbage
        b"put 0 0 10 5 idp:\r\n",
        b"put 0 0 10 5 grp:\r\n",
        b"put 0 0 10 5 aft:\r\n",
        b"put 0 0 10 5 con:\r\n",
        b"put 0 0 10 5 con:key:0\r\n",
        b"put 0 0 10 5 con:key:-1\r\n",
        b"put 0 0 10 5 con:key:99999999999999\r\n",
        b"put 0 0 10 5 idp:key:abc\r\n",
        // Tab-separated instead of space
        b"put\t0\t0\t10\t5\r\n",
        // Carriage return only (no line feed)
        b"stats\r",
    ];

    // Send all garbage on a single connection and verify the server responds
    // to each with an error (not a crash).
    let mut conn = server.connect().await;

    for input in &garbage_inputs {
        // Some inputs contain invalid UTF-8 which will cause read_line to error,
        // closing the connection. We handle that by reconnecting.
        conn.writer.write_all(input).await.unwrap();
    }

    // After all garbage, the server should still accept new connections
    // and process valid commands.
    let mut fresh = server.connect().await;
    fresh.mustsend("stats\r\n").await;
    let resp = fresh.read_ok_body().await;
    assert!(
        resp.contains("current-jobs-ready"),
        "server still healthy after garbage"
    );
}

/// Fire garbage that looks like put commands with mismatched body sizes.
#[tokio::test]
async fn test_fuzz_put_body_mismatch() {
    let server = TestServer::start().await;

    let cases: Vec<(&str, &[u8])> = vec![
        // Claim 5 bytes but send 0
        ("put 0 0 10 5\r\n", b"\r\n"),
        // Claim 5 bytes but send 100
        ("put 0 0 10 5\r\n", b"this is way more than five bytes of data, way way more!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\r\n"),
        // Claim 0 bytes
        ("put 0 0 10 0\r\n", b"\r\n"),
        // Body without trailing \r\n
        ("put 0 0 10 5\r\n", b"hello"),
        // Body with only \n (no \r)
        ("put 0 0 10 5\r\n", b"hello\n"),
    ];

    for (cmd, body) in &cases {
        // Each case might break the connection, so reconnect each time
        let mut conn = server.connect().await;
        conn.writer.write_all(cmd.as_bytes()).await.unwrap();
        conn.writer.write_all(body).await.unwrap();
        // Read whatever response we get (or timeout, which is fine)
        let _ = tokio::time::timeout(Duration::from_millis(500), conn.readline()).await;
    }

    // Server should still be alive
    let mut fresh = server.connect().await;
    fresh.mustsend("stats\r\n").await;
    let resp = fresh.read_ok_body().await;
    assert!(
        resp.contains("current-jobs-ready"),
        "server survived body mismatches"
    );
}

/// Rapid connect/disconnect without sending anything.
#[tokio::test]
async fn test_fuzz_rapid_connect_disconnect() {
    let server = TestServer::start().await;

    for _ in 0..100 {
        let stream = TcpStream::connect(("127.0.0.1", server.port))
            .await
            .unwrap();
        drop(stream);
    }

    // Server should still work
    let mut conn = server.connect().await;
    conn.mustsend("stats\r\n").await;
    let resp = conn.read_ok_body().await;
    assert!(
        resp.contains("current-jobs-ready"),
        "server survived rapid connect/disconnect"
    );
}

/// Send many concurrent connections with garbage.
#[tokio::test]
async fn test_fuzz_concurrent_garbage() {
    let server = TestServer::start().await;

    let mut handles = Vec::new();
    for i in 0..20 {
        let port = server.port;
        handles.push(tokio::spawn(async move {
            let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            let (_, mut writer) = stream.into_split();
            // Each connection sends different garbage
            for j in 0..50 {
                let garbage = format!("garbage_{}_{}_{}\r\n", i, j, "X".repeat(j % 100));
                let _ = writer.write_all(garbage.as_bytes()).await;
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    // Server should still work
    let mut conn = server.connect().await;
    conn.mustsend("stats\r\n").await;
    let resp = conn.read_ok_body().await;
    assert!(
        resp.contains("current-jobs-ready"),
        "server survived concurrent garbage"
    );
}

/// Interleave valid and invalid commands to test state machine robustness.
#[tokio::test]
async fn test_fuzz_interleaved_valid_invalid() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    // Valid: use a tube
    conn.mustsend("use test_fuzz\r\n").await;
    conn.ckresp("USING test_fuzz\r\n").await;

    // Garbage
    conn.mustsend("not_a_command\r\n").await;
    conn.ckresp("UNKNOWN_COMMAND\r\n").await;

    // Valid: put a job
    conn.mustsend("put 0 0 120 5\r\nhello\r\n").await;
    conn.ckrespsub("INSERTED").await;

    // Bad format
    conn.mustsend("put abc\r\n").await;
    conn.ckresp("BAD_FORMAT\r\n").await;

    // Valid: peek (FOUND has a body: "FOUND <id> <bytes>\r\n<body>\r\n")
    conn.mustsend("peek-ready\r\n").await;
    let resp = conn.readline().await;
    assert!(resp.starts_with("FOUND "), "peek-ready should find the job");
    let _body = conn.readline().await; // consume body line

    // More garbage
    conn.mustsend("delete\r\n").await; // missing id — "delete" without space is not matched by strip_prefix("delete ")
    conn.ckresp("UNKNOWN_COMMAND\r\n").await;

    conn.mustsend("delete not_a_number\r\n").await;
    conn.ckresp("BAD_FORMAT\r\n").await;

    // Valid: stats still works
    conn.mustsend("stats\r\n").await;
    let resp = conn.read_ok_body().await;
    assert!(
        resp.contains("current-jobs-ready: 1"),
        "job should still be there"
    );
}

/// Test that put with bytes=u32::MAX doesn't crash (OOM protection).
#[tokio::test]
async fn test_fuzz_put_huge_body_size() {
    // Use a small max_job_size to ensure rejection
    let server = TestServer::start_with_max_job_size(1024).await;
    let mut conn = server.connect().await;

    // Try to put with claimed size of 2GB — the server should reject before allocating
    conn.mustsend("put 0 0 10 2000000000\r\n").await;
    let resp = tokio::time::timeout(Duration::from_secs(2), conn.readline()).await;
    // Either JOB_TOO_BIG or connection closed is acceptable — crash is not
    match resp {
        Ok(line) => {
            assert!(
                line.contains("JOB_TOO_BIG") || line.contains("BAD_FORMAT"),
                "expected rejection, got: {:?}",
                line
            );
        }
        Err(_) => {
            // Timeout — acceptable, server is protecting itself
        }
    }

    // Server should still be alive
    let mut fresh = server.connect().await;
    fresh.mustsend("stats\r\n").await;
    let resp = fresh.read_ok_body().await;
    assert!(
        resp.contains("current-jobs-ready"),
        "server survived huge body size request"
    );
}

/// Test that put with body exceeding max_job_size is rejected but connection stays open.
#[tokio::test]
async fn test_put_oversized_body_keeps_connection() {
    let server = TestServer::start_with_max_job_size(10).await;
    let mut conn = server.connect().await;

    // Send a put with 20 bytes (over the 10-byte limit)
    conn.mustsend("put 0 0 120 20\r\n").await;
    conn.ckresp("JOB_TOO_BIG\r\n").await;

    // Send the body anyway (server should drain it)
    conn.mustsend("01234567890123456789\r\n").await;

    // Connection should still work for subsequent commands
    conn.mustsend("stats\r\n").await;
    let resp = conn.read_ok_body().await;
    assert!(
        resp.contains("current-jobs-ready"),
        "connection still works after oversized put"
    );
}

/// Test that a legitimate put with long extension keys works within the line limit.
#[tokio::test]
async fn test_fuzz_long_put_with_extensions() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    // 200-char key (max tube/key name length)
    let long_key = "a".repeat(200);
    let cmd =
        format!("put 0 0 120 5 idp:{long_key} grp:{long_key} aft:{long_key} con:{long_key}\r\n");
    // This is ~891 bytes — should be under the 1024 limit
    assert!(
        cmd.len() < 1024,
        "test setup: cmd should fit in MAX_LINE_LEN"
    );

    conn.mustsend(&cmd).await;
    conn.mustsend("hello\r\n").await;
    conn.ckrespsub("INSERTED").await;
}

/// Test that a line over MAX_LINE_LEN (1024) gets rejected.
#[tokio::test]
async fn test_fuzz_line_over_max_rejected() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    // Send a 1100-byte line — over the 1024 limit
    let long_cmd = format!("use {}\r\n", "B".repeat(1090));
    conn.writer.write_all(long_cmd.as_bytes()).await.unwrap();

    // Should get BAD_FORMAT (and connection closed)
    let result = tokio::time::timeout(Duration::from_secs(2), conn.readline()).await;
    match result {
        Ok(line) => {
            assert!(
                line.contains("BAD_FORMAT"),
                "expected BAD_FORMAT, got: {:?}",
                line
            );
        }
        Err(_) => {
            // Timeout — also acceptable
        }
    }
}

/// Test that a very long line without newline gets rejected (not OOM).
#[tokio::test]
async fn test_fuzz_very_long_line_no_newline() {
    let server = TestServer::start().await;

    let stream = TcpStream::connect(("127.0.0.1", server.port))
        .await
        .unwrap();
    stream.set_nodelay(true).unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // Send 1KB of 'A' with no newline — should exceed MAX_LINE_LEN (224 bytes)
    let long_line = "A".repeat(1000);
    writer.write_all(long_line.as_bytes()).await.unwrap();

    // Server should respond with BAD_FORMAT and close the connection
    let mut buf = String::new();
    let result = tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut buf)).await;
    match result {
        Ok(Ok(_)) => {
            assert!(
                buf.contains("BAD_FORMAT"),
                "expected BAD_FORMAT, got: {:?}",
                buf
            );
        }
        Ok(Err(_)) | Err(_) => {
            // Connection closed or timeout — also acceptable
        }
    }

    // Server should still be alive for other connections
    let mut fresh = server.connect().await;
    fresh.mustsend("stats\r\n").await;
    let resp = fresh.read_ok_body().await;
    assert!(
        resp.contains("current-jobs-ready"),
        "server survived long line attack"
    );
}

// ---------------------------------------------------------------------------
// Concurrency limit > 1, group+concurrency combos, idempotency+group combos
// ---------------------------------------------------------------------------

/// Concurrency limit greater than one: con:api:2 allows 2 concurrent reserves.
#[tokio::test]
async fn test_concurrency_limit_greater_than_one() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put 3 jobs with con:api:2 (limit 2)
    c.mustsend("put 0 0 60 1 con:api:2\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 con:api:2\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    c.mustsend("put 0 0 60 1 con:api:2\r\n").await;
    c.mustsend("c\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    // Reserve first two (both succeed, limit is 2)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("b\r\n").await;

    // Third is blocked — limit of 2 reached
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;

    // Delete first reserved job, freeing a slot
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Third job is now available
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 3 1\r\n").await;
    c.ckresp("c\r\n").await;
}

/// Group members with a concurrency key: concurrency blocks even within a group.
#[tokio::test]
async fn test_group_member_with_concurrency_key() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put 2 group members with con:api (default limit 1)
    c.mustsend("put 0 0 60 1 grp:batch con:api\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 grp:batch con:api\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Put after-job
    c.mustsend("put 0 0 60 1 aft:batch\r\n").await;
    c.mustsend("z\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    // Reserve first group member (succeeds, con:api slot taken)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // Second group member blocked by concurrency
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;

    // Delete first job, freeing con:api slot
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Second group member now available
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("b\r\n").await;

    // Delete second group member — group completes
    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // After-job becomes ready
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 3 1\r\n").await;
    c.ckresp("z\r\n").await;
}

/// After-job with concurrency key: group completes but after-job is blocked by concurrency.
#[tokio::test]
async fn test_after_job_with_concurrency_key() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put a plain job with con:held first and reserve it (takes the slot)
    c.mustsend("put 0 0 60 7 con:held\r\n").await;
    c.mustsend("blocker\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 7\r\n").await;
    c.ckresp("blocker\r\n").await;

    // Put a group child
    c.mustsend("put 0 0 60 5 grp:batch\r\n").await;
    c.mustsend("child\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Put after-job that also has con:held
    c.mustsend("put 0 0 60 5 aft:batch con:held\r\n").await;
    c.mustsend("after\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    // Reserve and delete the group child — group completes
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 5\r\n").await;
    c.ckresp("child\r\n").await;
    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // After-job should be blocked by con:held (blocker still reserved)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;

    // Delete the blocker — frees con:held slot
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // After-job now available
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 3 5\r\n").await;
    c.ckresp("after\r\n").await;
}

/// Concurrency enforcement is global across tubes.
#[tokio::test]
async fn test_concurrency_cross_tube() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put job on tubeA with con:shared
    c.mustsend("use tubeA\r\n").await;
    c.ckresp("USING tubeA\r\n").await;
    c.mustsend("put 0 0 60 1 con:shared\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Put job on tubeB with con:shared
    c.mustsend("use tubeB\r\n").await;
    c.ckresp("USING tubeB\r\n").await;
    c.mustsend("put 0 0 60 1 con:shared\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Watch both tubes
    c.mustsend("watch tubeA\r\n").await;
    c.ckresp("WATCHING 2\r\n").await;
    c.mustsend("watch tubeB\r\n").await;
    c.ckresp("WATCHING 3\r\n").await;

    // Reserve first → succeeds
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // Reserve second → TIMED_OUT (global concurrency)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;
}

/// Idempotency dedup within a group: duplicate put returns same ID and does not inflate pending count.
#[tokio::test]
async fn test_group_idempotency_duplicate() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put first job with grp and idp
    c.mustsend("put 0 0 60 5 grp:batch idp:key1\r\n").await;
    c.mustsend("first\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Put duplicate with same grp and idp → returns same ID with state
    c.mustsend("put 0 0 60 6 grp:batch idp:key1\r\n").await;
    c.mustsend("second\r\n").await;
    c.ckresp("INSERTED 1 READY\r\n").await;

    // stats-group should show pending: 1 (not 2)
    c.mustsend("stats-group batch\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("pending: 1"),
        "expected pending: 1, got: {:?}",
        body
    );
}

/// Deleting an after-job while the group is still pending does not cause issues.
#[tokio::test]
async fn test_delete_after_job_while_group_pending() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put group child
    c.mustsend("put 0 0 60 5 grp:batch\r\n").await;
    c.mustsend("child\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    // Put after-job
    c.mustsend("put 0 0 60 5 aft:batch\r\n").await;
    c.mustsend("after\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Delete the after-job directly (it's in delayed/waiting state)
    c.mustsend("delete 2\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Reserve and delete the child normally
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 5\r\n").await;
    c.ckresp("child\r\n").await;
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // No more jobs — should not crash
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("TIMED_OUT\r\n").await;

    // Verify server still works with a normal job
    c.mustsend("put 0 0 60 2\r\n").await;
    c.mustsend("ok\r\n").await;
    c.ckresp("INSERTED 3\r\n").await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 3 2\r\n").await;
    c.ckresp("ok\r\n").await;
}

/// Release with delay frees the concurrency slot so other jobs can be reserved.
#[tokio::test]
async fn test_concurrency_release_with_delay() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put 2 jobs with con:api
    c.mustsend("put 0 0 60 1 con:api\r\n").await;
    c.mustsend("a\r\n").await;
    c.ckresp("INSERTED 1\r\n").await;

    c.mustsend("put 0 0 60 1 con:api\r\n").await;
    c.mustsend("b\r\n").await;
    c.ckresp("INSERTED 2\r\n").await;

    // Reserve first
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 1 1\r\n").await;
    c.ckresp("a\r\n").await;

    // Release with 1 second delay
    c.mustsend("release 1 0 1\r\n").await;
    c.ckresp("RELEASED\r\n").await;

    // Immediately try reserve — should get second job (delayed job doesn't hold slot)
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckresp("RESERVED 2 1\r\n").await;
    c.ckresp("b\r\n").await;
}

#[tokio::test]
async fn test_close_releases_tube_reserved_counter() {
    let srv = TestServer::start().await;
    let mut prod = srv.connect().await;
    let mut cons = srv.connect().await;

    // Put a job
    prod.mustsend("put 0 0 100 1\r\n").await;
    prod.mustsend("a\r\n").await;
    prod.ckresp("INSERTED 1\r\n").await;

    // Reserve it
    cons.mustsend("reserve-with-timeout 0\r\n").await;
    cons.ckresp("RESERVED 1 1\r\n").await;
    cons.ckresp("a\r\n").await;

    // Verify tube stats show 1 reserved
    prod.mustsend("stats-tube default\r\n").await;
    let body = prod.read_ok_body().await;
    assert!(
        body.contains("current-jobs-reserved: 1"),
        "should have 1 reserved, got: {}",
        body
    );

    // Drop consumer — server should release the job and decrement tube reserved_ct
    drop(cons);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify tube stats show 0 reserved
    prod.mustsend("stats-tube default\r\n").await;
    let body = prod.read_ok_body().await;
    assert!(
        body.contains("current-jobs-reserved: 0"),
        "should have 0 reserved after disconnect, got: {}",
        body
    );
}

// --- idle tube reaping ---

#[tokio::test]
async fn test_idle_tubes_reaped() {
    let srv = TestServer::start().await;

    // Scope the connection so it drops (disconnects) before we check reaping
    {
        let mut c = srv.connect().await;
        c.mustsend("use ephemeral\r\n").await;
        c.ckresp("USING ephemeral\r\n").await;

        // Put and delete a job so the tube exists but ends up empty
        c.mustsend("put 0 0 60 4\r\n").await;
        c.mustsend("test\r\n").await;
        let resp = c.readline().await;
        let id = resp.trim().strip_prefix("INSERTED ").unwrap().to_string();

        c.mustsend(&format!("delete {}\r\n", id)).await;
        c.ckresp("DELETED\r\n").await;

        // Tube should still be visible while connection is using it
        c.mustsend("list-tubes\r\n").await;
        let body = c.read_ok_body().await;
        assert!(body.contains("ephemeral"), "tube should exist while in use");

        // Switch away so using_ct drops to 0
        c.mustsend("use default\r\n").await;
        c.ckresp("USING default\r\n").await;
    }
    // Connection dropped — watching_ct and using_ct should be 0

    // Wait for at least one tick (100ms interval) to reap the idle tube
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut c2 = srv.connect().await;
    c2.mustsend("list-tubes\r\n").await;
    let body = c2.read_ok_body().await;
    assert!(
        !body.contains("ephemeral"),
        "idle tube should have been reaped, got: {}",
        body
    );
    assert!(
        body.contains("default"),
        "default tube should never be reaped, got: {}",
        body
    );
}

#[tokio::test]
async fn test_stats_tube_new_fields_present() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put, reserve, delete to populate stats
    let id = c.put_and_reserve(0, 0, 60, "x").await;
    c.mustsend(&format!("delete {}\r\n", id)).await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    for field in &[
        "processing-time-fast-threshold:",
        "processing-time-ewma-fast:",
        "processing-time-samples-fast:",
        "processing-time-ewma-slow:",
        "processing-time-samples-slow:",
        "processing-time-p50:",
        "processing-time-p95:",
        "processing-time-p99:",
        "bury-rate:",
    ] {
        assert!(
            body.contains(field),
            "{} missing from stats-tube: {}",
            field,
            body
        );
    }
}

#[tokio::test]
async fn test_stats_tube_fast_ewma() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Reserve and immediately delete — processing time will be microseconds (< 100ms threshold)
    let id = c.put_and_reserve(0, 0, 60, "x").await;
    c.mustsend(&format!("delete {}\r\n", id)).await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    assert!(
        body.contains("processing-time-samples-fast: 1"),
        "fast sample should be 1: {}",
        body
    );
    assert!(
        body.contains("processing-time-samples-slow: 0"),
        "slow sample should be 0: {}",
        body
    );
    assert!(
        !body.contains("processing-time-ewma-fast: 0.000000"),
        "fast ewma should be non-zero: {}",
        body
    );
    assert!(
        body.contains("processing-time-ewma-slow: 0.000000"),
        "slow ewma should be zero: {}",
        body
    );
}

#[tokio::test]
async fn test_stats_tube_slow_ewma_and_percentiles() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Reserve and sleep >100ms to land in the slow bucket
    let id = c.put_and_reserve(0, 0, 60, "x").await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    c.mustsend(&format!("delete {}\r\n", id)).await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    assert!(
        body.contains("processing-time-samples-slow: 1"),
        "slow sample should be 1: {}",
        body
    );
    assert!(
        body.contains("processing-time-samples-fast: 0"),
        "fast sample should be 0: {}",
        body
    );
    assert!(
        !body.contains("processing-time-ewma-slow: 0.000000"),
        "slow ewma should be non-zero: {}",
        body
    );
    // Percentiles should be non-zero with a slow sample
    assert!(
        !body.contains("processing-time-p50: 0.000000"),
        "p50 should be non-zero: {}",
        body
    );
    assert!(
        !body.contains("processing-time-p95: 0.000000"),
        "p95 should be non-zero: {}",
        body
    );
    assert!(
        !body.contains("processing-time-p99: 0.000000"),
        "p99 should be non-zero: {}",
        body
    );
}

#[tokio::test]
async fn test_stats_tube_percentiles_fallback_to_fast() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Fast jobs only — percentiles should fall back to fast ring buffer
    let id = c.put_and_reserve(0, 0, 60, "x").await;
    c.mustsend(&format!("delete {}\r\n", id)).await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    // With no slow samples, percentiles come from fast ring — should be non-zero
    assert!(
        !body.contains("processing-time-p50: 0.000000"),
        "p50 should be non-zero with fast jobs: {}",
        body
    );
}

#[tokio::test]
async fn test_stats_tube_bury_rate() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Reserve and bury 1 job
    let id1 = c.put_and_reserve(0, 0, 60, "a").await;
    c.mustsend(&format!("bury {} 0\r\n", id1)).await;
    c.ckresp("BURIED\r\n").await;

    // Reserve and delete 1 job
    let id2 = c.put_and_reserve(0, 0, 60, "b").await;
    c.mustsend(&format!("delete {}\r\n", id2)).await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    // 1 bury / 2 reserves = 0.5
    assert!(
        body.contains("bury-rate: 0.500000"),
        "bury-rate should be 0.5: {}",
        body
    );
}

#[tokio::test]
async fn test_stats_tube_bury_rate_zero() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // No reserves at all — bury rate should be 0
    c.put_job(0, 0, 60, "x").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    assert!(
        body.contains("bury-rate: 0.000000"),
        "bury-rate should be 0 with no reserves: {}",
        body
    );
}

/// Parse a f64 value from a YAML stats body for a given key.
fn parse_stat_f64(body: &str, key: &str) -> f64 {
    body.lines()
        .find(|l| l.trim().starts_with(key))
        .unwrap_or_else(|| panic!("{} not found in: {}", key, body))
        .split_once(':')
        .unwrap()
        .1
        .trim()
        .parse::<f64>()
        .unwrap()
}

#[tokio::test]
async fn test_stats_tube_ewma_values_plausible() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // 3 fast jobs (immediate delete) + 3 slow jobs (200ms each)
    for _ in 0..3 {
        let id = c.put_and_reserve(0, 0, 60, "fast").await;
        c.mustsend(&format!("delete {}\r\n", id)).await;
        c.ckresp("DELETED\r\n").await;
    }
    for _ in 0..3 {
        let id = c.put_and_reserve(0, 0, 60, "slow").await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        c.mustsend(&format!("delete {}\r\n", id)).await;
        c.ckresp("DELETED\r\n").await;
    }

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    let ewma_fast = parse_stat_f64(&body, "processing-time-ewma-fast:");
    let ewma_slow = parse_stat_f64(&body, "processing-time-ewma-slow:");
    let samples_fast = parse_stat_f64(&body, "processing-time-samples-fast:") as u64;
    let samples_slow = parse_stat_f64(&body, "processing-time-samples-slow:") as u64;

    assert_eq!(samples_fast, 3, "expected 3 fast samples: {}", body);
    assert_eq!(samples_slow, 3, "expected 3 slow samples: {}", body);

    // Fast EWMA should be well under 100ms
    assert!(
        ewma_fast < 0.1,
        "fast ewma {:.6} should be < 0.1s: {}",
        ewma_fast,
        body
    );

    // Slow EWMA should be roughly 200ms (between 150ms and 400ms to allow for scheduling jitter)
    assert!(
        ewma_slow > 0.15 && ewma_slow < 0.4,
        "slow ewma {:.6} should be ~0.2s: {}",
        ewma_slow,
        body
    );
}

#[tokio::test]
async fn test_stats_tube_percentiles_with_varied_times() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Create slow jobs with varying sleep times to test percentile ordering
    let sleep_times = [120, 150, 200, 250, 300];
    for ms in &sleep_times {
        let id = c.put_and_reserve(0, 0, 60, "x").await;
        tokio::time::sleep(Duration::from_millis(*ms)).await;
        c.mustsend(&format!("delete {}\r\n", id)).await;
        c.ckresp("DELETED\r\n").await;
    }

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    let p50 = parse_stat_f64(&body, "processing-time-p50:");
    let p95 = parse_stat_f64(&body, "processing-time-p95:");
    let p99 = parse_stat_f64(&body, "processing-time-p99:");

    // p50 <= p95 <= p99
    assert!(
        p50 <= p95 && p95 <= p99,
        "percentiles should be ordered: p50={:.6} p95={:.6} p99={:.6}: {}",
        p50,
        p95,
        p99,
        body
    );

    // All should be in the 100ms-500ms range (with scheduling jitter margin)
    assert!(
        p50 > 0.1 && p50 < 0.5,
        "p50 {:.6} should be ~0.2s: {}",
        p50,
        body
    );
    assert!(
        p99 > 0.1 && p99 < 0.6,
        "p99 {:.6} should be ~0.3s: {}",
        p99,
        body
    );
}

#[tokio::test]
async fn test_stats_tube_bimodal_split() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Simulate bimodal: 5 instant jobs + 2 slow (250ms) jobs
    for _ in 0..5 {
        let id = c.put_and_reserve(0, 0, 60, "fast").await;
        c.mustsend(&format!("delete {}\r\n", id)).await;
        c.ckresp("DELETED\r\n").await;
    }
    for _ in 0..2 {
        let id = c.put_and_reserve(0, 0, 60, "slow").await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        c.mustsend(&format!("delete {}\r\n", id)).await;
        c.ckresp("DELETED\r\n").await;
    }

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    let ewma_fast = parse_stat_f64(&body, "processing-time-ewma-fast:");
    let ewma_slow = parse_stat_f64(&body, "processing-time-ewma-slow:");
    let ewma_overall = parse_stat_f64(&body, "processing-time-ewma:");
    let samples_fast = parse_stat_f64(&body, "processing-time-samples-fast:") as u64;
    let samples_slow = parse_stat_f64(&body, "processing-time-samples-slow:") as u64;

    assert_eq!(samples_fast, 5);
    assert_eq!(samples_slow, 2);

    // The overall EWMA blends both — it should be between the fast and slow EWMAs
    assert!(
        ewma_overall > ewma_fast && ewma_overall < ewma_slow,
        "overall ewma {:.6} should be between fast {:.6} and slow {:.6}: {}",
        ewma_overall,
        ewma_fast,
        ewma_slow,
        body
    );

    // The split EWMAs should be far apart (orders of magnitude)
    assert!(
        ewma_slow / ewma_fast > 100.0,
        "slow/fast ratio should be large: slow={:.6} fast={:.6}: {}",
        ewma_slow,
        ewma_fast,
        body
    );
}

#[tokio::test]
async fn test_stats_tube_queue_time_fields_present() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    let id = c.put_and_reserve(0, 0, 60, "x").await;
    c.mustsend(&format!("delete {}\r\n", id)).await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    for field in &[
        "queue-time-ewma:",
        "queue-time-min:",
        "queue-time-max:",
        "queue-time-samples:",
    ] {
        assert!(
            body.contains(field),
            "{} missing from stats-tube: {}",
            field,
            body
        );
    }
}

#[tokio::test]
async fn test_stats_tube_queue_time_immediate_reserve() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put and immediately reserve — queue time should be near zero
    let id = c.put_and_reserve(0, 0, 60, "x").await;
    c.mustsend(&format!("delete {}\r\n", id)).await;
    c.ckresp("DELETED\r\n").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    let ewma = parse_stat_f64(&body, "queue-time-ewma:");
    let samples = parse_stat_f64(&body, "queue-time-samples:") as u64;

    assert_eq!(samples, 1, "queue-time-samples should be 1: {}", body);
    assert!(
        ewma < 0.1,
        "queue-time-ewma {:.6} should be near zero for immediate reserve: {}",
        ewma,
        body
    );
}

#[tokio::test]
async fn test_stats_tube_queue_time_delayed_reserve() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put a job, wait 200ms, then reserve — queue time should reflect the wait
    c.put_job(0, 0, 60, "x").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    c.mustsend("reserve-with-timeout 0\r\n").await;
    let line = c.readline().await;
    assert!(line.starts_with("RESERVED "), "expected RESERVED: {}", line);
    c.readline().await; // consume body

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    let ewma = parse_stat_f64(&body, "queue-time-ewma:");
    let samples = parse_stat_f64(&body, "queue-time-samples:") as u64;

    assert_eq!(samples, 1, "queue-time-samples should be 1: {}", body);
    // Queue time should be ~200ms (between 150ms and 400ms with jitter)
    assert!(
        ewma > 0.15 && ewma < 0.4,
        "queue-time-ewma {:.6} should be ~0.2s: {}",
        ewma,
        body
    );
}

#[tokio::test]
async fn test_stats_tube_queue_time_no_reserves() {
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    // Put a job but don't reserve — queue-time should be zero
    c.put_job(0, 0, 60, "x").await;

    c.mustsend("stats-tube default\r\n").await;
    let body = c.read_ok_body().await;

    assert!(
        body.contains("queue-time-ewma: 0.000000"),
        "queue-time-ewma should be 0 with no reserves: {}",
        body
    );
    assert!(
        body.contains("queue-time-samples: 0"),
        "queue-time-samples should be 0: {}",
        body
    );
}

// ---------------------------------------------------------------------------
// --max-jobs-size memory limit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_max_jobs_size_rejects_put_over_budget() {
    // JOB_OVERHEAD_BYTES = 512 per job. Budget for exactly one 100-byte body
    // (100 + 512 = 612). A second 100-byte put must get OUT_OF_MEMORY.
    let srv = TestServer::start_with_max_jobs_size(612).await;
    let mut c = srv.connect().await;

    // First put fits.
    c.mustsend("put 0 0 60 100\r\n").await;
    c.mustsend(&format!("{}\r\n", "a".repeat(100))).await;
    c.ckrespsub("INSERTED ").await;

    // Second put is rejected.
    c.mustsend("put 0 0 60 100\r\n").await;
    c.mustsend(&format!("{}\r\n", "b".repeat(100))).await;
    c.ckresp("OUT_OF_MEMORY\r\n").await;

    // Delete the first job, freeing the budget.
    c.mustsend("delete 1\r\n").await;
    c.ckresp("DELETED\r\n").await;

    // Now another put succeeds.
    c.mustsend("put 0 0 60 100\r\n").await;
    c.mustsend(&format!("{}\r\n", "c".repeat(100))).await;
    c.ckrespsub("INSERTED ").await;
}

#[tokio::test]
async fn test_max_jobs_size_allows_reserve_release_at_capacity() {
    // Fill to exactly the limit, then confirm reserve/release/bury/kick/delete
    // never return OUT_OF_MEMORY.
    let srv = TestServer::start_with_max_jobs_size(612).await;
    let mut c = srv.connect().await;

    c.mustsend("put 0 0 60 100\r\n").await;
    c.mustsend(&format!("{}\r\n", "x".repeat(100))).await;
    c.ckrespsub("INSERTED ").await;

    // Reserve at capacity — must succeed.
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckrespsub("RESERVED ").await;
    c.readline().await; // consume body line

    // Release at capacity — must succeed.
    c.mustsend("release 1 0 0\r\n").await;
    c.ckresp("RELEASED\r\n").await;

    // Reserve again.
    c.mustsend("reserve-with-timeout 0\r\n").await;
    c.ckrespsub("RESERVED ").await;
    c.readline().await;

    // Bury at capacity — must succeed.
    c.mustsend("bury 1 0\r\n").await;
    c.ckresp("BURIED\r\n").await;

    // Kick at capacity — must succeed.
    c.mustsend("kick 1\r\n").await;
    c.ckresp("KICKED 1\r\n").await;
}

#[tokio::test]
async fn test_max_jobs_size_stats_fields_present() {
    let srv = TestServer::start_with_max_jobs_size(10_000).await;
    let mut c = srv.connect().await;

    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;

    assert!(
        body.contains("max-jobs-size: 10000"),
        "max-jobs-size missing or wrong: {body}"
    );
    assert!(
        body.contains("current-jobs-size: 0"),
        "current-jobs-size should start at 0: {body}"
    );

    // Put one 5-byte body, expect current-jobs-size = 5 + 512 = 517.
    c.put_job(0, 0, 60, "hello").await;
    c.mustsend("stats\r\n").await;
    let body2 = c.read_ok_body().await;
    assert!(
        body2.contains("current-jobs-size: 517"),
        "current-jobs-size after one put should be 517: {body2}"
    );
}

#[tokio::test]
async fn test_max_jobs_size_default_unlimited() {
    // When --max-jobs-size is not set, stats should report 0 (unlimited
    // sentinel) and put should never be rejected by the memory check.
    let srv = TestServer::start().await;
    let mut c = srv.connect().await;

    c.mustsend("stats\r\n").await;
    let body = c.read_ok_body().await;
    assert!(
        body.contains("max-jobs-size: 0"),
        "unlimited should show max-jobs-size: 0, got: {body}"
    );

    // 100 puts should all succeed on an unlimited server.
    for i in 0..100 {
        c.put_job(0, 0, 60, &format!("job-{i}")).await;
    }
}

// Regression guard for the process_queue waiter-scan amplification:
// with W parked waiters and N ready jobs sharing a saturated con: key,
// the pre-fix server did O(W × N) heap scans per delete. At W=32 and
// N=2500 that translated to single-digit ops/sec and a multi-minute
// drain; the post-fix server scans each tube once per outer pass and
// finishes in seconds.
#[tokio::test]
async fn test_many_waiters_concurrency_keyed_drain() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const WORKERS: usize = 32;
    const N_JOBS: usize = 2500;
    const KEYS: usize = 5;
    const TUBE: &str = "many-waiters-con";

    let server = TestServer::start().await;
    let port = server.port;

    // Putter: insert all jobs across KEYS con-keys (limit 1 each).
    let mut putter = server.connect().await;
    putter.mustsend(&format!("use {}\r\n", TUBE)).await;
    putter.ckrespsub(&format!("USING {}", TUBE)).await;
    for i in 0..N_JOBS {
        let body = format!("j{i}");
        let cmd = format!(
            "put 0 0 60 {} con:k{}:1\r\n",
            body.len(),
            i % KEYS
        );
        putter.mustsend(&cmd).await;
        putter.mustsend(&format!("{}\r\n", body)).await;
        putter.ckrespsub("INSERTED ").await;
    }

    let consumed = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(WORKERS);

    for _ in 0..WORKERS {
        let consumed = consumed.clone();
        handles.push(tokio::spawn(async move {
            let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            stream.set_nodelay(true).unwrap();
            let (reader, writer) = stream.into_split();
            let mut conn = TestConn {
                reader: BufReader::new(reader),
                writer,
            };

            conn.mustsend(&format!("watch {}\r\n", TUBE)).await;
            conn.readline().await;
            conn.mustsend("ignore default\r\n").await;
            conn.readline().await;

            loop {
                conn.mustsend("reserve-with-timeout 1\r\n").await;
                let line = conn.readline().await;
                if line.starts_with("TIMED_OUT") || line.starts_with("DEADLINE_SOON") {
                    break;
                }
                assert!(line.starts_with("RESERVED "), "{:?}", line);
                let parts: Vec<&str> = line.trim_end().split_whitespace().collect();
                let id: u64 = parts[1].parse().unwrap();
                let body_len: usize = parts[2].parse().unwrap();
                let mut body_buf = vec![0u8; body_len + 2];
                conn.reader.read_exact(&mut body_buf).await.unwrap();

                conn.mustsend(&format!("delete {}\r\n", id)).await;
                let line = conn.readline().await;
                assert!(line.starts_with("DELETED"), "{:?}", line);
                consumed.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    let start = std::time::Instant::now();
    let drain = async {
        for h in handles {
            h.await.unwrap();
        }
    };
    tokio::time::timeout(Duration::from_secs(30), drain)
        .await
        .expect("drain did not complete within 30s — process_queue scan amplification");
    let elapsed = start.elapsed();

    assert_eq!(
        consumed.load(Ordering::Relaxed),
        N_JOBS,
        "expected to drain {} jobs",
        N_JOBS
    );
    assert!(
        elapsed < Duration::from_secs(30),
        "drain took {:?}, expected under 30s",
        elapsed
    );
}
