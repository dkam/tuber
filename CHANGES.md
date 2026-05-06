# Changes

## v0.5.7

**Concurrency-keyed reserve throughput on large queues**

Three compounding bottlenecks made flood-style workloads (many ready jobs sharing a small set of saturated `con:` keys) crawl at single-digit ops/sec on a 100K-entry tube. Measured: 16-worker `flood_con` on 100K jobs went from ~5 jobs/sec to ~2,860 jobs/sec.

- `cmd_delete` now calls `process_queue` when a concurrency key is released and waiters exist (mirrors `cmd_release`). Previously a parked waiter only woke on the next tick.
- `process_queue` precomputes the per-tube unblocked top once per outer pass and uses it for the cheap waiter-eligibility check. Drops per-delete cost from O(W × N) to O(N + W) when many waiters share the same watched tube.
- `find_best_unblocked_ready` slow-path traversal is now capped at 256 nodes (new `FIND_UNBLOCKED_MAX_VISITS`). With saturated con: keys blocking many top jobs, the cap stops scans walking a 100K-entry heap on every reserve; missed jobs are picked up on the next event-driven re-check.
- Replaced the slow path's `collect+sort+linear-scan` with a lazy in-heap-order traversal via an auxiliary `BinaryHeap`.
- Tests: `test_cmd_delete_wakes_concurrency_waiter` (unit), `test_find_unblocked_returns_smallest_unblocked` (unit), `test_many_waiters_concurrency_keyed_drain` (integration, 32 waiters × 2,500 jobs).

**Body storage abstraction (Phase 2 placeholder)**

- Introduce `BodyRef::Inline(Vec<u8>)` / `BodyRef::External(BodyId)` enum to decouple `Job.body` from inline storage. `External` is reserved for a future external body store (TOAST) and is currently never constructed; access panics with a clear message.
- Replace dead `JobState::Reserved` WAL replay arm with `unreachable!()`.

## v0.5.6

**Container health check**

- Dockerfile `HEALTHCHECK` now invokes `tuber stats` so orchestrators (Docker, Kubernetes, Compose) can detect a hung server.

## v0.5.5

**Fix WAL pre-flight check blocking restarts**

- Remove the WAL on-disk size pre-flight check that compared raw binlog bytes against `--max-jobs-size`. In production, WAL bloat from tombstones, deleted records, and multi-file overhead made the on-disk size ~1.4× larger than the live in-memory set, blocking restarts even at 41% memory capacity. The post-replay check (added in 0.5.4) is the sole enforcement and uses the actual in-memory `total_job_bytes`.

## v0.5.4

**Bug fixes for stats, WAL replay, group state, and shutdown**

- `--max-jobs-size` is now exactly enforced after WAL replay. A post-replay check compares `total_job_bytes` against `--max-jobs-size` and aborts with a distinct diagnostic message naming the in-memory size.
- Fix `reserve-job` bypassing `after_group` dependencies; add tests for `waiting_ct` and WAL priority preservation.
- Await engine task on shutdown to prevent WAL data loss.
- Simplify SIGTERM WAL test; tidy shutdown comments.
- Extract `GroupState::remove_waiting_job` helper.
- Fix deleting held after-job leaking its ID in `GroupState::waiting_jobs`.
- Fix per-tube `waiting_ct` leak on disconnect; extract `job_pri` helper.
- Fix WAL replay resetting job priority to 0 for kicks, timeouts, and reserves.
- Remove `tube.waiting_conns` dead state; unify waiter removal.
- Fix `waiting_ct` stats never being updated.

## v0.5.3

**Configurable WAL durability + buffered writes**

- New `--wal-sync-interval` flag (env `TUBER_WAL_SYNC_INTERVAL`, default `100ms`)
  controls how often the WAL is `fsync`ed. `0` syncs on every write before
  acknowledging the client (strongest durability, slowest). Positive values
  bound how much committed state can be lost on crash. On clean shutdown the
  tail is always flushed and synced regardless of the interval.
- WAL writes now pass through a 64 KiB userland `BufWriter`, cutting syscall
  count under load without changing durability guarantees (every fsync path
  flushes the buffer first).
- When `--wal-sync-interval` is shorter than the engine's 100 ms tick, the
  tick shortens to match so fsync cadence isn't capped by maintenance cadence.

## v0.5.2

**Fix WAL replay losing per-job counters**

After a restart, `stats-job` showed `reserves: 0`, `buries: 0`, `kicks: 0` for every job — even jobs that had been reserved, buried, and kicked multiple times. The counters (`reserve_ct`, `bury_ct`, `timeout_ct`, `release_ct`, `kick_ct`) were only persisted in the FullJob WAL record written at `put` time (when all counters are 0). Subsequent state transitions wrote StateChange records that carried state/priority/delay but no counter data, so replay restored counters to zero.

Additionally, `reserve` and `timeout` transitions never wrote to the WAL at all, so their counters were lost even within a single WAL file's lifetime.

**Fix:** StateChange records now include a 1-byte "reason" field identifying the transition type (reserve, release, bury, kick, timeout). On replay, the appropriate counter is incremented based on this reason. WAL writes are also added for reserve and timeout transitions that previously had none.

- WAL version bumped from 3 to 4. New tuber reads both v3 and v4 files — v3 StateChange records replay with reason=None (counters stay at 0, no worse than before). Old tuber rejects v4 files cleanly via version check.
- StateChange record size grows from 38 to 39 bytes (+1 byte for reason).

## v0.5.1

**Env var support for all server flags**

Every `server` subcommand option can now be set via a `TUBER_*` environment variable.

## v0.5.0

**Memory budget, startup readiness, env var config**

- **`--max-jobs-size` memory budget** — new flag (and `TUBER_MAX_JOBS_SIZE` env var) that caps the total in-memory footprint of all jobs. When the budget is full, `put` returns `OUT_OF_MEMORY` — an explicit backpressure signal instead of a silent OOM kill. Workers can always reserve, release, bury, kick, and delete at capacity. Each job costs `body_len + 512` bytes against the budget; idempotency tombstones cost `key_len + 128`. Accepts human-readable suffixes (`2g`, `500M`, `100k`). Default is unlimited for backward compatibility.
- **Replay pre-check** — if the WAL on disk is larger than `--max-jobs-size`, tuber aborts at startup with a diagnostic error naming both sizes and what to do, instead of OOMing mid-replay.
- **Accounting drift detector** — when the queue drains to empty, tuber verifies the internal byte counter is zero. If not, it logs a warning, bumps an `accounting-drift-events` counter (exposed in stats and Prometheus), and self-heals by resetting. Every increment is a bug worth reporting.
- **Stats and metrics** — new `current-jobs-size`, `max-jobs-size`, and `accounting-drift-events` fields in `stats` output, plus Prometheus gauges `tuber_jobs_size_bytes`, `tuber_jobs_size_limit_bytes`, and counter `tuber_accounting_drift_events_total`.
- **`--max-job-size` suffix support** — the existing per-job body size limit (`-z`) now accepts the same human-readable suffixes (`-z 64k`, `-z 1m`). Backward compatible — raw integers still work.
- **Env var support for all server flags** — every `server` subcommand option can now be set via a `TUBER_*` environment variable (`TUBER_LISTEN`, `TUBER_PORT`, `TUBER_BINLOG_DIR`, `TUBER_MAX_JOB_SIZE`, `TUBER_MAX_JOBS_SIZE`, `TUBER_VERBOSE`, `TUBER_METRICS_PORT`, `TUBER_NAME`). Useful for Docker Compose and Kubernetes deployments where env vars are easier than command arrays.
- **Startup log shows full config** — the startup INFO line now includes `max-job-size`, `max-jobs-size` (if set), `binlog` path (if set), and `metrics` address (if set), so operators can verify the running config from `docker compose logs` without connecting.

## v0.4.2

**Replay WAL before binding listener**

The TCP listener was bound before WAL replay, so the accept port was reachable while the server was still loading the binlog into memory. TCP health checks would pass during replay even though no command could be served — and if replay OOMed, monitors saw a healthy port during the brief window between restart and next OOM. Now `run()` replays the WAL first, then binds. TCP reachability honestly reflects readiness. Also logs the binlog size before replay begins via `Wal::total_disk_bytes()`.

## v0.4.1

**Add floor and min samples to weighted-fair selection**

## v0.4.0

**Weighted-fair reserve mode**

New `reserve-mode weighted-fair` that allocates worker time proportional to tube weights, adjusted for processing time. Prevents slow tubes from starving fast ones.

## v0.3.15

**IDP priority upgrade on duplicate put**

When a duplicate idempotency put arrives with a higher priority (lower number), the existing job's priority is now upgraded instead of being silently ignored. Only upgrades are allowed — never downgrades — to prevent flapping. Works for all job states (ready, reserved, delayed, buried); for non-ready jobs the new priority takes effect on the next state transition. The response includes the new priority when an upgrade occurs: `INSERTED <id> <state> <pri>`.

## v0.3.14

**Percentile fallback, record_timing helper, EWMA_ALPHA cleanup**

Percentiles now fall back to fast-job samples when no slow samples exist, so tubes where all jobs complete in < 100ms get useful p50/p95/p99 instead of zeros. Also extracted `record_timing()` and `push_ring()` helpers to reduce duplication, and hoisted `EWMA_ALPHA` to module level.

## v0.3.13

**Add queue-time (time-in-queue) stats to `stats-tube`**

Tracks how long jobs wait from `put` to `reserve` via EWMA, min, max, and sample count. Growing queue time indicates you need more workers. Exposed in `stats-tube` and Prometheus `/metrics`.

## v0.3.12

**Enhanced processing time stats: dual EWMA, percentiles, and bury rate**

`stats-tube` now exposes bimodal-aware processing time tracking:
- **Dual EWMA** — jobs are split at a 100ms threshold into fast (e.g. idempotent exits) and slow (real work) buckets, each with its own EWMA.
- **Percentiles** — p50/p95/p99 computed from the last 1000 slow-job samples.
- **Bury rate** — `total-buries / total-reserves` for quick failure monitoring.
- All new fields are also exposed via the Prometheus `/metrics` endpoint.

The existing `processing-time-ewma` field is unchanged for backwards compatibility.

## v0.3.11

**Restore concurrency limits from WAL on restart**

`restore_jobs()` was not populating `concurrency_limits` during WAL replay, so after restart `is_concurrency_blocked()` would default to limit 1 instead of the configured limit.

## v0.3.10

**Add `--name` flag and `TUBER_NAME` env for instance naming**

New `--name` flag (and `TUBER_NAME` environment variable) to label server instances. The name appears in stats YAML output, startup log, and Prometheus `tuber_info` gauge with name/version/id labels.

## v0.3.9

**Reap idle tubes during maintenance tick**

Empty non-default tubes are now removed during the periodic maintenance tick when they have no jobs, no watchers, and no active connections — matching beanstalkd's cleanup behavior.

## v0.3.8

**Fix WAL `reserved_bytes` leak causing spurious `OUT_OF_MEMORY`**

Compaction migrations called `write_put()` which unconditionally incremented `reserved_bytes`. Long-lived idle jobs migrated repeatedly inflated the counter until `reserve_put()` rejected new puts. Fixed by only reserving for new jobs. Also simplified `reserve_put()` to match beanstalkd: the WAL creates files on demand, so the only constraint is that a record fits in one file.

## v0.3.7

**Fix WAL state change ref counting causing data loss after GC**

Non-delete state changes (bury/release/kick) incorrectly moved a job's WAL file reference from the FullJob record's file to the StateChange record's file. This allowed GC to delete the file containing the only FullJob, causing silent job loss on WAL replay. A subtle ref-counting bug — the data looked fine at runtime, but jobs vanished after a restart.

## v0.3.6

**Simplify WAL compaction code**

Cleanup pass on the WAL compaction implementation introduced in v0.3.4.

## v0.3.5

**Fix global command counters always showing zero**

Global stats counters (`cmd-put`, `cmd-reserve`, `cmd-delete`, etc.) were never incremented, causing tuber-tui throughput graphs to always show 0. Added `op_ct` increments for all protocol commands and replaced magic number indices with named constants.

## v0.3.4

**Implement WAL compaction**

The WAL grew unboundedly because compaction was never implemented — `maintain()` returned an empty Vec. Ported beanstalkd's self-regulating waste-ratio strategy: compute `waste = (total_space - alive) / alive`, and when ratio >= 2, migrate live jobs from the oldest file to the current file. More waste means more jobs moved per tick. Without this, any long-running server with persistence enabled would eventually exhaust disk.

## v0.3.3

**Include 'tuber' in stats version string**

Allows clients (like tuber-tui) to detect they're connected to tuber rather than vanilla beanstalkd.

## v0.3.2

**Fix per-tube reserved count not decremented on disconnect**

When a connection disconnected, reserved jobs were correctly released back to the ready queue, but the per-tube `reserved_ct` stat was not decremented. This caused `stats-tube` to show ghost reserved jobs that didn't actually exist — misleading for monitoring and debugging.

## v0.3.1

**Add `peek-reserved` command**

New tuber extension to inspect the oldest reserved job in the current tube. Useful for debugging ghost reservations from dead connections.

## v0.3.0

**Add jemalloc allocator, RSS and WAL disk stats**

Switch global allocator to jemalloc for better performance under job alloc/dealloc churn. Added `rusage-maxrss`, `binlog-file-count`, `binlog-total-bytes` to stats output and corresponding Prometheus metrics gauges.

## v0.2.11

**Add extension feature interaction tests and WAL/rusage stats**

Added integration tests for cross-cutting extension interactions: cross-tube concurrency, group + idempotency dedup, delete after-job while group pending, release-with-delay + concurrency. Exposed rusage and WAL disk stats in stats and Prometheus output.

## v0.2.10

**Add `binlog-enabled` field to stats output**

## v0.2.9

**Log version number on startup**

## v0.2.8

**Add `undrain` command**

Complement to `drain` — allows restoring normal operation without a server restart.

## v0.2.7

**Add `drain` command and logging**

Graceful drain mode: reject new `put` commands while allowing workers to finish processing existing jobs. Useful for planned maintenance.

## v0.2.6

**Add `reserve-mode` weighted random strategy**

New `reserve-mode weighted` command allows connections to reserve jobs from watched tubes using a weighted-random strategy instead of strict priority ordering. Tubes can be assigned weights via `watch <tube> <weight>`.

## v0.2.5

**Performance optimizations to close gap with beanstalkd**

- Enable `TCP_NODELAY` on accepted connections (matching beanstalkd)
- Skip `process_queue()` on put when no waiters exist
- Replace O(n) tube name validation with O(1) match expression
- Reduce redundant HashMap lookups in hot paths (put, reserve, delete)
- Use `swap_remove` instead of `retain` for reserved_jobs deletion
- Add `serialize_into()` to reuse write buffer across responses

## v0.2.4

**Add `delete-batch` command**

Bulk delete up to 1000 jobs in a single round-trip. Complementary to `reserve-batch` — clients can now reserve and delete in bulk.

## v0.2.3

**Fix DoS vulnerabilities**

- Cap command line reads at `MAX_LINE_LEN` (891 bytes), preventing unbounded memory growth from clients sending data without newlines
- Check body size against `max_job_size` before allocating the buffer, preventing OOM from malicious `put` commands claiming huge body sizes
- Added 10 fuzz/hardening integration tests

## v0.2.2

**Docker multi-arch builds, CI fixes**

Added multi-arch Docker image builds and updated CI actions for Node.js 24 compatibility.

## v0.2.1

**Add `reserve-batch` command, fix tube name validation**

Bulk reserve up to 100 jobs in a single round-trip. Fixed tube name validation to reject names containing spaces.

## v0.2.0

**Idempotency TTL cooldowns, concurrency limits, WAL tombstones**

Major extension release: idempotency keys with configurable TTL cooldown, concurrency key enforcement for single-job-at-a-time reservations, and WAL tombstone persistence for delete records. Reordered WAL payload fields and added WAL format spec.

## v0.1.x

**Proactive `DEADLINE_SOON`, `stats-group`, graceful shutdown**

Wake waiting clients proactively before their reservation deadline expires. Added `stats-group` command for inspecting job group state. Idempotency state included in responses. Graceful shutdown on SIGTERM.

**Job groups and concurrency keys**

Added `grp:`, `aft:`, and `con:` extension tags on `put` for job orchestration — group tracking with pending/buried counts, after-group dependencies for DAG-style workflows, and concurrency keys for mutual exclusion.

**Fix `process_queue` bug with competing waiters**

Competing waiters on a tube were incorrectly timed out when another waiter received a job. The root cause: the wrong waiter was being removed from the waiting list.

**WAL corruption recovery**

Replaced `unwrap()` calls in WAL deserialization with proper error propagation, allowing the server to recover from partial/corrupt WAL files instead of panicking on startup.

**Core beanstalkd implementation**

Initial Rust implementation of the beanstalkd protocol: TCP server, all standard commands, write-ahead log for persistence, binary min-heap for ready/delay queues, per-connection state management, and integration test suite.
