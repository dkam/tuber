# tuber

An experimental, simple, fast job queue server. One binary, zero dependencies.

Tuber is a re-write of Beanstalkd in Rust, it brings along priority queues, delayed jobs, job reservations, named tubes — and adds unique jobs, concurrency control, job group pipelines, batch operations and weighted queues.

[![tuber-tui](screenshots/tui.png)](https://github.com/tuberq/tuber-rs)
*[tuber-tui](https://github.com/tuberq/tuber-rs) — a terminal dashboard compatible with both Tuber and Beanstalkd.*

## How was this built?

Every line of Rust in this project was written by Claude Opus 4.6. The architecture, testing strategy, and design decisions were human-driven.  I program in Ruby, I have programmed in C, Java and PHP - I have never programmed in Rust.

I used Beanstalkd's C source code and tests as the foundation, first building a minimal working version, duplicating the tests, then incrementally adding the new extensions.

The docs/ directory contains the working files we used to plan and describe the implementation.

I use Beanstalkd in [Booko](https://booko.au) in several places requiring queues.  It's working very well.  Claude Code can use the tuber-cli to interact with the queue, finding buried (failed) jobs, which helps with debugging. 

Read more about it [on my blog](https://da.nmilne.com/shipping-a-job-queue-system-without-reading-the-source-code/).

## Quick Start

```bash
# Start the server
tuber server

# Put a job
tuber put "echo hello world"

# Put jobs from stdin (one per line)
echo -e "job1\njob2\njob3" | tuber put

# Process jobs (reserves and runs each job body as a shell command)
tuber work

# List tubes
tuber tubes

# Check stats
tuber stats
```

## Why Tuber?

Redis-backed queues are popular and performant, but Redis isn't a natural fit for job queues. You're bolting priorities, delays, reservations, and timeouts onto a general-purpose data structure server — complexity that grows with every edge case.

SQLite-backed queues are simple and fast, but limited to a single host. PostgreSQL and MySQL-backed queues can scale beyond one host, but a job queue should be separate from your application database for capacity planning — which means another instance to manage with connection pooling, tuning, vacuuming, backups, and restores.

Tuber is purpose-built for this. A single binary, easy to deploy in Docker, with optional write-ahead log for durability. No capacity planning, no tuning, no surprises. Workers wait efficiently at any scale.

Tuber is wire-compatible with [beanstalkd](https://github.com/beanstalkd/beanstalkd), so [dozens of client libraries](https://github.com/beanstalkd/beanstalkd/wiki/Client-Libraries) already work out of the box. For Tuber's extended features (idempotency, job groups, concurrency keys), see the [beaneater tuber fork](https://github.com/tuberq/beaneater/tree/tuber) for Ruby.

### Feature Comparison

| Feature | Tuber | Beanstalkd | Sidekiq + Redis | GoodJob | Solid Queue | RabbitMQ |
|---|---|---|---|---|---|---|
| **Unique / idempotent jobs** | Yes | — | Enterprise only ¹ | Yes | Yes ² | — ³ |
| **Concurrency control** | Yes (per-key) | — | Enterprise only ¹ | Yes | Yes | — |
| **Job groups / DAG pipelines** | Yes | — | Pro only ¹ | Batches only ⁴ | — | — |
| **Weighted queues** | Yes | — | Yes | — | — | — |
| **Per-job priority** | Yes (numeric) | Yes (numeric) | — ⁵ | Yes | Yes | Yes |
| **Delayed jobs** | Yes | Yes | Yes | Yes | Yes | Via plugin |
| **Batch reserve / delete** | Yes | — | — | — | — | Prefetch |
| **Memory backpressure** | Yes ¹⁰ | — | Redis `maxmemory` ¹¹ | DB limits | DB limits | Memory alarms ¹² |
| **Processing time stats** | EWMA + p50/p95/p99 | — | Histogram ⁷ | In DB ⁸ | — | — |
| **Queue latency stats** | EWMA + min/max | — | Oldest only ⁹ | In DB ⁸ | — | — |
| **Persistence** | WAL (optional) | WAL (optional) | Redis RDB/AOF | PostgreSQL | DB ⁶ | Durable queues |
| **Infrastructure** | None (single binary) | None (single binary) | Redis | PostgreSQL | DB ⁶ | Erlang runtime |

<sub>¹ Sidekiq's unique jobs, concurrency controls, and batches require [paid tiers](https://sidekiq.org) (Pro/Enterprise). The OSS version has queue weights and basic job processing.<br>
² Solid Queue unique jobs available from Rails 7.2+.<br>
³ RabbitMQ has a community deduplication plugin, but no built-in uniqueness.<br>
⁴ GoodJob batches support single-level fan-out/fan-in (enqueue N jobs, fire a callback when all complete). Multi-stage pipelines require manually chaining batches inside callbacks.<br>
⁵ Sidekiq uses queue-level ordering (strict or weighted), not per-job numeric priority.<br>
⁶ Solid Queue supports SQLite, PostgreSQL, or MySQL.<br>
⁷ Sidekiq 7+ tracks execution time per job class in exponential histogram buckets. No percentiles without external APM.<br>
⁸ GoodJob stores timestamps in PostgreSQL — you can query for percentiles with SQL, but nothing is computed or displayed by default.<br>
⁹ Sidekiq's `Queue#latency` returns the age of the oldest job only, not a distribution. SQS has a similar `ApproximateAgeOfOldestMessage`.<br>
¹⁰ Tuber's `--max-jobs-size` rejects new `put` commands with `OUT_OF_MEMORY` when the budget is full, but workers can always reserve, release, bury, kick, and delete — the queue keeps draining even at capacity.<br>
¹¹ Redis `maxmemory` with an eviction policy can drop data silently. With `noeviction`, writes fail but Sidekiq has no built-in handling — workers stall on Redis errors.<br>
¹² RabbitMQ blocks publishers when memory or disk alarms fire, but also blocks consumers on the same connection — a full queue can prevent workers from ACKing messages, causing a deadlock. Tuber's design avoids this by only gating `put`.</sub>

## What Can You Do With It?

### Background jobs

Offload slow work from your web app. Your request handler queues a job and returns immediately — workers process it in the background.

These examples use shell commands for clarity, but in practice you'd typically interact with the queue programmatically via a client library.

```bash
tuber put -t emails "send-welcome user@example.com"
tuber put -t thumbnails "resize /uploads/photo.jpg 200x200"

# Workers process jobs in the background
tuber work -t emails -j 4 &
tuber work -t thumbnails -j 2 &
```

### Task pipelines

Chain stages together with job groups. Import rows in parallel, then fire a follow-up when they're all done.

```bash
tuber put -g import "./import.sh row1"
tuber put -g import "./import.sh row2"
tuber put -g import "./import.sh row3"
tuber put --aft import "./send-summary.sh"
```

### Rate-limited processing

Use concurrency keys to ensure only one job per key is processed at a time. Different keys run in parallel.

```bash
# One deploy per host at a time, but different hosts in parallel
tuber put -c "web1" "./deploy.sh web1"
tuber put -c "web1" "./deploy.sh web1"   # queued until first finishes
tuber put -c "web2" "./deploy.sh web2"   # runs in parallel — different key
```

### Distributed cron

Running the same cron on multiple hosts? Idempotency keys prevent duplicate work.

```bash
# Safe to call from multiple cron hosts — only one job created
tuber put -i "nightly-report:300" "./generate-report.sh"
```

### Shell task runner

`tuber put` + `tuber work` is a simple distributed task runner. Queue shell commands, and workers execute them.

```bash
tuber server &
tuber work -j 4 &

tuber put "echo 'hello world'"
tuber put "curl -s https://example.com/api/webhook -d '{\"event\": \"done\"}'"
tuber put -i "transcode-42" "ffmpeg -i /data/video-42.raw -c:v libx264 /data/video-42.mp4"
```

## Features

All the great hits — priority queues, delayed jobs, TTR, named tubes, bury & kick — plus idempotency, concurrency keys, job groups, weighted reserve, and batch operations.

### Core

- **Priority queues** — lower priority number = more urgent. Jobs with priority < 1024 are considered "urgent".
- **Delayed jobs** — submit a job now, make it available after a delay.
- **Time-to-run (TTR)** — if a worker doesn't finish within TTR seconds, the job goes back to ready.
- **Named tubes** — organise jobs into separate queues. Default tube is `default`.
- **Bury & kick** — set aside problem jobs for later inspection, then kick them back to ready.
- **Peek** — inspect jobs without reserving them. Peek by ID, or peek at the next ready/delayed/buried job.
- **Pause** — temporarily stop a tube from serving jobs.
- **Persistence with split storage** — optional write-ahead log (`-b` flag) for crash recovery, with job bodies stored separately from metadata in an append-only body store. The WAL stays small (records carry just metadata + a body reference), so fsyncs are cheap regardless of body size. Bodies live on disk and stream into worker responses on demand.
- **Large job bodies, tiny RAM** — because bodies live on disk, the in-memory cost per job is ~512 bytes regardless of body size. A 1 MB job uses the same RAM as a 1 KB job. Real-world numbers: 1058 active jobs × 1 MB bodies = ~1 GiB on disk, ~540 KB in RAM (~2000× more 1 MB jobs at the same RAM budget vs. an in-memory queue). The queue scales by job *count*, not job *size*.
- **Memory budget** — `--max-jobs-size` caps in-memory job footprint (metadata + idempotency tombstones when persistence is on; full job bytes when it isn't). PUT returns `OUT_OF_MEMORY` when the budget is exhausted, giving producers an explicit backpressure signal instead of a silent OOM kill. Workers can always reserve, release, bury, kick, and delete — state transitions never fail due to the budget. Stats (`current-jobs-size`, `max-jobs-size`) and Prometheus gauges (`tuber_jobs_size_bytes`, `tuber_jobs_size_limit_bytes`) let you alert before the budget fills up. The budget also applies at startup: tuber aborts with a diagnostic error rather than OOMing mid-replay.
- **Storage budget** — `--max-storage-bytes` (mandatory with `-b`) caps the combined WAL + body-store footprint on disk. PUT returns `OUT_OF_STORAGE` once exceeded; state changes always succeed because tuber reserves one WAL segment's headroom for delete/release/bury/kick records. No more silent disk-fill outages.
- **Prometheus metrics** — expose a `/metrics` endpoint for monitoring. See [Statistics Reference](docs/statistics.md#prometheus-metrics).

### Statistics

Most job queue systems treat performance monitoring as the application's problem. Tuber tracks it at the broker, per tube, with no external tooling required:

- **Processing time** — EWMA, min, max, and sample count for how long workers take to complete jobs (reserve-to-delete).
- **Dual EWMA** — jobs are automatically split at a 100ms threshold into fast and slow buckets, each with its own EWMA. This surfaces bimodal distributions (e.g. idempotent fast-exits vs real work) that a single average would hide.
- **Percentiles** — p50, p95, p99 from the last 1000 samples. Uses slow-job samples when available, falls back to fast-job samples for tubes where all jobs are quick.
- **Queue time (time-in-queue)** — EWMA, min, and max of how long jobs waited from `put` to `reserve`. Growing queue time means you need more workers — and you'll know before your users do.
- **Bury rate** — fraction of reserves that ended in a bury, for quick failure monitoring.

All stats are available via `stats-tube`, the Prometheus `/metrics` endpoint, and [tuber-tui](https://github.com/tuberq/tuber-tui). See the full [Statistics Reference](docs/statistics.md) for field details.

### Weighted Reserve

By default, `reserve` picks the highest-priority job across all watched tubes. Two weighted modes let you distribute work across tubes:

**`weighted`** — a tube is chosen randomly in proportion to its weight, then the highest-priority job from that tube is returned:

```text
watch email
watch notifications 2
watch another-tube 6
reserve-mode weighted
reserve
```

Tubes default to weight 1. Here, `another-tube` is selected 3x as often as `notifications` and 6x as often as `email`.

**`weighted-fair`** — like `weighted`, but adjusts for processing time so that **worker-time** (not job count) is allocated proportional to weights. Each tube's effective weight is `weight / processing_time_ewma`:

```text
reserve-mode weighted-fair
```

This prevents slow tubes from starving fast ones. For example, if `alerter` jobs take 0.1s and `fetcher` jobs take 10s, standard `weighted` with equal weights would lock workers on `fetcher` 99% of the time. With `weighted-fair`, selection compensates for the processing time difference so both tubes get an equal share of worker capacity. Tubes with no processing history yet fall back to raw weights.

### Unique Jobs (Idempotency)

Prevent duplicate jobs with an `idp:` key on `put`. If a job with the same key already exists in the tube, the original job ID is returned along with the existing job's state:

```text
put 100 0 30 5 idp:my-key
<body>
→ INSERTED 1

put 100 0 30 5 idp:my-key
<body>
→ INSERTED 1 READY       (dedup hit — job is ready)
```

#### Priority Escalation

If a duplicate `put` arrives with a higher priority (lower number) than the existing job, the job's priority is upgraded and the new priority is included in the response:

```text
put 100 0 30 5 idp:my-key
<body>
→ INSERTED 1

put 50 0 30 5 idp:my-key
<body>
→ INSERTED 1 READY 50    (dedup hit — priority upgraded to 50)

put 200 0 30 5 idp:my-key
<body>
→ INSERTED 1 READY       (dedup hit — priority NOT downgraded)
```

Priority can only increase (lower number), never decrease — this prevents flapping when multiple producers disagree. The upgrade applies regardless of job state (ready, reserved, delayed, or buried); for non-ready jobs, the new priority takes effect on the next state transition.

The response state tells you exactly what happened to the original job:

| Response | Meaning |
|---|---|
| `INSERTED <id>` | Fresh insert, new job created |
| `INSERTED <id> READY` | Dedup hit — original job is waiting to be reserved |
| `INSERTED <id> READY <pri>` | Dedup hit — priority upgraded to `<pri>` |
| `INSERTED <id> RESERVED` | Dedup hit — original job is being processed |
| `INSERTED <id> RESERVED <pri>` | Dedup hit — priority upgraded (applies on release) |
| `INSERTED <id> DELAYED` | Dedup hit — original job is delayed |
| `INSERTED <id> DELAYED <pri>` | Dedup hit — priority upgraded (applies when ready) |
| `INSERTED <id> BURIED` | Dedup hit — original job is buried |
| `INSERTED <id> BURIED <pri>` | Dedup hit — priority upgraded (applies on kick) |
| `INSERTED <id> DELETED` | Dedup hit during TTL cooldown (see below) |

The state suffix only appears on dedup hits — a `put` without `idp:` always returns plain `INSERTED <id>`, keeping the response fully backwards-compatible with standard beanstalkd clients.

The key is scoped to the tube and cleared when the job is deleted, so the same key can be reused afterwards.

#### Cooldown TTL

By default, the idempotency key is removed as soon as the job is deleted. Add a TTL with `idp:key:N` to keep deduplicating for N seconds after deletion:

```text
put 0 0 30 5 idp:report:300
<body>
→ INSERTED 1

(reserve → delete job 1)

put 0 0 30 5 idp:report:300
<body>
→ INSERTED 1 DELETED     (still deduped — within 300s cooldown)
```

After the cooldown expires, the key is freed and a new job will be created. `idp:key` (no TTL) keeps the original behaviour — key removed immediately on delete.

### Job Groups (Fan-out / Fan-in)

Group related jobs together with `grp:` and chain dependent work with `aft:`. After-jobs are held until every job in the group they depend on has been deleted:

```text
put 0 0 30 11 grp:import
import-row-1
put 0 0 30 11 grp:import
import-row-2
put 0 0 60 14 aft:import
send-summary
```

The `send-summary` job stays held until both `import` group jobs are deleted. Buried jobs block group completion — kick them to let the group finish. If an `aft:` job isn't running and you're not sure why, use `stats-group <name>` to check whether the group still has pending or buried members.

Chain stages together by combining `aft:` and `grp:` on the same job — the job waits on one group while belonging to another:

```text
put 0 0 30 5 grp:extract
row-1
put 0 0 30 5 grp:extract
row-2
put 0 0 30 5 aft:extract grp:transform
transform
put 0 0 30 5 aft:transform
load
```

Here `transform` waits for the `extract` group to finish and is itself a member of the `transform` group. `load` waits for `transform` to complete — giving you a simple DAG pipeline.

Use `stats-group <name>` to inspect group state — useful for debugging why `aft:` jobs aren't running:

```text
stats-group import
→ OK <bytes>
---
name: "import"
pending: 2
buried: 1
complete: false
waiting-jobs: 1
```

A buried job blocks group completion (`complete: false`). Kick it to let the group finish.

Group names are global — jobs in the same group can span multiple tubes. Note that the server does not detect cycles: if two groups depend on each other, the waiting jobs will be held indefinitely. Cycle avoidance is the client's responsibility.

### Concurrency Keys

Limit parallel processing of related jobs. When a job with a `con:` key is reserved, other ready jobs sharing the same key are hidden from `reserve` until the reservation ends (via delete, release, bury, TTR timeout, or disconnect):

```text
put 0 0 30 7 con:user-42
payload1
put 0 0 30 7 con:user-42
payload2
```

Only one `con:user-42` job can be reserved at a time, ensuring serial processing per key.

Set a higher limit with `con:key:N` to allow N concurrent reservations:

```text
put 0 0 30 7 con:api:3
payload1
put 0 0 30 7 con:api:3
payload2
```

Up to 3 `con:api` jobs can be reserved simultaneously. `con:key` (no `:N`) defaults to a limit of 1.

Burying or releasing-with-delay a job frees its concurrency slot immediately — the slot is only held while the job is reserved. Delayed jobs don't occupy a slot until they become ready and are reserved. Use `stats-job <id>` to check a job's current state if reserves are unexpectedly blocked.

### Batch Operations

Reduce round trips when working with many jobs at once.

#### reserve-batch

Reserve up to N jobs in a single call (1–1000). Returns immediately with whatever is available — if fewer jobs are ready than requested, you get fewer:

```text
reserve-batch 5
→ RESERVED_BATCH 3
→ RESERVED 1 5
→ hello
→ RESERVED 2 5
→ world
→ RESERVED 3 7
→ goodbye
```

The response starts with `RESERVED_BATCH <count>`, followed by standard `RESERVED <id> <bytes>\r\n<body>\r\n` entries for each job. If no jobs are available, `RESERVED_BATCH 0` is returned.

#### delete-batch

Delete multiple jobs in a single call (1–1000 IDs, space-separated):

```text
delete-batch 1 2 3 99
→ DELETED_BATCH 3 1
```

Returns `DELETED_BATCH <deleted_count> <not_found_count>` — here 3 jobs were deleted and 1 was not found.

## Installation

### Homebrew

```bash
brew tap tuberq/tuber
brew install tuber
```

### Cargo

```bash
cargo install --git https://github.com/tuberq/tuber
```

Pre-built binaries for Linux and macOS are available on the [releases page](https://github.com/tuberq/tuber/releases).

### Docker

```bash
docker run ghcr.io/tuberq/tuber server -l 0.0.0.0 -p 11300
```

### Building from source

```bash
cargo build --release
```

The binary will be at `target/release/tuber`.

## CLI Reference

### Server

```bash
tuber server [OPTIONS]
```

| Option | Env var | Default | Description |
|---|---|---|---|
| `-l`, `--listen` | `TUBER_LISTEN` | `0.0.0.0` | Listen address |
| `-p`, `--port` | `TUBER_PORT` | `11300` | Listen port |
| `-b`, `--binlog-dir` | `TUBER_BINLOG_DIR` | — | WAL + body-store directory (enables persistence) |
| `-s`, `--max-storage-bytes` | `TUBER_MAX_STORAGE_BYTES` | — | Combined cap on WAL + body-store disk usage. **Mandatory** when `-b` is set. PUT returns `OUT_OF_STORAGE` when the projected footprint would exceed this; state changes (reserve/release/bury/kick/delete) always succeed. Accepts suffixes: `k`, `m`, `g`, `t` (e.g. `100g`). |
| `-i`, `--sync-interval` | `TUBER_SYNC_INTERVAL` | `100ms` | How often the WAL and body store fsync to disk. Lower = less data loss on crash, more I/O. Accepts `ms`, `s`, `m`, `h`. |
| `--migrate-wal` | `TUBER_MIGRATE_WAL` | off | Opt in to upgrading a pre-v5 WAL to the v5 + body-store format on startup. Without this flag the server refuses to start when it detects pre-v5 records. |
| `-z`, `--max-job-size` | `TUBER_MAX_JOB_SIZE` | `65535` | Max size of a single job body. Accepts suffixes: `k`, `m`, `g`, `t` (e.g. `64k`). |
| `--max-jobs-size` | `TUBER_MAX_JOBS_SIZE` | unlimited | Max total in-memory footprint of jobs. With persistence enabled, bodies live on disk and don't count — this caps job *metadata* (~512 B/job) plus idempotency tombstones. Without persistence, bodies are in RAM and counted. PUT returns `OUT_OF_MEMORY` when exceeded; reserve/release/bury/kick/delete always succeed. Accepts suffixes: `k`, `m`, `g`, `t` (e.g. `2g`, `500M`). |
| `-V` | `TUBER_VERBOSE` | warn | Verbosity (`-V` info, `-VV` debug) |
| `--metrics-port` | `TUBER_METRICS_PORT` | — | Prometheus metrics endpoint port |
| `--name` | `TUBER_NAME` | — | Instance name (shown in stats and metrics) |

```bash
# Listen on a custom port with persistence
tuber server -p 11301 -b /var/lib/tuber

# Verbose mode with metrics
tuber server -VV --metrics-port 9100

# Memory-bounded + disk-bounded server (Docker-friendly)
tuber server --max-jobs-size 2g -b /var/lib/tuber -s 100g --metrics-port 9100
```

#### Durability & fsync

When persistence is enabled (`-b`), tuber writes job mutations to two append-only stores:

- **WAL** — small metadata records (`FullJob`, `StateChange`) carrying a `BodyId` reference, not the body bytes themselves.
- **Body store** — separate segment files holding the raw job payloads, addressed by `BodyId`.

Both are fsynced every `--sync-interval` (default 100ms) as part of the server's internal tick — not on every write. The body store fsyncs *first*, then the WAL, so a crash mid-sync can leave orphan bodies (no WAL reference) but never dangling references; orphans are reclaimed automatically on the next replay.

- **At most one tick interval of data can be lost on a crash.** Jobs written since the last sync may not have made it to disk yet.
- **fsync overhead is constant regardless of throughput.** Whether you're doing 10 jobs/sec or 100,000 jobs/sec, tuber calls fsync at the configured rate. On NVMe/SSD storage this adds negligible latency; on spinning disks it costs ~50–150ms/sec of I/O time.
- **Large bodies don't slow down fsync.** Because the WAL records only carry a `BodyId`, the latency-critical WAL fsync flushes a constant ~100 bytes per record regardless of body size.

This is a different trade-off from databases like PostgreSQL or MySQL, which fsync on every transaction commit to guarantee durability of each acknowledged write (the "D" in ACID). Tuber's `INSERTED` response means the job is buffered but not necessarily fsynced — similar to PostgreSQL's `synchronous_commit = off` mode. For most queue workloads, losing a fraction of a second of jobs on a hard crash is acceptable, and the throughput benefit is significant.

Without `-b`, all state is in-memory only and lost on restart.

### Put

```bash
tuber put [OPTIONS] [BODY]
```

| Option | Default | Description |
|---|---|---|
| `-t`, `--tube` | `default` | Tube name |
| `-p`, `--pri` | `0` | Priority (0 is most urgent) |
| `-d`, `--delay` | `0` | Delay in seconds before job becomes ready |
| `--ttr` | `60` | Time-to-run in seconds |
| `-i`, `--idp` | — | Idempotency key — `key` or `key:ttl` (TTL seconds keeps deduping after delete) |
| `-g`, `--grp` | — | Group name (for job grouping) |
| `--aft` | — | After-group dependency (wait for this group to complete) |
| `-c`, `--con` | — | Concurrency key — `key` or `key:N` (N = max concurrent reservations, default 1) |
| `-a`, `--addr` | `localhost:11300` | Server address |

```bash
# Put a job on a specific tube with priority
tuber put -t emails --pri 100 "send welcome email"

# Pipe jobs from a file
cat jobs.txt | tuber put -t batch

# Put a job with a concurrency key
tuber put -c deploy "deploy-service-a"

# Put grouped jobs with a dependent follow-up
tuber put -g import "import-row-1"
tuber put -g import "import-row-2"
tuber put --aft import "send-summary"
```

### Work

Reserve and execute jobs as shell commands.

```bash
tuber work [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `-t`, `--tube` | `default` | Tube to watch |
| `-j`, `--parallel` | `1` | Number of parallel workers |
| `-a`, `--addr` | `localhost:11300` | Server address |

```bash
# Process jobs from the "emails" tube with 4 workers
tuber work -t emails -j 4
```

### Tubes

List all tubes with a summary of job counts.

```bash
tuber tubes [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `-a`, `--addr` | `localhost:11300` | Server address |

```bash
$ tuber tubes
default: ready=4 reserved=0 delayed=0 buried=0
my-tube: ready=16 reserved=0 delayed=0 buried=0
```

### Stats

Show global server statistics or per-tube statistics. See [Statistics Reference](docs/statistics.md) for all available fields.

```bash
tuber stats [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `-t`, `--tube` | — | Tube name (omit for global stats) |
| `-a`, `--addr` | `localhost:11300` | Server address |

```bash
# Global stats
tuber stats

# Per-tube stats
tuber stats -t emails
```

## Protocol Reference

Tuber speaks the [beanstalkd protocol](https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt) — any beanstalkd client library works out of the box. Commands marked with **+** are tuber extensions.

All commands are `\r\n`-terminated. `<id>` is a 64-bit job ID, `<pri>` is a 32-bit priority (0 = most urgent), `<delay>` and `<ttr>` are seconds, `<bytes>` is body length.

### Producer commands

| Command | Description |
|---|---|
| `put <pri> <delay> <ttr> <bytes> [tags]\r\n<body>\r\n` | Submit a job. Returns `INSERTED <id>` or `BURIED <id>`. |
| `use <tube>\r\n` | Set the tube for subsequent `put` commands. Returns `USING <tube>`. |

**+ Put extension tags** — append space-separated tags after `<bytes>`:

| Tag | Effect |
|---|---|
| `idp:<key>` or `idp:<key>:<ttl>` | Idempotency — deduplicates jobs by key within the tube. Optional TTL (seconds) keeps deduplicating after deletion. See [Unique Jobs](#unique-jobs-idempotency). |
| `grp:<name>` | Assigns the job to a group for fan-out/fan-in. See [Job Groups](#job-groups-fan-out--fan-in). |
| `aft:<name>` | Holds the job until all jobs in the named group are deleted. See [Job Groups](#job-groups-fan-out--fan-in). |
| `con:<key>` or `con:<key>:<limit>` | Concurrency key — limits how many jobs per key can be reserved at once (default 1). See [Concurrency Keys](#concurrency-keys). |

### Worker commands

| Command | Description |
|---|---|
| `reserve\r\n` | Block until a job is available. Returns `RESERVED <id> <bytes>\r\n<body>`. |
| `reserve-with-timeout <seconds>\r\n` | Like `reserve` but times out. Returns `RESERVED …` or `TIMED_OUT`. |
| `reserve-job <id>\r\n` | Reserve a specific job by ID. Returns `RESERVED …` or `NOT_FOUND`. |
| **+** `reserve-batch <count>\r\n` | Reserve up to `<count>` jobs at once (1–1000). Non-blocking — returns whatever is available. See [Batch Operations](#batch-operations). |
| **+** `reserve-mode <mode>\r\n` | Set reserve strategy: `default` (priority-first), `weighted` (random by tube weight), or `weighted-fair` (adjusted for processing time). See [Weighted Reserve](#weighted-reserve). |
| `delete <id>\r\n` | Delete a job. Returns `DELETED` or `NOT_FOUND`. |
| **+** `delete-batch <id> …\r\n` | Delete multiple jobs by ID (1–1000, space-separated). Returns `DELETED_BATCH <deleted> <not_found>`. See [Batch Operations](#batch-operations). |
| `release <id> <pri> <delay>\r\n` | Release a reserved job back to ready (or delayed). Returns `RELEASED`. |
| `bury <id> <pri>\r\n` | Bury a reserved job. Returns `BURIED`. |
| `touch <id>\r\n` | Reset the TTR timer on a reserved job. Returns `TOUCHED`. |
| `watch <tube> [weight]\r\n` | Add a tube to the watch list. Optional **+** weight for weighted mode. Returns `WATCHING <count>`. |
| `ignore <tube>\r\n` | Remove a tube from the watch list. Returns `WATCHING <count>` or `NOT_IGNORED`. |

### Peek / inspect commands

| Command | Description |
|---|---|
| `peek <id>\r\n` | Peek at a job by ID. Returns `FOUND <id> <bytes>\r\n<body>` or `NOT_FOUND`. |
| `peek-ready\r\n` | Peek at the next ready job in the used tube. |
| `peek-delayed\r\n` | Peek at the next delayed job in the used tube. |
| `peek-buried\r\n` | Peek at the next buried job in the used tube. |

### Admin commands

| Command | Description |
|---|---|
| `kick <bound>\r\n` | Kick up to `<bound>` buried/delayed jobs in the used tube. Returns `KICKED <count>`. |
| `kick-job <id>\r\n` | Kick a specific buried or delayed job. Returns `KICKED` or `NOT_FOUND`. |
| `pause-tube <tube> <delay>\r\n` | Pause a tube for `<delay>` seconds. Returns `PAUSED`. |
| **+** `flush-tube <tube>\r\n` | Delete all jobs from a tube. Returns `FLUSHED <count>`. |
| `stats\r\n` | Server-wide statistics in YAML. See [Statistics Reference](docs/statistics.md). |
| `stats-job <id>\r\n` | Statistics for a single job in YAML. See [Statistics Reference](docs/statistics.md#job-stats-stats-job-id). |
| `stats-tube <tube>\r\n` | Statistics for a tube in YAML. See [Statistics Reference](docs/statistics.md#tube-stats-stats-tube-tube). |
| **+** `stats-group <name>\r\n` | Statistics for a job group in YAML. See [Statistics Reference](docs/statistics.md#group-stats-stats-group-name). |
| `list-tubes\r\n` | List all existing tubes in YAML. |
| `list-tube-used\r\n` | Show the currently used tube. Returns `USING <tube>`. |
| `list-tubes-watched\r\n` | List watched tubes in YAML. |
| `drain\r\n` | Enter drain mode: rejects new `put` commands with `DRAINING` while allowing workers to finish existing jobs. Also triggered by `SIGUSR1`. |
| **+** `undrain\r\n` | Exit drain mode: resumes accepting `put` commands. Returns `NOT_DRAINING`. |
| `quit\r\n` | Close the connection. |

## Performance

Tuber achieves throughput comparable to beanstalkd on standard workloads. Indicative numbers from a single-client benchmark on localhost (100k jobs, Apple M-series):

| Scenario | PUT/s | Reserve+Delete/s |
|---|---|---|
| Small body, no WAL | ~34,000 | ~7,300 |
| Small body, WAL | ~26,500 | ~6,300 |
| 4KB body, no WAL | ~27,000 | ~7,300 |
| 4KB body, WAL | ~18,000 | ~6,600 |

The batch API (`reserve-batch`, `delete-batch`) significantly improves throughput by amortising per-command overhead:

| Scenario | Reserve+Delete/s |
|---|---|
| Individual reserve + delete | ~7,300 |
| Batch reserve (1000) + individual delete | ~32,500 |
| Batch reserve (1000) + batch delete (1000) | ~300,000 |

Results will vary by hardware, network, and workload. Run your own benchmarks for production sizing.

## Claude Code Skill

The `skill/` directory contains a [Claude Code skill](https://support.claude.com/en/articles/12512198-how-to-create-custom-skills) that teaches AI coding agents how to interact with Tuber (and beanstalkd) using `echo` and `nc`. It covers the full protocol with copy-paste examples.

To install it globally in Claude Code:

```bash
ln -s "$(pwd)/skill" ~/.claude/skills/tuber
```

## License

MIT — see [LICENSE](LICENSE).

Originally created by Keith Rarick and contributors. The original beanstalkd is licensed under the [MIT License](https://github.com/beanstalkd/beanstalkd/blob/master/LICENSE).
