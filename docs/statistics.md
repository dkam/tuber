# Statistics Reference

Tuber provides detailed statistics at the server, tube, job, and group level
via protocol commands, plus Prometheus metrics for monitoring.

## Global Stats (`stats`)

Returns server-wide statistics in YAML format.

### Job counts

| Field | Description |
|---|---|
| `current-jobs-urgent` | Jobs with priority < 1024 |
| `current-jobs-ready` | Jobs waiting to be reserved |
| `current-jobs-reserved` | Jobs currently reserved by workers |
| `current-jobs-delayed` | Jobs waiting for their delay to expire |
| `current-jobs-buried` | Buried jobs |
| `total-jobs` | Cumulative jobs created since server start |

### Connections

| Field | Description |
|---|---|
| `current-connections` | Open connections |
| `current-producers` | Connections that have issued `put` |
| `current-workers` | Connections that have issued `reserve` |
| `current-waiting` | Connections blocked waiting for a job |
| `total-connections` | Cumulative connections since server start |
| `current-tubes` | Number of tubes |

### Command counters

Each is the cumulative count since server start:

`cmd-put`, `cmd-reserve`, `cmd-reserve-with-timeout`, `cmd-reserve-mode`,
`cmd-delete`, `cmd-release`, `cmd-bury`, `cmd-kick`, `cmd-touch`,
`cmd-use`, `cmd-watch`, `cmd-ignore`,
`cmd-stats`, `cmd-stats-job`, `cmd-stats-tube`,
`cmd-peek`, `cmd-peek-ready`, `cmd-peek-delayed`, `cmd-peek-buried`,
`cmd-list-tubes`, `cmd-list-tube-used`, `cmd-list-tubes-watched`,
`cmd-pause-tube`.

### Server info

| Field | Description |
|---|---|
| `pid` | Server process ID |
| `version` | Tuber version |
| `uptime` | Seconds since server start |
| `max-job-size` | Maximum job body size in bytes |
| `job-timeouts` | Cumulative TTR expirations |
| `current-concurrency-keys` | Active concurrency keys |
| `draining` | Whether server is draining |
| `hostname` | Server hostname |
| `id` | Server instance ID (random hex) |
| `os` | Operating system |
| `platform` | Platform identifier |
| `rusage-utime` | User CPU time (seconds.microseconds) |
| `rusage-stime` | System CPU time (seconds.microseconds) |

### Write-ahead log

Only meaningful when persistence is enabled with `-b`:

| Field | Description |
|---|---|
| `binlog-oldest-index` | Oldest WAL segment number |
| `binlog-current-index` | Current WAL segment number |
| `binlog-max-size` | Maximum WAL file size |

## Tube Stats (`stats-tube <tube>`)

Returns per-tube statistics in YAML format.

### Job counts and connections

| Field | Description |
|---|---|
| `name` | Tube name |
| `current-jobs-urgent` | Urgent jobs in this tube |
| `current-jobs-ready` | Ready jobs |
| `current-jobs-reserved` | Reserved jobs |
| `current-jobs-delayed` | Delayed jobs |
| `current-jobs-buried` | Buried jobs |
| `total-jobs` | Cumulative jobs created in this tube |
| `current-using` | Connections using this tube for `put` |
| `current-watching` | Connections watching this tube |
| `current-waiting` | Connections waiting for jobs on this tube |
| `cmd-delete` | Cumulative deletes on this tube |
| `cmd-pause-tube` | Cumulative pause commands on this tube |
| `pause` | Current pause duration in seconds |
| `pause-time-left` | Seconds remaining in current pause |

### Throughput and failure rates

| Field | Description |
|---|---|
| `total-reserves` | Cumulative reserve operations |
| `total-timeouts` | Cumulative TTR timeouts |
| `total-buries` | Cumulative bury operations |
| `bury-rate` | Fraction of reserves that ended in a bury (`total-buries / total-reserves`, 0 if no reserves) |

### Processing time

These fields track how long workers take to complete jobs on this tube,
measured from `reserve` to `delete`.

| Field | Description |
|---|---|
| `processing-time-ewma` | Exponentially weighted moving average of processing time (seconds, alpha=0.1) |
| `processing-time-min` | Minimum observed processing time (seconds) |
| `processing-time-max` | Maximum observed processing time (seconds) |
| `processing-time-samples` | Number of completed jobs included in timing stats |

### Dual EWMA (fast/slow)

Job processing times often have a bimodal distribution — for example, idempotent
jobs that exit immediately vs jobs that do real work. A single EWMA blends these
into a misleading middle value. Tuber splits samples at a 100ms threshold:

| Field | Description |
|---|---|
| `processing-time-fast-threshold` | Threshold separating fast from slow jobs (seconds, currently 0.1) |
| `processing-time-ewma-fast` | EWMA for jobs completing below the threshold (alpha=0.1) |
| `processing-time-samples-fast` | Number of fast samples |
| `processing-time-ewma-slow` | EWMA for jobs completing in >= 100ms (alpha=0.1) |
| `processing-time-samples-slow` | Number of slow samples |

### Percentiles

Percentiles are computed from the last 1000 samples in the slow ring buffer
(>= 100ms). If there are no slow samples, they fall back to the fast ring
buffer (< 100ms), so tubes where all jobs are fast still get useful percentiles.

| Field | Description |
|---|---|
| `processing-time-p50` | Median processing time (seconds) |
| `processing-time-p95` | 95th percentile processing time (seconds) |
| `processing-time-p99` | 99th percentile processing time (seconds) |

All percentile fields return 0 when no samples have been recorded.

### Queue time (time-in-queue)

Queue time measures how long a job waited from `put` to `reserve` — the time
spent in the queue before a worker picked it up. Growing queue time indicates
you need more workers.

| Field | Description |
|---|---|
| `queue-time-ewma` | EWMA of queue time (seconds, alpha=0.1) |
| `queue-time-min` | Minimum observed queue time (seconds) |
| `queue-time-max` | Maximum observed queue time (seconds) |
| `queue-time-samples` | Number of reserves included in queue-time stats |

## Job Stats (`stats-job <id>`)

Returns per-job statistics in YAML format.

| Field | Description |
|---|---|
| `id` | Job ID |
| `tube` | Tube name |
| `state` | Current state: `ready`, `reserved`, `delayed`, or `buried` |
| `pri` | Priority |
| `age` | Seconds since job was created |
| `delay` | Original delay in seconds |
| `ttr` | Time-to-run in seconds |
| `time-left` | Seconds until deadline (reserved: TTR expiry, delayed: becomes ready) |
| `time-reserved` | Seconds this job has been reserved (0 if not reserved) |
| `reserves` | Times this job has been reserved |
| `timeouts` | Times this job has timed out (TTR expired) |
| `releases` | Times this job has been released |
| `buries` | Times this job has been buried |
| `kicks` | Times this job has been kicked |
| `idempotency-key` | Idempotency key (empty if none) |
| `idempotency-ttl` | Cooldown TTL in seconds after deletion (0 if none) |
| `group` | Group name (empty if none) |
| `after-group` | After-group dependency (empty if none) |
| `concurrency-key` | Concurrency key (empty if none) |
| `concurrency-limit` | Max concurrent reservations for this key |
| `file` | WAL file sequence number (0 if no WAL) |

## Group Stats (`stats-group <name>`)

Returns group state in YAML format. Returns `NOT_FOUND` if no member or
dependent has ever referenced the group, or if every member has been
deleted and there are no `aft:` waiters left (groups are reaped the
moment they go idle).

| Field | Description |
|---|---|
| `name` | Group name |
| `ready` | Members currently in the ready queue |
| `reserved` | Members currently reserved by a worker |
| `delayed` | Members currently delayed |
| `buried` | Members currently buried (block group completion) |
| `waiting-jobs` | `aft:` jobs held until this group completes |

`ready/reserved/delayed/buried` are computed by scanning live jobs at
query time, so they always reflect current state without depending on
counter bookkeeping at every transition. Field names are intentionally
bare (no `current-jobs-` prefix) since `stats-group` is a tuber
extension and has no beanstalkd-client compatibility surface.

## Prometheus Metrics

When `--metrics-port` is set, an HTTP endpoint at `/metrics` serves
Prometheus-format metrics.

### Gauges (instantaneous values)

| Metric | Description |
|---|---|
| `tuber_jobs_urgent` | Current urgent jobs |
| `tuber_jobs_ready` | Current ready jobs |
| `tuber_jobs_reserved` | Current reserved jobs |
| `tuber_jobs_delayed` | Current delayed jobs |
| `tuber_jobs_buried` | Current buried jobs |
| `tuber_connections_current` | Current connections |
| `tuber_producers_current` | Current producers |
| `tuber_workers_current` | Current workers |
| `tuber_waiting_current` | Current waiting connections |
| `tuber_tubes_current` | Current number of tubes |
| `tuber_uptime_seconds` | Server uptime |

### Counters (cumulative)

| Metric | Description |
|---|---|
| `tuber_jobs_total` | Total jobs created |
| `tuber_job_timeouts_total` | Total TTR timeouts |
| `tuber_connections_total` | Total connections |
| `tuber_cmd_total{cmd="<name>"}` | Per-command counters |

### Per-tube metrics

Each has a `tube` label:

| Metric | Description |
|---|---|
| `tuber_tube_ready_jobs{tube="..."}` | Ready jobs |
| `tuber_tube_delayed_jobs{tube="..."}` | Delayed jobs |
| `tuber_tube_buried_jobs{tube="..."}` | Buried jobs |
| `tuber_tube_reserved_jobs{tube="..."}` | Reserved jobs |
| `tuber_tube_waiting{tube="..."}` | Waiting connections |
| `tuber_tube_jobs_total{tube="..."}` | Total jobs created |
| `tuber_tube_deletes_total{tube="..."}` | Total deletes |
| `tuber_tube_bury_rate{tube="..."}` | Bury rate |
| `tuber_tube_processing_time_ewma{tube="..."}` | Processing time EWMA |
| `tuber_tube_processing_time_ewma_fast{tube="..."}` | Processing time EWMA (fast jobs) |
| `tuber_tube_processing_time_ewma_slow{tube="..."}` | Processing time EWMA (slow jobs) |
| `tuber_tube_processing_time_p50{tube="..."}` | Processing time p50 |
| `tuber_tube_processing_time_p95{tube="..."}` | Processing time p95 |
| `tuber_tube_processing_time_p99{tube="..."}` | Processing time p99 |
| `tuber_tube_queue_time_ewma{tube="..."}` | Queue time EWMA |
