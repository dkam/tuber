# Protocol Reference

beanstalkd uses a text-based protocol over TCP. Commands and responses are
terminated by `\r\n`. Job bodies are raw bytes of the declared length.

## Producer Commands

### put \<pri\> \<delay\> \<ttr\> \<bytes\> [tags...]\r\n\<body\>\r\n

Insert a job. Body must be exactly `<bytes>` bytes followed by `\r\n`.

Optional extension tags (space-separated, after `<bytes>`):

| Tag | Description |
|---|---|
| `idp:<key>` or `idp:<key>:<ttl>` | Idempotency key. Deduplicates within the tube — if a live job with the same key exists, returns the original ID and state. Optional TTL (seconds) keeps deduplicating after the job is deleted. |
| `grp:<name>` | Assign the job to a named group for fan-out/fan-in. |
| `aft:<name>` | Hold the job until all jobs in the named group are deleted. |
| `con:<key>` or `con:<key>:<N>` | Concurrency key. Limits parallel reservations sharing this key. Default limit is 1. |

Responses:
- `INSERTED <id>\r\n` — new job created
- `INSERTED <id> <STATE>\r\n` — idempotency dedup hit (STATE is `READY`, `RESERVED`, `DELAYED`, `BURIED`, or `DELETED`)
- `BURIED <id>\r\n` (out of memory)
- `EXPECTED_CRLF\r\n` (body not terminated correctly)
- `JOB_TOO_BIG\r\n`
- `DRAINING\r\n`
- `NOT_DRAINING\r\n`

### use \<tube\>\r\n

Switch the connection's use tube.

Response: `USING <tube>\r\n`

## Worker Commands

### reserve\r\n / reserve-with-timeout \<seconds\>\r\n

Reserve a job from watched tubes. Blocks until a job is available or timeout.

Responses:
- `RESERVED <id> <bytes>\r\n<body>\r\n`
- `TIMED_OUT\r\n`
- `DEADLINE_SOON\r\n` — the connection has a reserved job whose TTR is about to expire (see below).

### reserve-job \<id\>\r\n

Reserve a specific job by ID (regardless of tube).

Responses:
- `RESERVED <id> <bytes>\r\n<body>\r\n`
- `NOT_FOUND\r\n`

### delete \<id\>\r\n

Delete a job.

Response: `DELETED\r\n` or `NOT_FOUND\r\n`

### release \<id\> \<pri\> \<delay\>\r\n

Release a reserved job back to ready (or delayed).

Response: `RELEASED\r\n`, `BURIED\r\n`, or `NOT_FOUND\r\n`

### bury \<id\> \<pri\>\r\n

Bury a reserved job.

Response: `BURIED\r\n` or `NOT_FOUND\r\n`

### touch \<id\>\r\n

Reset the TTR timer on a reserved job.

Response: `TOUCHED\r\n` or `NOT_FOUND\r\n`

### watch \<tube\> [weight]\r\n

Add a tube to the watch set. Optional weight for weighted reserve mode.

Response: `WATCHING <count>\r\n`

### ignore \<tube\>\r\n

Remove a tube from the watch set.

Response: `WATCHING <count>\r\n` or `NOT_IGNORED\r\n`

### reserve-mode \<mode\>\r\n

Set reserve mode to `fifo` or `weighted`.

Response: `USING <mode>\r\n` or `BAD_FORMAT\r\n`

#### DEADLINE_SOON

When a connection has a reserved job whose TTR (time-to-run) is within 1 second of expiring, the server returns `DEADLINE_SOON` instead of reserving a new job. This happens in two ways:

1. **On reserve**: If you send `reserve` or `reserve-with-timeout` while holding a nearly-expired reservation, the server responds immediately with `DEADLINE_SOON` (unless a ready job is available, in which case it's returned normally).

2. **Proactive wake**: If you're already blocked waiting on a `reserve-with-timeout` and one of your reserved jobs enters the 1-second safety margin, the server interrupts the wait and sends `DEADLINE_SOON` — you don't have to wait for your reserve timeout to expire.

When you receive `DEADLINE_SOON`, you should `touch`, `delete`, or `release` the expiring job before trying to reserve again. If you don't act, the job's TTR will expire and the server will return it to the ready queue for another worker to pick up.

The safety margin is 1 second. The minimum TTR is also 1 second, so a job with TTR=1 may trigger `DEADLINE_SOON` almost immediately after being reserved.

## Other Commands

### peek \<id\>\r\n / peek-ready\r\n / peek-delayed\r\n / peek-buried\r\n

Response: `FOUND <id> <bytes>\r\n<body>\r\n` or `NOT_FOUND\r\n`

### kick \<bound\>\r\n

Kick up to `<bound>` buried/delayed jobs in the use tube.

Response: `KICKED <count>\r\n`

### kick-job \<id\>\r\n

Kick a specific job.

Response: `KICKED\r\n` or `NOT_FOUND\r\n`

### stats\r\n / stats-job \<id\>\r\n / stats-tube \<tube\>\r\n

Response: `OK <bytes>\r\n<yaml>\r\n` or `NOT_FOUND\r\n`

### stats-group \<name\>\r\n

Statistics for a job group (used with `grp:`/`aft:` features).

Response: `OK <bytes>\r\n<yaml>\r\n` or `NOT_FOUND\r\n`

YAML fields: `name`, `ready`, `reserved`, `delayed`, `buried`, `waiting-jobs`.

### list-tubes\r\n / list-tube-used\r\n / list-tubes-watched\r\n

Response: `OK <bytes>\r\n<yaml>\r\n` or `USING <tube>\r\n`

### pause-tube \<tube> \<delay\>\r\n

Pause a tube for `<delay>` seconds.

Response: `PAUSED\r\n` or `NOT_FOUND\r\n`

### flush-tube \<tube\>\r\n

Delete all jobs from a tube.

Response: `FLUSHED <count>\r\n` or `NOT_FOUND\r\n`

### drain\r\n

Enter drain mode — rejects new `put` commands with `DRAINING`, allows in-flight work to complete. Also triggered by `SIGUSR1`.

Response: `DRAINING\r\n`

### undrain\r\n

Exit drain mode — resumes accepting `put` commands. Idempotent.

Response: `NOT_DRAINING\r\n`

### quit\r\n

Close the connection.

## Constants

| Constant | Value |
|---|---|
| Default port | 11300 |
| Max tube name length | 200 chars |
| Tube name chars | `A-Za-z0-9` `-+/;.$_()` |
| Default max job size | 65535 bytes |
| Max possible job size | 1GB |
| Urgent priority threshold | < 1024 |
| Default TTR | 1 second |
| Priority range | 0 (most urgent) to 4294967295 |
