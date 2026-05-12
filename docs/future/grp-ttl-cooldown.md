# `grp:<name>:<ttl>` — group lingering after completion

> **Status: deferred (YAGNI).** Captured here so it can be revived if the
> NOT_FOUND ambiguity becomes a real problem for downstream pipeline tooling.
> The companion stats-group consistency change (per-state counters
> ready/reserved/delayed/buried/waiting-jobs) is being landed separately —
> without the TTL machinery.

## Context

Today `grp:` is tracked in `GroupState { pending, buried, waiting_jobs }`
(`src/server.rs:138-171`) and the entry is removed the instant the group goes
idle (`src/server.rs:2722-2730`). Consequence: `stats-group <name>` returns
`NOT_FOUND` for both "never existed" and "already finished" — so external
pipelines cannot use it as a completion signal without falling back to an
`aft:` sentinel job.

This change extends the producer syntax to `grp:<name>:<ttl>` (TTL in seconds),
matching the existing `idp:<key>:<ttl>` and `con:<key>:<limit>` shape
(`src/protocol.rs:326-364`). After the group becomes idle, its `GroupState`
lingers for `ttl` seconds so `stats-group` returns `complete: true` (with the
remaining `ttl`) before the entry is reaped.

## Confirmed decisions

- **Restart behaviour:** survive across restart by mirroring the idp
  tombstone mechanism. FullJob records carry `(group_name, ttl)` so puts
  replay with the right TTL and counters rebuild from live `JobState`. When
  a group first becomes idle with `ttl > 0`, emit a `GroupCooldown { name,
  expires_at_epoch_secs }` record — the same shape as idp tombstones at
  `src/wal.rs:1206-1219` — which keeps its segment alive until expiry and
  is collected on replay into a counter-zero `GroupState` with `expires_at`
  restored. No need to journal counters: they're derived.
- **`ttl: 0` semantics:** bare `grp:<name>` and `grp:<name>:0` are
  equivalent — both express "no linger". Max-wins still applies, so `:0`
  cannot override an existing positive TTL. There is intentionally no
  producer-side way to forcibly clear a TTL; this protects polling consumers
  from a late short-TTL put racing the linger window to zero.
- **`stats-group` shape:** rework to match `stats-tube` vocabulary:
  ```
  ready: <u64>
  reserved: <u64>
  delayed: <u64>
  buried: <u64>
  waiting-jobs: <u64>
  ```
  When the group is empty (ready+reserved+delayed+buried == 0) and lingering,
  also emit `cooldown-remaining: <secs>`. The legacy `pending` and
  `complete` fields are dropped — `ready/reserved/delayed/buried == 0` is the
  completion signal, and `cooldown-remaining` distinguishes "lingering" from
  "just emptied this tick".
- **TTL conflict resolution:** max-wins. Mirrors the `con:` limit merge at
  `src/server.rs:1001-1004` and is conservative — a group never expires
  earlier than any producer asked.
- **Late member during linger:** resurrect the group. Clear `expires_at`,
  increment `pending`, normal flow resumes. Matches the lazy
  "groups appear on first put" model.

## Pros

- Resolves the NOT_FOUND ambiguity for pipeline pollers.
- Mirrors a pattern producers and docs already speak.
- Pure derived state — no FullJob schema change, no replay change.
- Cheap memory: one `Option<Instant>` + `u32 ttl` per group.
- Backward compatible: bare `grp:<name>` keeps current behaviour (TTL=0).

## Cons / accepted trade-offs

- A stale repeating producer can keep a group "alive" indefinitely via the
  resurrect path. This is the same risk a producer already has with any
  `grp:` use; we don't try to fence it.
- Another HashMap to sweep on tick; folded into the existing
  idp-tombstone sweep cadence.
- TTL is producer-supplied even though the consumer is the poller — same
  trade-off `idp:` already makes.
- New WAL record type (`GroupCooldown`) — small schema cost but mirrors a
  pattern already in the file. WAL GC must learn not to drop a segment that
  holds an unexpired `GroupCooldown` record (idp tombstones already pin
  segments this way; reuse the mechanism).

## Implementation

1. **Parser** — `src/protocol.rs:342-346`: extend `grp:` parsing to accept an
   optional `:<ttl>`, copying the `rfind(':')` split pattern used for `idp:`
   at lines 326-340. Change `Command::Put`'s `group` field from
   `Option<String>` to `Option<(String, u32)>` (lines 12, 88, and the test
   fixtures at 582-607, 887-966, 1010-1022).

2. **GroupState** — `src/server.rs:138-171`: replace `pending: u64` with
   per-state counters `ready: u64, reserved: u64, delayed: u64`. Keep
   `buried: u64`. Add `ttl: u32` and `expires_at: Option<Instant>`. New
   helpers: `total_live() = ready + reserved + delayed + buried`,
   `is_complete() = total_live() == 0`, `is_idle()` returns true only when
   `is_complete()` AND `waiting_jobs.is_empty()` AND `expires_at` is None or
   in the past.

3. **State-transition bookkeeping** — every place a job changes state needs
   to bump/decrement the group counters. The current `pending` mutations at
   `src/server.rs:1019, 1440, 1577` cover only put/delete; we'll need more.
   Required touch points:
   - put → ready (`1019`)
   - put with delay → delayed (`1034-1041`)
   - put held for after-group → delayed (`1042-1053`)
   - reserve → ready→reserved
   - release → reserved→ready (or reserved→delayed if delay>0)
   - touch — no state change
   - bury → reserved→buried (`1745`)
   - kick → buried→ready (or buried→delayed)
   - delayed→ready promotion in tick (`server/tick.rs`)
   - delete from any state → decrement the matching counter (`1440, 1577,
     1920, 2024`)
   - timeout → reserved→ready
   Centralise this in a helper `group_state_transition(job, from, to)` on
   `ServerState` to avoid drift.

4. **Put paths** — `src/server.rs:1013-1031`: on group creation/lookup, set
   `ttl = ttl.max(existing_ttl)` (max-wins). On the resurrect path
   (group was idle, lingering): clear `expires_at`. Increment the
   appropriate state counter (ready or delayed).

5. **Idle cleanup** — `src/server.rs:2722-2730`: when `is_complete()` first
   becomes true and `waiting_jobs` is empty, if `ttl > 0`:
   - set `expires_at = Some(now + Duration::from_secs(ttl as u64))`
   - emit a `GroupCooldown { name, expires_at_epoch_secs }` WAL record
   - keep the entry
   If `ttl == 0`, remove as today. On the resurrect path, no need to emit a
   "cancel" record — the next idle transition will write a fresh
   `GroupCooldown` that supersedes the older one during replay (last-wins
   per group name).

6. **Expiry sweep** — `src/server/tick.rs` alongside the idp tombstone sweep
   (`src/server/tick.rs:180-194`): iterate `self.groups`, drop entries where
   `expires_at` is `Some(t)` and `t <= now`. Same cadence as the existing
   sweep; no new tick interval.

7. **Stats** — rewrite `src/server.rs:2296-2316` to emit:
   ```yaml
   ---
   name: "<group>"
   ready: <u64>
   reserved: <u64>
   delayed: <u64>
   buried: <u64>
   waiting-jobs: <u64>
   ```
   When `total_live() == 0` and `expires_at` is `Some(t)` with `t > now`,
   append `cooldown-remaining: <secs>` (`(t - now).as_secs()`). Otherwise
   omit it. `NOT_FOUND` continues to mean "no such group, or already swept".

8. **WAL** — `src/wal.rs`:
   - Extend the FullJob payload to carry `(group_name, ttl)`. Add a new
     `write_option_string_u32` (or pair the existing
     `write_option_string`/`read_option_string` at `src/wal.rs:222-224,
     449-450` with a `u32`) so the TTL travels with the name. Bump WAL
     version to v6; readers continue to accept v3/v4/v5 (legacy TTL=0).
   - Add a new `GroupCooldown` record type modelled on the idp tombstone
     encoding at `src/wal.rs:1206-1219`. Replay collects unexpired
     cooldowns and hands them to the engine for restoration (same shape as
     `Vec<IdpTombstone>` at `src/server/tick.rs:398`).
   - WAL GC: a segment containing an unexpired `GroupCooldown` is pinned
     (refs+=1 effectively), mirroring how idp tombstone segments stay
     alive. Decrement on expiry sweep.
   - Per-state counters are rebuilt purely from live `JobState` of replayed
     jobs — no counter records.

9. **Docs** — `docs/statistics.md:185-195` (stats-group fields) and the
   protocol/extension docs (`grp:` syntax). Note restart behaviour and the
   new `cooldown-remaining` field.

## Files to change

- `src/protocol.rs`
- `src/server.rs`
- `src/server/tick.rs`
- `src/wal.rs`
- `src/server/tests.rs` (new tests, see below)
- `docs/statistics.md`, `README.md` (extension syntax line)
- Public `client.rs` / `cmd_put.rs` group argument may need a TTL pass-through

## Verification

- **Parser unit tests** (in `protocol.rs` tests): accept `grp:G`, accept
  `grp:G:60`, reject `grp:G:`, reject `grp:G:abc`, reject `grp:G:-1`.
- **Counter transitions** (`server/tests.rs`): for each lifecycle
  (put→reserve→delete, put→reserve→release, put→reserve→bury→kick→delete,
  put with delay → ready promotion, reserve→timeout), assert the
  ready/reserved/delayed/buried sums match expectations after each step.
  This is the load-bearing test — without it, counter drift is silent.
- **TTL linger test**: put `grp:G:5`, delete the job, immediately
  `stats-group G` → all counters zero, `cooldown-remaining: 5`. Advance time
  past 5s (existing tests use `tokio::time::pause`/`advance` — see the
  `cmd_stats_group` test at `server/tests.rs:1262` for the pattern), assert
  `NOT_FOUND`.
- **Bare-grp regression**: `grp:G` with no TTL still vanishes immediately on
  idle (no `cooldown-remaining` ever appears).
- **Max-wins conflict**: two puts `grp:G:5` and `grp:G:60`; after both
  delete, `cooldown-remaining` starts at ~60.
- **Resurrect**: put `grp:G:60`, delete, put `grp:G:60` during linger →
  `ready=1`, no `cooldown-remaining` field.
- **`:0` is no-op against existing TTL**: put `grp:G:60`, put `grp:G:0`,
  delete both → `cooldown-remaining` ~60.
- **WAL roundtrip for lingering group**: put `grp:G:60`, delete, kill server
  while linger is still active, restart, `stats-group G` reports
  `cooldown-remaining` (allowing for elapsed wall-clock time).
- **WAL GC pinning**: with `grp:G:3600` and a tiny segment size, force WAL
  rotation+GC after the delete; assert the segment holding the
  `GroupCooldown` record is not dropped until expiry.
- **WAL roundtrip**: put `grp:G:60`, kill server before delete, restart, the
  job replays with the right TTL (verify via `stats-group` after deleting
  post-restart).
- **Manual**: `cargo run -- -l 0.0.0.0 -p 11300 -b /tmp/tuber-data`, then via
  `tuber-cli`: put a job with `grp:G:30`, delete it, `tuber stats-group G`
  shows zeroed counters and `cooldown-remaining: 30`, wait, becomes
  `NOT_FOUND`.
- `cargo clippy && cargo fmt -- --check && cargo test`.
