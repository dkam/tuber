# Architecture

## Overview

tuber is a Rust rewrite of [beanstalkd](https://beanstalkd.github.io/), a simple
fast work queue. It is wire-compatible with existing beanstalkd clients.

## Core Concepts

### Jobs

A job is a unit of work with a body (arbitrary bytes), priority, delay, and TTR
(time-to-run). Jobs move through a state machine:

```
         put(delay>0)          deadline
    ┌──────────────────► DELAYED ─────────────► READY
    │                       ▲                    │  ▲
    │          release(     │                    │  │
    │          delay>0)     │         reserve    │  │ kick
    │                       │                    ▼  │
  CLIENT              ◄──────────────────── RESERVED
                      release(delay=0)          │
                            │                   │ bury
                            │                   ▼
                            │                BURIED
                            │                   │
                     delete from any state ──► DELETED
```

### Tubes

Tubes are named queues. Each tube has:
- **ready heap** -- priority-ordered min-heap of jobs ready for consumption
- **delay heap** -- deadline-ordered min-heap of jobs waiting to become ready
- **buried list** -- jobs that have been buried (failed), kicked to re-enter ready
- **waiting connections** -- connections blocked on `reserve` for this tube

The default tube is named `"default"`.

### Connections

Each TCP connection has:
- **use tube** -- the tube that `put` commands insert into (default: `"default"`)
- **watch set** -- the set of tubes that `reserve` commands draw from (default: `{"default"}`)
- **reserved jobs** -- jobs currently reserved by this connection
- **reserve mode** -- FIFO (default) or weighted selection across watched tubes

## Rust Architecture

### Engine Pattern (Single-owner state)

```
  TCP Connection tasks ──(Command, response_tx)──► Engine task
                        ◄──────(Response)──────────┘
```

The C beanstalkd is single-threaded with an event loop. We preserve this
simplicity with a single **engine task** that owns all server state:

- `ServerState` holds all tubes, jobs (in a `HashMap<u64, Job>`), connections,
  and stats
- Each TCP connection spawns its own tokio task for reading/writing the socket
- Connection tasks send `Command` messages to the engine via an `mpsc` channel
- The engine processes commands sequentially (no locking needed) and sends
  responses back via per-request `oneshot` channels

For blocking `reserve`: the engine stores the `oneshot::Sender` and fires it
when a matching job becomes available (or on timeout).

The engine also runs a tick timer to:
- Promote delayed jobs to ready when their deadline passes
- Expire reserved jobs past their TTR back to ready
- Unpause paused tubes

### Job Ownership

Jobs are stored in a central `HashMap<u64, Job>`. All other structures
(tube heaps, buried lists, connection reserved lists) reference jobs by their
`u64` ID. This avoids `Rc`/`Arc`/`RefCell` entirely.

Heap entries store `(sort_key, job_id)` tuples so ordering comparisons don't
need to look up the job.

### Custom IndexHeap

Rust's `std::collections::BinaryHeap` does not support removal by index, which
is needed for deleting/kicking jobs from tube heaps. We implement a custom
`IndexHeap<K>` that maintains an internal `HashMap<u64, usize>` mapping IDs to
positions, updated during sift operations.

## Durability and Sync Modes

When persistence is enabled (`-b <dir>`), tuber maintains a Write-Ahead Log
plus a separate body store ("TOAST"). Both must be `fsync`'d for an acked
`put` to be durable. The `--sync-interval` flag picks between two modes
with very different throughput characteristics.

| `--sync-interval` | Mode | Ack timing | Crash loss | Typical throughput (8 producers, `-b`) |
|---|---|---|---|---|
| `0` | **strict** | After fsync (group-commit batched) | nothing | hundreds of ops/sec |
| `> 0` | **relaxed** | Immediately, before fsync | up to interval of acked work | tens of thousands of ops/sec |

Strict mode is for deployments that need every acked `put` to survive power
loss. Relaxed mode is the default — it bounds crash loss to a configurable
window while keeping throughput close to the in-memory ceiling.

### How the engine batches: the drain loop

The engine task processes one `EngineMsg` at a time, but it does so in a
shape that lets group commit do useful work in strict mode.

```rust
msg = engine_rx.recv() => {                  // suspend until a message arrives
    process_message(msg);                    // handle the one we just got

    if strict {                              // only when batching pays off
        while pending.len() < MAX_BATCH {
            match engine_rx.try_recv() {     // non-blocking: empty? exit
                Ok(m) => process_message(m),
                Err(_) => break,
            }
        }
        sync_wal();                          // ONE fsync covers the whole batch
        drain_pending();                     // release every deferred ack
    }
    // relaxed mode falls straight through here — process_message already
    // sent each ack as Immediate. select! yields, per-conn tasks deliver.
}
```

The two phases:

1. **`recv()`** awaits — the engine yields the runtime until at least one
   message lands on the channel.
2. **`try_recv()`** in a loop — once we're awake, greedily pull every other
   message that's *already queued* without blocking. This is the drain.

In strict mode, every command's ack is deferred onto a `pending` list and
released only after one shared fsync. In relaxed mode each ack goes out
immediately; the drain isn't run because there's no batch to amortise — and
running it would starve per-connection tasks that need runtime time to
deliver their `oneshot` replies to client sockets.

### Group commit walkthrough (strict mode, 12 concurrent producers)

```
t=0    producers 1..12 each fire a put → 12 EngineMsgs queued on the channel

t=tiny engine task wakes:
       recv()      → msg 1   → buffer to WAL+TOAST, defer ack
                                pending = [(tx_1, INSERTED 1)]
       try_recv()  → msg 2   → ...               → pending = [..., (tx_2, ...)]
       try_recv()  → msg 3                                    [..., (tx_3, ...)]
       ...
       try_recv()  → msg 12                                   [..., (tx_12, ...)]
       try_recv()  → Empty   → exit drain

t=tiny state.sync_wal():
         fsync TOAST  (~7 ms)
         fsync WAL    (~7 ms)               ← ONE pair, covers all 12 puts

t=14ms drain_pending() walks the 12 entries:
         tx_1.send(INSERTED 1)   ─┐
         tx_2.send(INSERTED 2)    │  fires nearly simultaneously
         ...                      │
         tx_12.send(INSERTED 12) ─┘

t=14ms+ select! yields → per-conn tasks wake → all 12 producers see INSERTED
```

12 puts in ~14 ms ≈ **850 ops/sec**. Without group commit it would be 12
sequential fsync pairs ≈ 170 ms total → ~70 ops/sec. The speedup scales with
however many producers are in flight — `MAX_BATCH = 512` is the cap.

The cost is per-put latency: every producer waits the full ~14 ms for its
ack, vs. some hypothetical first-finisher seeing it sooner. *Throughput*
scales with concurrency; *latency* is fixed at "one fsync pair." That's
the classic group-commit trade.

### Sync ordering invariant

TOAST is fsync'd before WAL, never the other way round. A crash mid-sync
leaves orphan bodies (TOAST has bytes the WAL never references) — wasted
space, recoverable on restart via `BodyStore::delete_many`. The reverse
ordering would leave **dangling references** (a WAL record pointing at a
body that didn't make it to disk), which would surface as `InternalError`
on `reserve` and require manual cleanup. This is why we don't run the two
fsyncs in parallel: you'd halve the latency but trade the zero-zombie
guarantee for a recovery class we never want to handle.

### Tick branch — the SLA backstop

In relaxed mode the engine doesn't fsync inside the recv arm. The tick
branch carries that responsibility:

```rust
_ = tick_interval.tick() => {
    state.tick();                            // promote delayed, expire TTR, etc.
    if state.wal_is_dirty() {
        let due = state.wal_sync_interval()
            .zip(state.wal_last_sync_elapsed())
            .is_some_and(|(int, el)| int.is_zero() || el >= int);
        if due { state.sync_wal(); }
    }
}
```

`tick_period = min(sync_interval, 100 ms)` so the SLA is honoured even
under sustained traffic that never lets the channel empty. `state.tick()`
itself can dirty the WAL (TTR expiry writes Reserved → Ready state changes),
which is why the dirty-check happens *after* `state.tick()`.
