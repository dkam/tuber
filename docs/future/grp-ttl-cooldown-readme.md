#### Cooldown TTL

By default, a group is removed as soon as its last job is deleted. A late `aft:` arriving moments after — say from a slow producer or out-of-order enqueue — would find no group to wait on and run immediately, even though the dependency was real.

Add a TTL with `grp:name:N` to keep the group alive for N seconds after completion. Late `aft:` jobs arriving within the window see the group as already complete and run with correct semantics:

```text
put 0 0 30 5 grp:import:300
row-1
put 0 0 30 5 grp:import:300
row-2

(reserve → delete both jobs)

put 0 0 60 14 aft:import   (arrives 30s later)
send-summary
→ runs immediately — group completed within 300s cooldown window
```

The TTL ratchets up across producers and never resets the countdown. If `grp:import:300` and `grp:import:600` are both used, the cooldown is 600s; a later `grp:import:100` has no effect. The countdown begins when the last job in the group is deleted, not when the TTL is set. `grp:name` (no TTL) keeps the original behaviour — group removed immediately. `grp:name:0` is an explicit "no cooldown" and is equivalent to `grp:name`.

During the cooldown window, `stats-group` reports the group as complete with a `cooldown-remaining` field:
