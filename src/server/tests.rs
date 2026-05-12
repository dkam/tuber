use super::*;

fn make_state() -> ServerState {
    ServerState::new(65535, None, None, None)
}

fn register(state: &mut ServerState) -> u64 {
    static NEXT_CONN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let id = NEXT_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    state.conns.insert(id, ConnState::new(id));
    state.stats.total_connections += 1;
    state.ensure_tube("default");
    if let Some(t) = state.tubes.get_mut("default") {
        t.watching_ct += 1;
        t.using_ct += 1;
    }
    id
}

fn put_cmd(pri: u32, delay: u32, ttr: u32, bytes: u32) -> Command {
    Command::Put {
        pri,
        delay,
        ttr,
        bytes,
        idempotency_key: None,
        group: None,
        after_group: None,
        concurrency_key: None,
    }
}

fn put_con_cmd(pri: u32, ttr: u32, bytes: u32, key: &str, limit: u32) -> Command {
    Command::Put {
        pri,
        delay: 0,
        ttr,
        bytes,
        idempotency_key: None,
        group: None,
        after_group: None,
        concurrency_key: Some((key.to_string(), limit)),
    }
}

#[test]
fn test_put_and_reserve() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(c, put_cmd(0, 0, 10, 5), Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    let resp = s.handle_command(c, Command::Reserve, None);
    match resp {
        Response::Reserved { id, body } => {
            assert_eq!(id, 1);
            assert_eq!(body, b"hello");
        }
        _ => panic!("expected Reserved, got {:?}", resp),
    }
}

#[test]
fn test_priority_ordering() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(5, 0, 10, 1), Some(b"a".to_vec()));
    s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"b".to_vec()));
    s.handle_command(c, put_cmd(3, 0, 10, 1), Some(b"c".to_vec()));

    // Should get priority 1 first
    let resp = s.handle_command(c, Command::Reserve, None);
    match resp {
        Response::Reserved { body, .. } => assert_eq!(body, b"b"),
        _ => panic!("expected Reserved"),
    }
}

#[test]
fn test_fifo_same_priority() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"a".to_vec()));
    s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"b".to_vec()));
    s.handle_command(c, put_cmd(1, 0, 10, 1), Some(b"c".to_vec()));

    let resp = s.handle_command(c, Command::Reserve, None);
    match resp {
        Response::Reserved { body, .. } => assert_eq!(body, b"a"),
        _ => panic!("expected Reserved"),
    }
    let resp = s.handle_command(c, Command::Reserve, None);
    match resp {
        Response::Reserved { body, .. } => assert_eq!(body, b"b"),
        _ => panic!("expected Reserved"),
    }
}

#[test]
fn test_delete_ready() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    let resp = s.handle_command(c, Command::Peek { id: 1 }, None);
    assert!(matches!(resp, Response::NotFound));
}

#[test]
fn test_bury_and_kick() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    let resp = s.handle_command(c, Command::Reserve, None);
    let id = match resp {
        Response::Reserved { id, .. } => id,
        _ => panic!("expected Reserved"),
    };

    let resp = s.handle_command(c, Command::Bury { id, pri: 0 }, None);
    assert!(matches!(resp, Response::Buried));

    let resp = s.handle_command(c, Command::Kick { bound: 1 }, None);
    assert!(matches!(resp, Response::Kicked(1)));

    // Job should be ready again
    let resp = s.handle_command(c, Command::PeekReady, None);
    assert!(matches!(resp, Response::Found { .. }));
}

#[test]
fn test_multi_tube() {
    let mut s = make_state();
    let producer = register(&mut s);
    let consumer = register(&mut s);

    // Producer uses "emails"
    s.handle_command(
        producer,
        Command::Use {
            tube: "emails".into(),
        },
        None,
    );
    s.handle_command(producer, put_cmd(0, 0, 10, 5), Some(b"hello".to_vec()));

    // Consumer watches "emails", reserve should find it
    s.handle_command(
        consumer,
        Command::Watch {
            tube: "emails".into(),
            weight: 1,
        },
        None,
    );
    let resp = s.handle_command(consumer, Command::Reserve, None);
    match resp {
        Response::Reserved { body, .. } => assert_eq!(body, b"hello"),
        _ => panic!("expected Reserved, got {:?}", resp),
    }
}

#[test]
fn test_release_to_ready() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(5, 0, 10, 1), Some(b"x".to_vec()));
    let resp = s.handle_command(c, Command::Reserve, None);
    let id = match resp {
        Response::Reserved { id, .. } => id,
        _ => panic!(),
    };

    let resp = s.handle_command(
        c,
        Command::Release {
            id,
            pri: 0,
            delay: 0,
        },
        None,
    );
    assert!(matches!(resp, Response::Released));

    // Should be back in ready with new priority
    let resp = s.handle_command(c, Command::PeekReady, None);
    assert!(matches!(resp, Response::Found { .. }));
}

#[test]
fn test_touch() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    let resp = s.handle_command(c, Command::Reserve, None);
    let id = match resp {
        Response::Reserved { id, .. } => id,
        _ => panic!(),
    };

    let resp = s.handle_command(c, Command::Touch { id }, None);
    assert!(matches!(resp, Response::Touched));
}

#[test]
fn test_drain_mode() {
    let mut s = make_state();
    let c = register(&mut s);
    s.drain_mode = true;

    let resp = s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    assert!(matches!(resp, Response::Draining));
}

#[test]
fn test_undrain_mode() {
    let mut s = make_state();
    let c = register(&mut s);

    // Enter drain mode
    let resp = s.handle_command(c, Command::Drain, None);
    assert!(matches!(resp, Response::Draining));

    // Put should fail
    let resp = s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    assert!(matches!(resp, Response::Draining));

    // Undrain
    let resp = s.handle_command(c, Command::Undrain, None);
    assert!(matches!(resp, Response::NotDraining));

    // Put should work again
    let resp = s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    assert!(matches!(resp, Response::Inserted(_)));
}

#[test]
fn test_close_releases_jobs() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    s.handle_command(c, Command::Reserve, None);

    // Disconnect
    s.unregister_conn(c);

    // Job should be back in ready (for another consumer)
    assert_eq!(s.ready_ct, 1);
}

#[test]
fn test_reserve_mode_weighted() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(
        c,
        Command::ReserveMode {
            mode: "weighted".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Using(m) if m == "weighted"));

    let resp = s.handle_command(
        c,
        Command::ReserveMode {
            mode: "fifo".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Using(m) if m == "fifo"));

    let resp = s.handle_command(
        c,
        Command::ReserveMode {
            mode: "invalid".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::BadFormat));
}

#[test]
fn test_reserve_mode_weighted_fair() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(
        c,
        Command::ReserveMode {
            mode: "weighted-fair".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Using(m) if m == "weighted-fair"));

    // Switch back to fifo
    let resp = s.handle_command(
        c,
        Command::ReserveMode {
            mode: "fifo".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Using(m) if m == "fifo"));
}

#[test]
fn test_weighted_fair_favors_fast_tubes() {
    let mut s = make_state();
    let c = register(&mut s);

    // Set up weighted-fair mode
    s.handle_command(
        c,
        Command::ReserveMode {
            mode: "weighted-fair".into(),
        },
        None,
    );

    // Watch "fast" tube (weight 1) and "slow" tube (weight 1)
    s.handle_command(
        c,
        Command::Watch {
            tube: "fast".into(),
            weight: 1,
        },
        None,
    );
    s.handle_command(
        c,
        Command::Watch {
            tube: "slow".into(),
            weight: 1,
        },
        None,
    );
    s.handle_command(
        c,
        Command::Ignore {
            tube: "default".into(),
        },
        None,
    );

    // Put jobs into both tubes
    s.handle_command(
        c,
        Command::Use {
            tube: "fast".into(),
        },
        None,
    );
    for _ in 0..100 {
        s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"f".to_vec()));
    }

    s.handle_command(
        c,
        Command::Use {
            tube: "slow".into(),
        },
        None,
    );
    for _ in 0..100 {
        s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"s".to_vec()));
    }

    // Set processing time EWMAs: fast=0.1s, slow=10.0s (100x slower)
    s.tubes.get_mut("fast").unwrap().stat.processing_time_ewma = 0.1;
    s.tubes
        .get_mut("fast")
        .unwrap()
        .stat
        .processing_time_samples = 100;
    s.tubes.get_mut("slow").unwrap().stat.processing_time_ewma = 10.0;
    s.tubes
        .get_mut("slow")
        .unwrap()
        .stat
        .processing_time_samples = 100;

    // Reserve many jobs and count which tube they came from
    let mut fast_count = 0u32;
    let mut slow_count = 0u32;
    for _ in 0..50 {
        let resp = s.handle_command(c, Command::Reserve, None);
        if let Response::Reserved { id, .. } = resp {
            let job = s.jobs.get(&id).unwrap();
            if job.tube_name == "fast" {
                fast_count += 1;
            } else {
                slow_count += 1;
            }
            // Delete so the job doesn't stay reserved
            s.handle_command(c, Command::Delete { id }, None);
        }
    }

    // With equal weights and 100x processing time difference,
    // fast tube should get ~99% of reserves.
    // Be generous: fast should get at least 80% (40 out of 50).
    assert!(
        fast_count > 40,
        "weighted-fair should heavily favor fast tube: fast={fast_count}, slow={slow_count}"
    );
}

#[test]
fn test_weighted_fair_fallback_no_ewma() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(
        c,
        Command::ReserveMode {
            mode: "weighted-fair".into(),
        },
        None,
    );

    // Watch two tubes, neither has processing time data
    s.handle_command(
        c,
        Command::Watch {
            tube: "a".into(),
            weight: 3,
        },
        None,
    );
    s.handle_command(
        c,
        Command::Watch {
            tube: "b".into(),
            weight: 1,
        },
        None,
    );
    s.handle_command(
        c,
        Command::Ignore {
            tube: "default".into(),
        },
        None,
    );

    // Put one job in each
    s.handle_command(c, Command::Use { tube: "a".into() }, None);
    s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"a".to_vec()));
    s.handle_command(c, Command::Use { tube: "b".into() }, None);
    s.handle_command(c, put_cmd(0, 0, 60, 1), Some(b"b".to_vec()));

    // Both tubes have ewma=0 so effective weights should fall back to raw weights.
    // Just verify we can reserve both without panicking.
    let resp = s.handle_command(c, Command::Reserve, None);
    assert!(matches!(resp, Response::Reserved { .. }));
}

#[test]
fn test_watch_and_ignore() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(
        c,
        Command::Watch {
            tube: "foo".into(),
            weight: 1,
        },
        None,
    );
    assert!(matches!(resp, Response::Watching(2)));

    let resp = s.handle_command(
        c,
        Command::Ignore {
            tube: "default".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Watching(1)));

    // Can't ignore last tube
    let resp = s.handle_command(c, Command::Ignore { tube: "foo".into() }, None);
    assert!(matches!(resp, Response::NotIgnored));
}

#[test]
fn test_kick_job_buried() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Bury { id: 1, pri: 0 }, None);

    let resp = s.handle_command(c, Command::KickJob { id: 1 }, None);
    assert!(matches!(resp, Response::KickedOne));
}

#[test]
fn test_pause_tube() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(
        c,
        Command::PauseTube {
            tube: "default".into(),
            delay: 60,
        },
        None,
    );
    assert!(matches!(resp, Response::Paused));

    assert!(s.tubes.get("default").unwrap().is_paused());
}

#[test]
fn test_stats_job() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(100, 0, 60, 5), Some(b"hello".to_vec()));

    let resp = s.handle_command(c, Command::StatsJob { id: 1 }, None);
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("id: 1"));
            assert!(yaml.contains("state: ready"));
            assert!(yaml.contains("pri: 100"));
        }
        _ => panic!("expected Ok"),
    }
}

#[test]
fn test_stats_job_with_extensions() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: Some("grp1".into()),
        after_group: Some("grp0".into()),
        concurrency_key: Some(("con1".into(), 1)),
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));

    let resp = s.handle_command(c, Command::StatsJob { id: 1 }, None);
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("idempotency-key: mykey"), "yaml: {}", yaml);
            assert!(yaml.contains("group: grp1"), "yaml: {}", yaml);
            assert!(yaml.contains("after-group: grp0"), "yaml: {}", yaml);
            assert!(yaml.contains("concurrency-key: con1"), "yaml: {}", yaml);
            assert!(yaml.contains("concurrency-limit: 1"), "yaml: {}", yaml);
        }
        _ => panic!("expected Ok"),
    }
}

#[test]
fn test_list_tubes() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(
        c,
        Command::Use {
            tube: "emails".into(),
        },
        None,
    );

    let resp = s.handle_command(c, Command::ListTubes, None);
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("- default"));
            assert!(yaml.contains("- emails"));
        }
        _ => panic!("expected Ok"),
    }
}

#[test]
fn test_concurrency_key_limit_n() {
    // Put 3 jobs with con:api:2, reserve 2 (succeed), 3rd blocked, delete 1, 3rd succeeds
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);

    let con_put = |s: &mut ServerState, conn: u64, body: &[u8]| {
        s.handle_command(
            conn,
            put_con_cmd(0, 120, body.len() as u32, "api", 2),
            Some(body.to_vec()),
        )
    };

    // Insert 3 jobs
    assert!(matches!(con_put(&mut s, c1, b"j1"), Response::Inserted(1)));
    assert!(matches!(con_put(&mut s, c1, b"j2"), Response::Inserted(2)));
    assert!(matches!(con_put(&mut s, c1, b"j3"), Response::Inserted(3)));

    // Reserve first two — should succeed
    let r1 = s.handle_command(c1, Command::Reserve, None);
    assert!(matches!(r1, Response::Reserved { id: 1, .. }));

    let r2 = s.handle_command(c2, Command::Reserve, None);
    assert!(matches!(r2, Response::Reserved { id: 2, .. }));

    // Third reserve should be blocked (2 already reserved, limit is 2)
    let r3 = s.handle_command(c1, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(matches!(r3, Response::TimedOut));

    // Delete one reserved job — frees a slot
    let resp = s.handle_command(c1, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    // Now reserve should succeed
    let r4 = s.handle_command(c1, Command::Reserve, None);
    assert!(matches!(r4, Response::Reserved { id: 3, .. }));
}

#[test]
fn test_concurrency_key_default_limit_one() {
    // con:key (no :N) defaults to limit 1
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);

    let con_put = |s: &mut ServerState, conn: u64, body: &[u8]| {
        s.handle_command(
            conn,
            put_con_cmd(0, 120, body.len() as u32, "single", 1),
            Some(body.to_vec()),
        )
    };

    con_put(&mut s, c1, b"j1");
    con_put(&mut s, c1, b"j2");

    // Reserve first — succeeds
    let r1 = s.handle_command(c1, Command::Reserve, None);
    assert!(matches!(r1, Response::Reserved { id: 1, .. }));

    // Second reserve should be blocked
    let r2 = s.handle_command(c2, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(matches!(r2, Response::TimedOut));
}

#[test]
fn test_cmd_delete_wakes_concurrency_waiter() {
    // A reserver parked while a con: slot is held must be fulfilled
    // synchronously when the holder deletes the job. Without process_queue
    // in cmd_delete, the waiter would only wake on the next tick.
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);

    let con_put = |s: &mut ServerState, conn: u64, body: &[u8]| {
        s.handle_command(
            conn,
            put_con_cmd(0, 120, body.len() as u32, "k", 1),
            Some(body.to_vec()),
        )
    };

    con_put(&mut s, c1, b"j1");
    con_put(&mut s, c1, b"j2");

    // c1 reserves j1, taking the only con-key slot.
    let r1 = s.handle_command(c1, Command::Reserve, None);
    assert!(matches!(r1, Response::Reserved { id: 1, .. }));

    // c2 parks as a waiter (j2 is con-blocked by j1's slot).
    let (tx, mut rx) = oneshot::channel();
    s.add_waiter(c2, tx, None);
    assert_eq!(s.stats.waiting_ct, 1);

    // c1 deletes j1 — slot freed, waiter must be fulfilled with j2.
    let resp = s.handle_command(c1, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    assert_eq!(
        s.stats.waiting_ct, 0,
        "delete must wake parked waiter when concurrency slot is freed"
    );
    let received = rx
        .try_recv()
        .expect("waiter should have received a response");
    assert!(
        matches!(received, Response::Reserved { id: 2, .. }),
        "expected Reserved(2), got {:?}",
        received
    );
}

#[test]
fn test_find_unblocked_returns_smallest_unblocked() {
    // Correctness probe for find_best_unblocked_ready: with the heap top
    // blocked by a con: key, the function must still return the
    // lowest-priority unblocked job, not just any unblocked one.
    let mut s = make_state();
    let c1 = register(&mut s);
    let _c2 = register(&mut s);

    // j1: pri 1 con:k (blocks); j2: pri 2 unblocked (target);
    // j3: pri 0 con:k (smallest overall but blocked).
    let send = |s: &mut ServerState, cmd: Command, body: &[u8]| {
        s.handle_command(c1, cmd, Some(body.to_vec()))
    };
    assert!(matches!(send(&mut s, put_con_cmd(1, 120, 1, "k", 1), b"a"), Response::Inserted(1)));
    assert!(matches!(send(&mut s, put_cmd(2, 0, 120, 1),          b"b"), Response::Inserted(2)));
    assert!(matches!(send(&mut s, put_con_cmd(0, 120, 1, "k", 1), b"c"), Response::Inserted(3)));

    // Reserve j3 (pri 0) — takes the con:k slot.
    let r = s.handle_command(c1, Command::Reserve, None);
    assert!(matches!(r, Response::Reserved { id: 3, .. }));

    // Now j1 (pri 1, con:k) is at heap top but blocked.
    // The next unblocked is j2 (pri 2, no con). We must NOT return
    // anything else, even if a future change adds out-of-order traversal.
    let r = s.handle_command(c1, Command::Reserve, None);
    assert!(
        matches!(r, Response::Reserved { id: 2, .. }),
        "expected smallest-unblocked j2, got {:?}",
        r
    );
}

#[test]
fn test_idempotency_ttl_cooldown() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put with idp:key:5
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 5)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Reserve and delete
    let resp = s.handle_command(c, Command::Reserve, None);
    assert!(matches!(resp, Response::Reserved { id: 1, .. }));
    let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    // Re-put with same key should return original ID with DELETED state (cooldown active)
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 5)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "DELETED", None)));
}

#[test]
fn test_idempotency_no_ttl_no_cooldown() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put with idp:key (no TTL)
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Reserve and delete
    let resp = s.handle_command(c, Command::Reserve, None);
    assert!(matches!(resp, Response::Reserved { id: 1, .. }));
    let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    // Re-put should get new ID (no cooldown)
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::Inserted(2)));
}

#[test]
fn test_idempotency_ttl_flush_clears_cooldowns() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put with cooldown
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 60)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Delete to start cooldown, then flush
    let resp = s.handle_command(c, Command::Reserve, None);
    assert!(matches!(resp, Response::Reserved { id: 1, .. }));
    let resp = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(matches!(resp, Response::Deleted));

    // Flush tube clears cooldowns
    let resp = s.handle_command(
        c,
        Command::FlushTube {
            tube: "default".into(),
        },
        None,
    );
    assert!(matches!(resp, Response::Flushed(0)));

    // Re-put should get new ID
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 60)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::Inserted(2)));
}

#[test]
fn test_idempotency_dedup_returns_ready_state() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Duplicate put returns InsertedDup with READY state
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "READY", None)));
}

#[test]
fn test_idempotency_dedup_returns_reserved_state() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));

    // Reserve the job
    s.handle_command(c, Command::Reserve, None);

    // Duplicate put returns InsertedDup with RESERVED state
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "RESERVED", None)));
}

#[test]
fn test_idempotency_dedup_returns_buried_state() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));

    // Reserve and bury
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Bury { id: 1, pri: 0 }, None);

    // Duplicate put returns InsertedDup with BURIED state
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "BURIED", None)));
}

#[test]
fn test_idempotency_dedup_returns_delayed_state() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 3600,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd.clone(), Some(b"hello".to_vec()));

    // Duplicate put returns InsertedDup with DELAYED state
    let resp = s.handle_command(c, cmd, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "DELAYED", None)));
}

#[test]
fn test_idempotency_cooldown_dedup_returns_deleted_state() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 60)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));

    // Reserve and delete (starts cooldown)
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Delete { id: 1 }, None);

    // Re-put during cooldown returns InsertedDup with DELETED state
    let cmd2 = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 60)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "DELETED", None)));
}

#[test]
fn test_idempotency_priority_upgrade_ready() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put job at pri 100
    let cmd1 = Command::Put {
        pri: 100,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Put another job at pri 75 (no IDP) to compare ordering
    let cmd2 = Command::Put {
        pri: 75,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: None,
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"other".to_vec()));
    assert!(matches!(resp, Response::Inserted(2)));

    // Duplicate put at pri 50 — should upgrade
    let cmd3 = Command::Put {
        pri: 50,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd3, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "READY", Some(50))));

    // Verify job 1 (now pri 50) is reserved before job 2 (pri 75)
    let reserve = Command::ReserveWithTimeout { timeout: 0 };
    let resp = s.handle_command(c, reserve, None);
    match resp {
        Response::Reserved { id, .. } => assert_eq!(id, 1),
        other => panic!("expected Reserved, got {:?}", other),
    }
}

#[test]
fn test_idempotency_priority_upgrade_no_downgrade() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put job at pri 50
    let cmd1 = Command::Put {
        pri: 50,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Duplicate put at pri 100 — should NOT downgrade
    let cmd2 = Command::Put {
        pri: 100,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "READY", None)));

    // Verify job still has pri 50
    assert_eq!(s.jobs.get(&1).unwrap().priority, 50);
}

#[test]
fn test_idempotency_priority_upgrade_reserved() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put and reserve a job at pri 100
    let cmd1 = Command::Put {
        pri: 100,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    let reserve = Command::ReserveWithTimeout { timeout: 0 };
    s.handle_command(c, reserve, None);

    // Duplicate put at pri 30 — should upgrade even though reserved
    let cmd2 = Command::Put {
        pri: 30,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(
        resp,
        Response::InsertedDup(1, "RESERVED", Some(30))
    ));

    // Verify the job's priority was updated
    assert_eq!(s.jobs.get(&1).unwrap().priority, 30);
}

#[test]
fn test_idempotency_priority_upgrade_buried() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put, reserve, and bury a job at pri 100
    let cmd1 = Command::Put {
        pri: 100,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    let reserve = Command::ReserveWithTimeout { timeout: 0 };
    s.handle_command(c, reserve, None);
    let bury = Command::Bury { id: 1, pri: 100 };
    s.handle_command(c, bury, None);

    // Duplicate put at pri 20 — should upgrade buried job
    let cmd2 = Command::Put {
        pri: 20,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "BURIED", Some(20))));
    assert_eq!(s.jobs.get(&1).unwrap().priority, 20);
}

#[test]
fn test_idempotency_priority_upgrade_delayed() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put a delayed job at pri 100
    let cmd1 = Command::Put {
        pri: 100,
        delay: 3600,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    assert!(matches!(resp, Response::Inserted(1)));

    // Duplicate put at pri 40 — should upgrade delayed job
    let cmd2 = Command::Put {
        pri: 40,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(
        resp,
        Response::InsertedDup(1, "DELAYED", Some(40))
    ));
    assert_eq!(s.jobs.get(&1).unwrap().priority, 40);
}

#[test]
fn test_idempotency_priority_upgrade_urgent_stats() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put job at pri 2000 (not urgent)
    let cmd1 = Command::Put {
        pri: 2000,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd1, Some(b"hello".to_vec()));
    assert_eq!(s.stats.urgent_ct, 0);

    // Duplicate put at pri 500 (urgent, < 1024) — should update urgent count
    let cmd2 = Command::Put {
        pri: 500,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 0)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let resp = s.handle_command(c, cmd2, Some(b"world".to_vec()));
    assert!(matches!(resp, Response::InsertedDup(1, "READY", Some(500))));
    assert_eq!(s.stats.urgent_ct, 1);
}

#[test]
fn test_stats_group_not_found() {
    let s = make_state();
    let resp = s.cmd_stats_group("nonexistent");
    assert!(matches!(resp, Response::NotFound));
}

#[test]
fn test_stats_group_pending() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: None,
        group: Some("g1".into()),
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));

    let resp = s.cmd_stats_group("g1");
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("name: \"g1\""), "yaml: {}", yaml);
            assert!(yaml.contains("ready: 1"), "yaml: {}", yaml);
            assert!(yaml.contains("reserved: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("delayed: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("buried: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("waiting-jobs: 0"), "yaml: {}", yaml);
        }
        _ => panic!("expected Ok, got {:?}", resp),
    }
}

#[test]
fn test_chained_aft_pipeline() {
    // Verify the ETL pipeline scenario: extract -> transform -> load
    // where each stage is held until the previous stage's group completes.
    let mut s = make_state();
    let c = register(&mut s);

    // Step 1: Put 2 extract jobs tagged with grp:extract
    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: None,
            group: Some("extract".into()),
            after_group: None,
            concurrency_key: None,
        },
        Some(b"ext-1".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(1)));

    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: None,
            group: Some("extract".into()),
            after_group: None,
            concurrency_key: None,
        },
        Some(b"ext-2".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(2)));

    // Step 2: Put 1 transform job with aft:extract grp:transform
    // It should be held because the extract group is not yet complete.
    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: None,
            group: Some("transform".into()),
            after_group: Some("extract".into()),
            concurrency_key: None,
        },
        Some(b"xform".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(3)));

    // Step 3: Put 1 load job with aft:transform
    // It should be held because the transform group is not yet complete.
    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 5,
            idempotency_key: None,
            group: None,
            after_group: Some("transform".into()),
            concurrency_key: None,
        },
        Some(b"load!".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(4)));

    // Step 4: Verify the transform job (3) and load job (4) are not reservable
    // while the extract group still has pending jobs.
    // Only extract jobs 1 and 2 should be available.
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::Reserved { id: 1, .. }),
        "expected extract job 1, got {:?}",
        r
    );
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::Reserved { id: 2, .. }),
        "expected extract job 2, got {:?}",
        r
    );
    // No more ready jobs -- transform is still held
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::TimedOut),
        "expected TimedOut (transform held), got {:?}",
        r
    );

    // Step 5: Delete both extract jobs. This completes the extract group,
    // which should promote the transform job to ready.
    s.handle_command(c, Command::Delete { id: 1 }, None);
    s.handle_command(c, Command::Delete { id: 2 }, None);

    // Step 6: Verify the transform job is now reservable.
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::Reserved { id: 3, .. }),
        "expected transform job 3 after extract completes, got {:?}",
        r
    );
    // Load job is still held -- transform group not yet complete
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::TimedOut),
        "expected TimedOut (load held), got {:?}",
        r
    );

    // Step 7: Delete the transform job. This completes the transform group,
    // which should promote the load job to ready.
    s.handle_command(c, Command::Delete { id: 3 }, None);

    // Step 8: Verify the load job is now reservable.
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(
        matches!(r, Response::Reserved { id: 4, .. }),
        "expected load job 4 after transform completes, got {:?}",
        r
    );
}

#[test]
fn test_stats_group_buried() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: None,
        group: Some("g1".into()),
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));

    // Reserve and bury
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Bury { id: 1, pri: 0 }, None);

    let resp = s.cmd_stats_group("g1");
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("ready: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("reserved: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("delayed: 0"), "yaml: {}", yaml);
            assert!(yaml.contains("buried: 1"), "yaml: {}", yaml);
        }
        _ => panic!("expected Ok, got {:?}", resp),
    }
}

#[test]
fn test_stats_job_idempotency_ttl() {
    let mut s = make_state();
    let c = register(&mut s);

    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 10,
        bytes: 5,
        idempotency_key: Some(("mykey".into(), 30)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));

    let resp = s.handle_command(c, Command::StatsJob { id: 1 }, None);
    match resp {
        Response::Ok(data) => {
            let yaml = String::from_utf8(data).unwrap();
            assert!(yaml.contains("idempotency-key: mykey"), "yaml: {}", yaml);
            assert!(yaml.contains("idempotency-ttl: 30"), "yaml: {}", yaml);
        }
        _ => panic!("expected Ok"),
    }
}

#[test]
fn test_delete_batch_all_reserved() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put and reserve 3 jobs
    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"a".to_vec()));
    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"b".to_vec()));
    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"c".to_vec()));
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Reserve, None);
    s.handle_command(c, Command::Reserve, None);

    let resp = s.handle_command(c, Command::DeleteBatch { ids: vec![1, 2, 3] }, None);
    assert_eq!(
        resp,
        Response::DeletedBatch {
            deleted: 3,
            not_found: 0
        }
    );
}

#[test]
fn test_delete_batch_mixed() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"a".to_vec()));
    s.handle_command(c, Command::Reserve, None);

    let resp = s.handle_command(
        c,
        Command::DeleteBatch {
            ids: vec![1, 999, 1000],
        },
        None,
    );
    assert_eq!(
        resp,
        Response::DeletedBatch {
            deleted: 1,
            not_found: 2
        }
    );
}

#[test]
fn test_delete_batch_all_not_found() {
    let mut s = make_state();
    let c = register(&mut s);

    let resp = s.handle_command(c, Command::DeleteBatch { ids: vec![99, 100] }, None);
    assert_eq!(
        resp,
        Response::DeletedBatch {
            deleted: 0,
            not_found: 2
        }
    );
}

#[test]
fn test_delete_batch_other_conn_reserved() {
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);

    // c1 reserves a job
    s.handle_command(c1, put_cmd(0, 0, 10, 1), Some(b"a".to_vec()));
    s.handle_command(c1, Command::Reserve, None);

    // c2 tries to delete-batch it — should be not_found (not reserved by c2)
    let resp = s.handle_command(c2, Command::DeleteBatch { ids: vec![1] }, None);
    assert_eq!(
        resp,
        Response::DeletedBatch {
            deleted: 0,
            not_found: 1
        }
    );
}

// --- Memory accounting (--max-jobs-size) ---

/// Sum the live memory cost of every job and tombstone from scratch.
/// Must match `state.total_job_bytes` at all times.
fn recompute_total_job_bytes(state: &ServerState) -> u64 {
    let jobs: u64 = state.jobs.values().map(ServerState::job_memory_cost).sum();
    let tombs: u64 = state
        .tubes
        .values()
        .flat_map(|t| t.idempotency_cooldowns.keys())
        .map(|k| ServerState::tombstone_memory_cost(k))
        .sum();
    jobs + tombs
}

fn make_state_with_limit(max_bytes: Option<u64>) -> ServerState {
    ServerState::new(65535, max_bytes, None, None)
}

#[test]
fn test_memory_accounting_put_delete_zero() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 60, 5), Some(b"hello".to_vec()));
    s.handle_command(c, put_cmd(0, 0, 60, 3), Some(b"foo".to_vec()));
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));
    assert_eq!(s.total_job_bytes, (5 + 512) + (3 + 512));

    s.handle_command(c, Command::Delete { id: 1 }, None);
    s.handle_command(c, Command::Delete { id: 2 }, None);
    assert_eq!(s.total_job_bytes, 0);
    assert_eq!(recompute_total_job_bytes(&s), 0);
}

#[test]
fn test_memory_accounting_reserve_release_is_neutral() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 60, 5), Some(b"hello".to_vec()));
    let baseline = s.total_job_bytes;

    let resp = s.handle_command(c, Command::Reserve, None);
    let id = match resp {
        Response::Reserved { id, .. } => id,
        _ => panic!("expected Reserved"),
    };
    // Reserve doesn't add bytes.
    assert_eq!(s.total_job_bytes, baseline);

    s.handle_command(
        c,
        Command::Release {
            id,
            pri: 0,
            delay: 0,
        },
        None,
    );
    assert_eq!(s.total_job_bytes, baseline);
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));
}

#[test]
fn test_memory_accounting_bury_kick_is_neutral() {
    let mut s = make_state();
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 60, 5), Some(b"hello".to_vec()));
    let baseline = s.total_job_bytes;

    let resp = s.handle_command(c, Command::Reserve, None);
    let id = match resp {
        Response::Reserved { id, .. } => id,
        _ => panic!("expected Reserved"),
    };
    s.handle_command(c, Command::Bury { id, pri: 0 }, None);
    assert_eq!(s.total_job_bytes, baseline);

    s.handle_command(c, Command::Kick { bound: 1 }, None);
    assert_eq!(s.total_job_bytes, baseline);
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));
}

#[test]
fn test_memory_limit_blocks_put() {
    // Budget for exactly one 10-byte job (10 + 512 = 522). A second put
    // of the same size must be rejected.
    let mut s = make_state_with_limit(Some(522));
    let c = register(&mut s);

    let r1 = s.handle_command(c, put_cmd(0, 0, 60, 10), Some(vec![b'a'; 10]));
    assert!(matches!(r1, Response::Inserted(1)), "first put: {r1:?}");

    let r2 = s.handle_command(c, put_cmd(0, 0, 60, 10), Some(vec![b'b'; 10]));
    assert!(matches!(r2, Response::OutOfMemory), "second put: {r2:?}");

    // Delete frees the budget; next put should succeed again.
    s.handle_command(c, Command::Delete { id: 1 }, None);
    assert_eq!(s.total_job_bytes, 0);

    let r3 = s.handle_command(c, put_cmd(0, 0, 60, 10), Some(vec![b'c'; 10]));
    assert!(
        matches!(r3, Response::Inserted(2)),
        "post-delete put: {r3:?}"
    );
}

#[test]
fn test_memory_limit_allows_reserve_at_capacity() {
    let mut s = make_state_with_limit(Some(522));
    let c = register(&mut s);

    s.handle_command(c, put_cmd(0, 0, 60, 10), Some(vec![b'a'; 10]));
    // At the limit. Reserve must still succeed.
    let resp = s.handle_command(c, Command::Reserve, None);
    assert!(matches!(resp, Response::Reserved { .. }));

    // And release, too.
    let resp = s.handle_command(
        c,
        Command::Release {
            id: 1,
            pri: 0,
            delay: 0,
        },
        None,
    );
    assert!(matches!(resp, Response::Released));
}

#[test]
fn test_memory_limit_put_with_idp_accounts_for_tombstone() {
    // A 5-byte body + 512 job overhead + "k1" tombstone (2 + 128 = 130)
    // = 647. Allow exactly that, put once. Then put a second fresh idp:
    // 5 + 512 = 517 of job + "k2" 2 + 128 = 130 of tombstone = 647 more.
    // Budget of 647 fits exactly one such put, second must fail.
    let mut s = make_state_with_limit(Some(647));
    let c = register(&mut s);

    let cmd_idp1 = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("k1".to_string(), 30)), // TTL > 0 → tombstone cost
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let r1 = s.handle_command(c, cmd_idp1, Some(b"hello".to_vec()));
    assert!(matches!(r1, Response::Inserted(1)), "got {r1:?}");

    let cmd_idp2 = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("k2".to_string(), 30)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    let r2 = s.handle_command(c, cmd_idp2, Some(b"world".to_vec()));
    assert!(matches!(r2, Response::OutOfMemory), "got {r2:?}");
}

#[test]
fn test_memory_accounting_flush_tube_drains_tombstones() {
    let mut s = make_state();
    let c = register(&mut s);

    // Put with idp+TTL, then delete → creates cooldown tombstone.
    let cmd = Command::Put {
        pri: 0,
        delay: 0,
        ttr: 60,
        bytes: 5,
        idempotency_key: Some(("k1".to_string(), 30)),
        group: None,
        after_group: None,
        concurrency_key: None,
    };
    s.handle_command(c, cmd, Some(b"hello".to_vec()));
    s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(
        s.total_job_bytes > 0,
        "tombstone should remain after delete"
    );
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));

    s.handle_command(
        c,
        Command::FlushTube {
            tube: "default".to_string(),
        },
        None,
    );
    assert_eq!(s.total_job_bytes, 0);
    assert_eq!(recompute_total_job_bytes(&s), 0);
}

#[test]
fn test_memory_accounting_churn_returns_to_zero() {
    // Mixed stream of puts, deletes, reserves, releases. After everything
    // is deleted, the counter must match a from-scratch recomputation
    // (0 in this case since all jobs are gone).
    let mut s = make_state();
    let c = register(&mut s);

    let mut live_ids: Vec<u64> = Vec::new();

    for i in 0..500u32 {
        let body = format!("job-{i}").into_bytes();
        let cmd = put_cmd(0, 0, 60, body.len() as u32);
        if let Response::Inserted(id) = s.handle_command(c, cmd, Some(body)) {
            live_ids.push(id);
        }
    }
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));

    // Reserve+release a batch — net zero.
    for _ in 0..100 {
        if let Response::Reserved { id, .. } = s.handle_command(c, Command::Reserve, None) {
            s.handle_command(
                c,
                Command::Release {
                    id,
                    pri: 0,
                    delay: 0,
                },
                None,
            );
        }
    }
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));

    // Reserve+bury+kick a batch — net zero.
    for _ in 0..50 {
        if let Response::Reserved { id, .. } = s.handle_command(c, Command::Reserve, None) {
            s.handle_command(c, Command::Bury { id, pri: 0 }, None);
        }
    }
    s.handle_command(c, Command::Kick { bound: 100 }, None);
    assert_eq!(s.total_job_bytes, recompute_total_job_bytes(&s));

    // Delete every remaining job.
    for id in live_ids {
        s.handle_command(c, Command::Delete { id }, None);
    }
    assert_eq!(s.total_job_bytes, 0);
    assert_eq!(recompute_total_job_bytes(&s), 0);
}

#[test]
fn test_drift_detector_fires_and_self_heals_on_empty_state() {
    let mut s = make_state();
    // Simulate a missed accounting decrement: counter non-zero, no jobs.
    s.total_job_bytes = 9999;
    assert_eq!(s.stats.accounting_drift_events, 0);

    s.tick();

    assert_eq!(
        s.total_job_bytes, 0,
        "drift detector should reset counter to 0"
    );
    assert_eq!(
        s.stats.accounting_drift_events, 1,
        "drift detector should bump the counter"
    );
}

#[test]
fn test_drift_detector_does_not_fire_with_live_jobs() {
    let mut s = make_state();
    let c = register(&mut s);
    s.handle_command(c, put_cmd(0, 0, 60, 5), Some(b"hello".to_vec()));
    let baseline = s.total_job_bytes;

    // Corrupt the counter upward. With a live job present, the drift
    // detector must not touch it — we can't tell if the extra bytes are
    // drift or real.
    s.total_job_bytes += 1000;
    let corrupted = s.total_job_bytes;

    s.tick();

    assert_eq!(
        s.total_job_bytes, corrupted,
        "detector must only fire on empty state"
    );
    assert_eq!(s.stats.accounting_drift_events, 0);
    // Clean up so the test doesn't leak a weird counter.
    s.total_job_bytes = baseline;
}

#[test]
fn test_parse_bytes_variants() {
    assert_eq!(parse_bytes("1024"), Ok(1024));
    assert_eq!(parse_bytes("1k"), Ok(1024));
    assert_eq!(parse_bytes("1K"), Ok(1024));
    assert_eq!(parse_bytes("2m"), Ok(2 * 1024 * 1024));
    assert_eq!(parse_bytes("2M"), Ok(2 * 1024 * 1024));
    assert_eq!(parse_bytes("3g"), Ok(3 * 1024 * 1024 * 1024));
    assert_eq!(parse_bytes("3G"), Ok(3 * 1024 * 1024 * 1024));
    assert_eq!(parse_bytes("1t"), Ok(1024u64.pow(4)));
    assert_eq!(parse_bytes("2gb"), Ok(2 * 1024 * 1024 * 1024));
    assert_eq!(parse_bytes("2GB"), Ok(2 * 1024 * 1024 * 1024));
    assert_eq!(parse_bytes("500M"), Ok(500 * 1024 * 1024));
    assert!(parse_bytes("").is_err());
    assert!(parse_bytes("nope").is_err());
    assert!(parse_bytes("1.5g").is_err()); // decimals not supported
}

#[test]
fn test_reserve_job_respects_after_group() {
    // reserve-job by ID must not bypass after_group dependencies.
    let mut s = make_state();
    let c = register(&mut s);

    // Put a predecessor job in group "g1"
    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 4,
            idempotency_key: None,
            group: Some("g1".into()),
            after_group: None,
            concurrency_key: None,
        },
        Some(b"pre1".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(1)));

    // Put a dependent job with aft:g1
    let resp = s.handle_command(
        c,
        Command::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            bytes: 4,
            idempotency_key: None,
            group: None,
            after_group: Some("g1".into()),
            concurrency_key: None,
        },
        Some(b"dep1".to_vec()),
    );
    assert!(matches!(resp, Response::Inserted(2)));

    // reserve-job 2 should fail — g1 is not complete
    let r = s.handle_command(c, Command::ReserveJob { id: 2 }, None);
    assert!(
        matches!(r, Response::NotFound),
        "reserve-job should block on incomplete after_group, got {:?}",
        r
    );

    // Complete g1: reserve and delete job 1
    let r = s.handle_command(c, Command::ReserveWithTimeout { timeout: 0 }, None);
    assert!(matches!(r, Response::Reserved { id: 1, .. }));
    let r = s.handle_command(c, Command::Delete { id: 1 }, None);
    assert!(matches!(r, Response::Deleted));

    // Now reserve-job 2 should succeed
    let r = s.handle_command(c, Command::ReserveJob { id: 2 }, None);
    assert!(
        matches!(r, Response::Reserved { id: 2, .. }),
        "reserve-job should succeed after group completes, got {:?}",
        r
    );
}

// --- waiting_ct accounting ---

#[test]
fn test_waiting_ct_reserve_blocks() {
    let mut s = make_state();
    let c = register(&mut s);
    let (tx, _rx) = oneshot::channel();
    s.add_waiter(c, tx, None);
    assert_eq!(s.stats.waiting_ct, 1);
    assert_eq!(s.tubes.get("default").unwrap().stat.waiting_ct, 1);
}

#[test]
fn test_waiting_ct_fulfilled_waiter_resets() {
    let mut s = make_state();
    let c = register(&mut s);
    let (tx, _rx) = oneshot::channel();
    s.add_waiter(c, tx, None);
    assert_eq!(s.stats.waiting_ct, 1);

    // Put a job — process_queue should wake the waiter
    s.handle_command(c, put_cmd(0, 0, 10, 1), Some(b"x".to_vec()));
    assert_eq!(s.stats.waiting_ct, 0);
    assert_eq!(s.tubes.get("default").unwrap().stat.waiting_ct, 0);
}

#[test]
fn test_waiting_ct_disconnect_clears() {
    let mut s = make_state();
    let c1 = register(&mut s);
    let c2 = register(&mut s);

    // c2 watches an extra tube
    s.handle_command(
        c2,
        Command::Watch {
            tube: "foo".into(),
            weight: 1,
        },
        None,
    );

    let (tx1, _rx1) = oneshot::channel();
    s.add_waiter(c1, tx1, None);
    let (tx2, _rx2) = oneshot::channel();
    s.add_waiter(c2, tx2, None);

    assert_eq!(s.stats.waiting_ct, 2);
    assert_eq!(s.tubes.get("default").unwrap().stat.waiting_ct, 2);
    assert_eq!(s.tubes.get("foo").unwrap().stat.waiting_ct, 1);

    // Disconnect c1 — its waiter should be removed
    s.unregister_conn(c1);
    assert_eq!(s.stats.waiting_ct, 1);
    assert_eq!(s.tubes.get("default").unwrap().stat.waiting_ct, 1);
    assert_eq!(s.tubes.get("foo").unwrap().stat.waiting_ct, 1);
}
