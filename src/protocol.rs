use crate::job::MAX_TUBE_NAME_LEN;

/// Commands parsed from the beanstalkd text protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Put {
        pri: u32,
        delay: u32,
        ttr: u32,
        bytes: u32,
        idempotency_key: Option<(String, u32)>,
        group: Option<String>,
        after_group: Option<String>,
        concurrency_key: Option<(String, u32)>,
    },
    Use {
        tube: String,
    },
    Reserve,
    ReserveWithTimeout {
        timeout: u32,
    },
    ReserveJob {
        id: u64,
    },
    ReserveMode {
        mode: String,
    },
    Delete {
        id: u64,
    },
    Release {
        id: u64,
        pri: u32,
        delay: u32,
    },
    Bury {
        id: u64,
        pri: u32,
    },
    Touch {
        id: u64,
    },
    Watch {
        tube: String,
        weight: u32,
    },
    Ignore {
        tube: String,
    },
    Peek {
        id: u64,
    },
    PeekReady,
    PeekDelayed,
    PeekBuried,
    PeekReserved,
    Kick {
        bound: u32,
    },
    KickJob {
        id: u64,
    },
    StatsJob {
        id: u64,
    },
    StatsTube {
        tube: String,
    },
    Stats,
    ListTubes,
    ListTubeUsed,
    ListTubesWatched,
    PauseTube {
        tube: String,
        delay: u32,
    },
    FlushTube {
        tube: String,
    },
    ReserveBatch {
        count: u32,
    },
    DeleteBatch {
        ids: Vec<u64>,
    },
    StatsGroup {
        group: String,
    },
    Drain,
    Undrain,
    Quit,
}

/// Responses sent back to the client.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    Inserted(u64),
    InsertedDup(u64, &'static str, Option<u32>),
    BuriedId(u64),
    Buried,
    Using(String),
    Reserved { id: u64, body: Vec<u8> },
    DeadlineSoon,
    TimedOut,
    Deleted,
    Released,
    Touched,
    Kicked(u32),
    KickedOne,
    Found { id: u64, body: Vec<u8> },
    NotFound,
    Watching(usize),
    NotIgnored,
    ReservedBatch(Vec<(u64, Vec<u8>)>),
    DeletedBatch { deleted: u32, not_found: u32 },
    Ok(Vec<u8>),
    Paused,
    Flushed(u32),
    OutOfMemory,
    OutOfStorage,
    InternalError,
    Draining,
    NotDraining,
    BadFormat,
    UnknownCommand,
    ExpectedCrlf,
    JobTooBig,
}

impl Response {
    /// Serialize the response into a reusable buffer, avoiding per-call allocation.
    pub fn serialize_into(&self, buf: &mut Vec<u8>) {
        use std::io::Write;
        match self {
            Response::Inserted(id) => { let _ = write!(buf, "INSERTED {id}\r\n"); }
            Response::InsertedDup(id, state, None) => { let _ = write!(buf, "INSERTED {id} {state}\r\n"); }
            Response::InsertedDup(id, state, Some(pri)) => { let _ = write!(buf, "INSERTED {id} {state} {pri}\r\n"); }
            Response::BuriedId(id) => { let _ = write!(buf, "BURIED {id}\r\n"); }
            Response::Buried => buf.extend_from_slice(b"BURIED\r\n"),
            Response::Using(name) => { let _ = write!(buf, "USING {name}\r\n"); }
            Response::Reserved { id, body } => {
                let _ = write!(buf, "RESERVED {id} {}\r\n", body.len());
                buf.extend_from_slice(body);
                buf.extend_from_slice(b"\r\n");
            }
            Response::DeadlineSoon => buf.extend_from_slice(b"DEADLINE_SOON\r\n"),
            Response::TimedOut => buf.extend_from_slice(b"TIMED_OUT\r\n"),
            Response::Deleted => buf.extend_from_slice(b"DELETED\r\n"),
            Response::Released => buf.extend_from_slice(b"RELEASED\r\n"),
            Response::Touched => buf.extend_from_slice(b"TOUCHED\r\n"),
            Response::Kicked(n) => { let _ = write!(buf, "KICKED {n}\r\n"); }
            Response::KickedOne => buf.extend_from_slice(b"KICKED\r\n"),
            Response::Found { id, body } => {
                let _ = write!(buf, "FOUND {id} {}\r\n", body.len());
                buf.extend_from_slice(body);
                buf.extend_from_slice(b"\r\n");
            }
            Response::NotFound => buf.extend_from_slice(b"NOT_FOUND\r\n"),
            Response::Watching(n) => { let _ = write!(buf, "WATCHING {n}\r\n"); }
            Response::NotIgnored => buf.extend_from_slice(b"NOT_IGNORED\r\n"),
            Response::ReservedBatch(jobs) => {
                let _ = write!(buf, "RESERVED_BATCH {}\r\n", jobs.len());
                for (id, body) in jobs {
                    let _ = write!(buf, "RESERVED {id} {}\r\n", body.len());
                    buf.extend_from_slice(body);
                    buf.extend_from_slice(b"\r\n");
                }
            }
            Response::DeletedBatch { deleted, not_found } => {
                let _ = write!(buf, "DELETED_BATCH {deleted} {not_found}\r\n");
            }
            Response::Ok(data) => {
                let _ = write!(buf, "OK {}\r\n", data.len());
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            Response::Paused => buf.extend_from_slice(b"PAUSED\r\n"),
            Response::Flushed(n) => { let _ = write!(buf, "FLUSHED {n}\r\n"); }
            Response::OutOfMemory => buf.extend_from_slice(b"OUT_OF_MEMORY\r\n"),
            Response::OutOfStorage => buf.extend_from_slice(b"OUT_OF_STORAGE\r\n"),
            Response::InternalError => buf.extend_from_slice(b"INTERNAL_ERROR\r\n"),
            Response::Draining => buf.extend_from_slice(b"DRAINING\r\n"),
            Response::NotDraining => buf.extend_from_slice(b"NOT_DRAINING\r\n"),
            Response::BadFormat => buf.extend_from_slice(b"BAD_FORMAT\r\n"),
            Response::UnknownCommand => buf.extend_from_slice(b"UNKNOWN_COMMAND\r\n"),
            Response::ExpectedCrlf => buf.extend_from_slice(b"EXPECTED_CRLF\r\n"),
            Response::JobTooBig => buf.extend_from_slice(b"JOB_TOO_BIG\r\n"),
        }
    }

    /// Serialize the response to bytes for sending over TCP.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        self.serialize_into(&mut buf);
        buf
    }
}

fn is_valid_name_byte(b: u8) -> bool {
    matches!(b,
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' |
        b'-' | b'+' | b'/' | b';' | b'.' | b'$' | b'_' | b'(' | b')'
    )
}

fn is_valid_tube_name(name: &str) -> bool {
    let len = name.len();
    len > 0
        && len <= MAX_TUBE_NAME_LEN
        && name.bytes().all(is_valid_name_byte)
        && !name.starts_with('-')
}

/// Parse a single command line (without the trailing \r\n).
pub fn parse_command(line: &str) -> Result<Command, Response> {
    // The line has already been stripped of \r\n by the caller.
    let line = line.trim_end_matches('\0');

    // Check for embedded NULs (malicious input)
    if line.bytes().any(|b| b == 0) {
        return Err(Response::BadFormat);
    }

    if let Some(rest) = line.strip_prefix("put ") {
        parse_put(rest)
    } else if let Some(rest) = line.strip_prefix("peek ") {
        parse_peek(rest)
    } else if line == "peek-ready" {
        Ok(Command::PeekReady)
    } else if line == "peek-delayed" {
        Ok(Command::PeekDelayed)
    } else if line == "peek-buried" {
        Ok(Command::PeekBuried)
    } else if line == "peek-reserved" {
        Ok(Command::PeekReserved)
    } else if let Some(rest) = line.strip_prefix("reserve-batch ") {
        parse_reserve_batch(rest)
    } else if let Some(rest) = line.strip_prefix("reserve-mode ") {
        Ok(Command::ReserveMode {
            mode: rest.to_string(),
        })
    } else if let Some(rest) = line.strip_prefix("reserve-with-timeout ") {
        parse_reserve_with_timeout(rest)
    } else if let Some(rest) = line.strip_prefix("reserve-job ") {
        parse_reserve_job(rest)
    } else if line == "reserve" {
        Ok(Command::Reserve)
    } else if let Some(rest) = line.strip_prefix("delete-batch ") {
        parse_delete_batch(rest)
    } else if let Some(rest) = line.strip_prefix("delete ") {
        parse_uint(rest).map(|id| Command::Delete { id })
    } else if let Some(rest) = line.strip_prefix("release ") {
        parse_release(rest)
    } else if let Some(rest) = line.strip_prefix("bury ") {
        parse_bury(rest)
    } else if let Some(rest) = line.strip_prefix("kick-job ") {
        parse_uint(rest).map(|id| Command::KickJob { id })
    } else if let Some(rest) = line.strip_prefix("kick ") {
        parse_uint(rest).map(|bound| Command::Kick { bound })
    } else if let Some(rest) = line.strip_prefix("touch ") {
        parse_uint(rest).map(|id| Command::Touch { id })
    } else if let Some(rest) = line.strip_prefix("stats-job ") {
        parse_uint(rest).map(|id| Command::StatsJob { id })
    } else if let Some(rest) = line.strip_prefix("stats-tube ") {
        parse_stats_tube(rest)
    } else if let Some(rest) = line.strip_prefix("stats-group ") {
        parse_stats_group(rest)
    } else if line == "stats" {
        Ok(Command::Stats)
    } else if let Some(rest) = line.strip_prefix("use ") {
        parse_use(rest)
    } else if let Some(rest) = line.strip_prefix("watch ") {
        parse_watch(rest)
    } else if let Some(rest) = line.strip_prefix("ignore ") {
        parse_ignore(rest)
    } else if line == "list-tubes-watched" {
        Ok(Command::ListTubesWatched)
    } else if line == "list-tube-used" {
        Ok(Command::ListTubeUsed)
    } else if line == "list-tubes" {
        Ok(Command::ListTubes)
    } else if line == "quit" {
        Ok(Command::Quit)
    } else if line.starts_with("pause-tube") {
        parse_pause_tube(line.strip_prefix("pause-tube").unwrap())
    } else if let Some(rest) = line.strip_prefix("flush-tube ") {
        parse_flush_tube(rest)
    } else if line == "drain" {
        Ok(Command::Drain)
    } else if line == "undrain" {
        Ok(Command::Undrain)
    } else {
        Err(Response::UnknownCommand)
    }
}

fn parse_uint<T: std::str::FromStr>(s: &str) -> Result<T, Response> {
    let s = s.trim();
    if s.is_empty() || s.starts_with('-') {
        return Err(Response::BadFormat);
    }
    s.parse().map_err(|_| Response::BadFormat)
}

fn is_valid_key(s: &str) -> bool {
    is_valid_tube_name(s)
}

fn parse_put(rest: &str) -> Result<Command, Response> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() < 4 {
        return Err(Response::BadFormat);
    }
    let pri = parts[0].parse::<u32>().map_err(|_| Response::BadFormat)?;
    let delay = parts[1].parse::<u32>().map_err(|_| Response::BadFormat)?;
    let ttr = parts[2].parse::<u32>().map_err(|_| Response::BadFormat)?;
    let bytes = parts[3].parse::<u32>().map_err(|_| Response::BadFormat)?;

    let mut idempotency_key = None;
    let mut group = None;
    let mut after_group = None;
    let mut concurrency_key = None;

    for part in &parts[4..] {
        if let Some(val) = part.strip_prefix("idp:") {
            // Parse idp:key or idp:key:N
            if let Some(colon_pos) = val.rfind(':') {
                let key_part = &val[..colon_pos];
                let ttl_part = &val[colon_pos + 1..];
                if !is_valid_key(key_part) {
                    return Err(Response::BadFormat);
                }
                let ttl: u32 = ttl_part.parse().map_err(|_| Response::BadFormat)?;
                idempotency_key = Some((key_part.to_string(), ttl));
            } else {
                if !is_valid_key(val) {
                    return Err(Response::BadFormat);
                }
                idempotency_key = Some((val.to_string(), 0));
            }
        } else if let Some(val) = part.strip_prefix("grp:") {
            if !is_valid_key(val) {
                return Err(Response::BadFormat);
            }
            group = Some(val.to_string());
        } else if let Some(val) = part.strip_prefix("aft:") {
            if !is_valid_key(val) {
                return Err(Response::BadFormat);
            }
            after_group = Some(val.to_string());
        } else if let Some(val) = part.strip_prefix("con:") {
            // Parse con:key or con:key:N
            if let Some(colon_pos) = val.rfind(':') {
                let key_part = &val[..colon_pos];
                let limit_part = &val[colon_pos + 1..];
                if !is_valid_key(key_part) {
                    return Err(Response::BadFormat);
                }
                let limit: u32 = limit_part.parse().map_err(|_| Response::BadFormat)?;
                if limit == 0 {
                    return Err(Response::BadFormat);
                }
                concurrency_key = Some((key_part.to_string(), limit));
            } else {
                if !is_valid_key(val) {
                    return Err(Response::BadFormat);
                }
                concurrency_key = Some((val.to_string(), 1));
            }
        } else {
            return Err(Response::BadFormat);
        }
    }

    Ok(Command::Put {
        pri,
        delay,
        ttr,
        bytes,
        idempotency_key,
        group,
        after_group,
        concurrency_key,
    })
}

fn parse_peek(rest: &str) -> Result<Command, Response> {
    parse_uint(rest).map(|id| Command::Peek { id })
}

const MAX_RESERVE_BATCH: u32 = 1000;

fn parse_reserve_batch(rest: &str) -> Result<Command, Response> {
    let count: u32 = parse_uint(rest)?;
    if count == 0 || count > MAX_RESERVE_BATCH {
        return Err(Response::BadFormat);
    }
    Ok(Command::ReserveBatch { count })
}

pub const MAX_DELETE_BATCH: usize = 1000;

fn parse_delete_batch(rest: &str) -> Result<Command, Response> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.is_empty() || parts.len() > MAX_DELETE_BATCH {
        return Err(Response::BadFormat);
    }
    let mut ids = Vec::with_capacity(parts.len());
    for part in parts {
        let id: u64 = part.parse().map_err(|_| Response::BadFormat)?;
        ids.push(id);
    }
    Ok(Command::DeleteBatch { ids })
}

fn parse_reserve_with_timeout(rest: &str) -> Result<Command, Response> {
    parse_uint(rest).map(|timeout| Command::ReserveWithTimeout { timeout })
}

fn parse_reserve_job(rest: &str) -> Result<Command, Response> {
    parse_uint(rest).map(|id| Command::ReserveJob { id })
}

fn parse_release(rest: &str) -> Result<Command, Response> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() != 3 {
        return Err(Response::BadFormat);
    }
    let id = parts[0].parse::<u64>().map_err(|_| Response::BadFormat)?;
    let pri = parts[1].parse::<u32>().map_err(|_| Response::BadFormat)?;
    let delay = parts[2].parse::<u32>().map_err(|_| Response::BadFormat)?;
    Ok(Command::Release { id, pri, delay })
}

fn parse_bury(rest: &str) -> Result<Command, Response> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(Response::BadFormat);
    }
    let id = parts[0].parse::<u64>().map_err(|_| Response::BadFormat)?;
    let pri = parts[1].parse::<u32>().map_err(|_| Response::BadFormat)?;
    Ok(Command::Bury { id, pri })
}

fn parse_use(rest: &str) -> Result<Command, Response> {
    let name = rest;
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    Ok(Command::Use {
        tube: name.to_string(),
    })
}

fn parse_watch(rest: &str) -> Result<Command, Response> {
    // Reject leading/trailing whitespace to avoid ambiguity with the weight delimiter
    if rest != rest.trim() {
        return Err(Response::BadFormat);
    }
    let parts: Vec<&str> = rest.split(' ').collect();
    if parts.is_empty() || parts.len() > 2 {
        return Err(Response::BadFormat);
    }
    let name = parts[0];
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    let weight = if parts.len() == 2 {
        let w = parts[1].parse::<u32>().map_err(|_| Response::BadFormat)?;
        if w == 0 || w > crate::conn::MAX_TUBE_WEIGHT {
            return Err(Response::BadFormat);
        }
        w
    } else {
        1
    };
    Ok(Command::Watch {
        tube: name.to_string(),
        weight,
    })
}

fn parse_ignore(rest: &str) -> Result<Command, Response> {
    let name = rest;
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    Ok(Command::Ignore {
        tube: name.to_string(),
    })
}

fn parse_stats_tube(rest: &str) -> Result<Command, Response> {
    let name = rest;
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    Ok(Command::StatsTube {
        tube: name.to_string(),
    })
}

fn parse_stats_group(rest: &str) -> Result<Command, Response> {
    let name = rest;
    if !is_valid_key(name) {
        return Err(Response::BadFormat);
    }
    Ok(Command::StatsGroup {
        group: name.to_string(),
    })
}

fn parse_flush_tube(rest: &str) -> Result<Command, Response> {
    let name = rest;
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    Ok(Command::FlushTube {
        tube: name.to_string(),
    })
}

fn parse_pause_tube(rest: &str) -> Result<Command, Response> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(Response::BadFormat);
    }
    let name = parts[0];
    if !is_valid_tube_name(name) {
        return Err(Response::BadFormat);
    }
    let delay = parts[1].parse::<u32>().map_err(|_| Response::BadFormat)?;
    Ok(Command::PauseTube {
        tube: name.to_string(),
        delay,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_parse_put() {
        assert_eq!(parse_command("put 0 0 10 5").unwrap(), put_cmd(0, 0, 10, 5));
    }

    #[test]
    fn test_parse_put_with_idempotency_key() {
        assert_eq!(
            parse_command("put 0 0 10 5 idp:abc123").unwrap(),
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: Some(("abc123".into(), 0)),
                group: None,
                after_group: None,
                concurrency_key: None,
            }
        );
    }

    #[test]
    fn test_parse_put_with_all_extensions() {
        assert_eq!(
            parse_command("put 0 0 10 5 idp:k1 grp:g1 aft:g0 con:c1").unwrap(),
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: Some(("k1".into(), 0)),
                group: Some("g1".into()),
                after_group: Some("g0".into()),
                concurrency_key: Some(("c1".into(), 1)),
            }
        );
    }

    #[test]
    fn test_parse_put_extensions_order_independent() {
        assert_eq!(
            parse_command("put 0 0 10 5 con:c1 idp:k1").unwrap(),
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: Some(("k1".into(), 0)),
                group: None,
                after_group: None,
                concurrency_key: Some(("c1".into(), 1)),
            }
        );
    }

    #[test]
    fn test_parse_put_unknown_tag_rejected() {
        assert!(parse_command("put 0 0 10 5 foo:bar").is_err());
    }

    #[test]
    fn test_parse_put_invalid_key_rejected() {
        // Leading dash is invalid
        assert!(parse_command("put 0 0 10 5 idp:-bad").is_err());
        // Empty value
        assert!(parse_command("put 0 0 10 5 idp:").is_err());
        // Space in value (would be split into separate parts, unknown tag)
        assert!(parse_command("put 0 0 10 5 idp:has space").is_err());
    }

    #[test]
    fn test_parse_reserve() {
        assert_eq!(parse_command("reserve").unwrap(), Command::Reserve);
    }

    #[test]
    fn test_parse_reserve_with_timeout() {
        assert_eq!(
            parse_command("reserve-with-timeout 30").unwrap(),
            Command::ReserveWithTimeout { timeout: 30 }
        );
    }

    #[test]
    fn test_parse_reserve_job() {
        assert_eq!(
            parse_command("reserve-job 123").unwrap(),
            Command::ReserveJob { id: 123 }
        );
    }

    #[test]
    fn test_parse_delete() {
        assert_eq!(
            parse_command("delete 123").unwrap(),
            Command::Delete { id: 123 }
        );
    }

    #[test]
    fn test_parse_release() {
        assert_eq!(
            parse_command("release 123 0 0").unwrap(),
            Command::Release {
                id: 123,
                pri: 0,
                delay: 0
            }
        );
    }

    #[test]
    fn test_parse_bury() {
        assert_eq!(
            parse_command("bury 123 0").unwrap(),
            Command::Bury { id: 123, pri: 0 }
        );
    }

    #[test]
    fn test_parse_touch() {
        assert_eq!(
            parse_command("touch 123").unwrap(),
            Command::Touch { id: 123 }
        );
    }

    #[test]
    fn test_parse_watch() {
        assert_eq!(
            parse_command("watch foo").unwrap(),
            Command::Watch {
                tube: "foo".into(),
                weight: 1
            }
        );
    }

    #[test]
    fn test_parse_watch_with_weight() {
        assert_eq!(
            parse_command("watch foo 5").unwrap(),
            Command::Watch {
                tube: "foo".into(),
                weight: 5
            }
        );
    }

    #[test]
    fn test_parse_watch_weight_zero_rejected() {
        assert!(parse_command("watch foo 0").is_err());
    }

    #[test]
    fn test_parse_watch_weight_too_large() {
        assert!(parse_command("watch foo 10000").is_err());
    }

    #[test]
    fn test_parse_ignore() {
        assert_eq!(
            parse_command("ignore foo").unwrap(),
            Command::Ignore { tube: "foo".into() }
        );
    }

    #[test]
    fn test_parse_use() {
        assert_eq!(
            parse_command("use foo").unwrap(),
            Command::Use { tube: "foo".into() }
        );
    }

    #[test]
    fn test_parse_kick() {
        assert_eq!(
            parse_command("kick 10").unwrap(),
            Command::Kick { bound: 10 }
        );
    }

    #[test]
    fn test_parse_kick_job() {
        assert_eq!(
            parse_command("kick-job 123").unwrap(),
            Command::KickJob { id: 123 }
        );
    }

    #[test]
    fn test_parse_stats() {
        assert_eq!(parse_command("stats").unwrap(), Command::Stats);
    }

    #[test]
    fn test_parse_stats_job() {
        assert_eq!(
            parse_command("stats-job 123").unwrap(),
            Command::StatsJob { id: 123 }
        );
    }

    #[test]
    fn test_parse_stats_tube() {
        assert_eq!(
            parse_command("stats-tube foo").unwrap(),
            Command::StatsTube { tube: "foo".into() }
        );
    }

    #[test]
    fn test_parse_list_tubes() {
        assert_eq!(parse_command("list-tubes").unwrap(), Command::ListTubes);
    }

    #[test]
    fn test_parse_list_tube_used() {
        assert_eq!(
            parse_command("list-tube-used").unwrap(),
            Command::ListTubeUsed
        );
    }

    #[test]
    fn test_parse_list_tubes_watched() {
        assert_eq!(
            parse_command("list-tubes-watched").unwrap(),
            Command::ListTubesWatched
        );
    }

    #[test]
    fn test_parse_pause_tube() {
        assert_eq!(
            parse_command("pause-tube foo 60").unwrap(),
            Command::PauseTube {
                tube: "foo".into(),
                delay: 60
            }
        );
    }

    #[test]
    fn test_parse_reserve_mode() {
        assert_eq!(
            parse_command("reserve-mode weighted").unwrap(),
            Command::ReserveMode {
                mode: "weighted".into()
            }
        );
    }

    #[test]
    fn test_parse_flush_tube() {
        assert_eq!(
            parse_command("flush-tube foo").unwrap(),
            Command::FlushTube { tube: "foo".into() }
        );
    }

    #[test]
    fn test_parse_flush_tube_bad_name() {
        assert!(parse_command("flush-tube -bad").is_err());
        assert!(parse_command("flush-tube").is_err());
    }

    #[test]
    fn test_parse_quit() {
        assert_eq!(parse_command("quit").unwrap(), Command::Quit);
    }

    #[test]
    fn test_parse_unknown() {
        assert!(matches!(
            parse_command("foobar").unwrap_err(),
            Response::UnknownCommand
        ));
    }

    #[test]
    fn test_parse_bad_format() {
        assert!(parse_command("put abc").is_err());
        assert!(parse_command("delete").is_err());
        assert!(parse_command("release 1 2").is_err());
    }

    #[test]
    fn test_tube_name_validation() {
        assert!(is_valid_tube_name("default"));
        assert!(is_valid_tube_name("my_tube.v2"));
        assert!(is_valid_tube_name("a"));
        assert!(!is_valid_tube_name(""));
        assert!(!is_valid_tube_name("-bad"));
        assert!(!is_valid_tube_name("has space"));
        assert!(!is_valid_tube_name("has\ttab"));
    }

    #[test]
    fn test_tube_name_with_trailing_space_rejected() {
        // Trailing spaces should not be silently trimmed — reject like beanstalkd does
        assert_eq!(parse_command("use ; "), Err(Response::BadFormat));
        assert_eq!(parse_command("use foo "), Err(Response::BadFormat));
        assert_eq!(parse_command("use  foo"), Err(Response::BadFormat));
        assert_eq!(parse_command("watch ; "), Err(Response::BadFormat));
        assert_eq!(parse_command("watch foo "), Err(Response::BadFormat));
        assert_eq!(parse_command("ignore foo "), Err(Response::BadFormat));
    }

    #[test]
    fn test_tube_name_max_length() {
        let name_200 = "a".repeat(200);
        assert!(is_valid_tube_name(&name_200));

        let name_201 = "a".repeat(201);
        assert!(!is_valid_tube_name(&name_201));
    }

    #[test]
    fn test_parse_put_con_key_with_limit() {
        assert_eq!(
            parse_command("put 0 0 10 5 con:api:5").unwrap(),
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: None,
                group: None,
                after_group: None,
                concurrency_key: Some(("api".into(), 5)),
            }
        );
    }

    #[test]
    fn test_parse_put_con_key_default_limit() {
        assert_eq!(
            parse_command("put 0 0 10 5 con:mykey").unwrap(),
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: None,
                group: None,
                after_group: None,
                concurrency_key: Some(("mykey".into(), 1)),
            }
        );
    }

    #[test]
    fn test_parse_put_con_key_limit_zero_rejected() {
        assert!(parse_command("put 0 0 10 5 con:api:0").is_err());
    }

    #[test]
    fn test_parse_put_con_key_limit_non_numeric_rejected() {
        assert!(parse_command("put 0 0 10 5 con:api:abc").is_err());
    }

    #[test]
    fn test_parse_put_con_key_limit_one() {
        assert_eq!(
            parse_command("put 0 0 10 5 con:api:1").unwrap(),
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: None,
                group: None,
                after_group: None,
                concurrency_key: Some(("api".into(), 1)),
            }
        );
    }

    #[test]
    fn test_parse_put_idp_with_ttl() {
        assert_eq!(
            parse_command("put 0 0 10 5 idp:key:60").unwrap(),
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: Some(("key".into(), 60)),
                group: None,
                after_group: None,
                concurrency_key: None,
            }
        );
    }

    #[test]
    fn test_parse_put_idp_with_zero_ttl() {
        assert_eq!(
            parse_command("put 0 0 10 5 idp:key:0").unwrap(),
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 10,
                bytes: 5,
                idempotency_key: Some(("key".into(), 0)),
                group: None,
                after_group: None,
                concurrency_key: None,
            }
        );
    }

    #[test]
    fn test_parse_put_idp_ttl_non_numeric_rejected() {
        assert!(parse_command("put 0 0 10 5 idp:key:abc").is_err());
    }


    #[test]
    fn test_parse_peek_reserved() {
        assert_eq!(
            parse_command("peek-reserved").unwrap(),
            Command::PeekReserved
        );
    }

    #[test]
    fn test_response_serialize() {
        assert_eq!(Response::Deleted.serialize(), b"DELETED\r\n");
        assert_eq!(Response::Inserted(42).serialize(), b"INSERTED 42\r\n");
        assert_eq!(Response::NotFound.serialize(), b"NOT_FOUND\r\n");
    }

    #[test]
    fn test_response_inserted_dup_serialize() {
        assert_eq!(
            Response::InsertedDup(42, "READY", None).serialize(),
            b"INSERTED 42 READY\r\n"
        );
        assert_eq!(
            Response::InsertedDup(7, "BURIED", None).serialize(),
            b"INSERTED 7 BURIED\r\n"
        );
        assert_eq!(
            Response::InsertedDup(1, "DELETED", None).serialize(),
            b"INSERTED 1 DELETED\r\n"
        );
    }

    #[test]
    fn test_parse_stats_group() {
        assert_eq!(
            parse_command("stats-group mygroup").unwrap(),
            Command::StatsGroup {
                group: "mygroup".into()
            }
        );
    }

    #[test]
    fn test_parse_stats_group_bad_name() {
        assert!(parse_command("stats-group -bad").is_err());
        assert!(parse_command("stats-group ").is_err());
    }

    #[test]
    fn test_parse_reserve_batch() {
        assert_eq!(
            parse_command("reserve-batch 5").unwrap(),
            Command::ReserveBatch { count: 5 }
        );
        assert_eq!(
            parse_command("reserve-batch 1").unwrap(),
            Command::ReserveBatch { count: 1 }
        );
        assert_eq!(
            parse_command("reserve-batch 1000").unwrap(),
            Command::ReserveBatch { count: 1000 }
        );
    }

    #[test]
    fn test_parse_reserve_batch_bad_format() {
        assert!(parse_command("reserve-batch 0").is_err());
        assert!(parse_command("reserve-batch 1001").is_err());
        assert!(parse_command("reserve-batch").is_err());
        assert!(parse_command("reserve-batch abc").is_err());
        assert!(parse_command("reserve-batch -1").is_err());
    }

    #[test]
    fn test_response_reserved_batch_serialize() {
        let resp = Response::ReservedBatch(vec![(1, b"hello".to_vec()), (2, b"world".to_vec())]);
        let out = resp.serialize();
        let expected = b"RESERVED_BATCH 2\r\nRESERVED 1 5\r\nhello\r\nRESERVED 2 5\r\nworld\r\n";
        assert_eq!(out, expected);
    }

    #[test]
    fn test_response_reserved_batch_empty_serialize() {
        let resp = Response::ReservedBatch(vec![]);
        assert_eq!(resp.serialize(), b"RESERVED_BATCH 0\r\n");
    }

    #[test]
    fn test_parse_delete_batch() {
        assert_eq!(
            parse_command("delete-batch 1 2 3").unwrap(),
            Command::DeleteBatch { ids: vec![1, 2, 3] }
        );
        assert_eq!(
            parse_command("delete-batch 42").unwrap(),
            Command::DeleteBatch { ids: vec![42] }
        );
    }

    #[test]
    fn test_parse_delete_batch_bad_format() {
        // No IDs
        assert!(parse_command("delete-batch").is_err());
        // Non-numeric
        assert!(parse_command("delete-batch abc").is_err());
        // Negative
        assert!(parse_command("delete-batch -1").is_err());
    }

    #[test]
    fn test_parse_delete_batch_max_limit() {
        let ids: Vec<String> = (1..=1000).map(|i| i.to_string()).collect();
        let cmd = format!("delete-batch {}", ids.join(" "));
        assert!(parse_command(&cmd).is_ok());

        let ids: Vec<String> = (1..=1001).map(|i| i.to_string()).collect();
        let cmd = format!("delete-batch {}", ids.join(" "));
        assert!(parse_command(&cmd).is_err());
    }

    #[test]
    fn test_response_deleted_batch_serialize() {
        let resp = Response::DeletedBatch {
            deleted: 3,
            not_found: 1,
        };
        assert_eq!(resp.serialize(), b"DELETED_BATCH 3 1\r\n");
    }
}
