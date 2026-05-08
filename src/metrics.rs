use crate::client::TuberClient;
use std::collections::HashMap;
use std::io;
use std::net::IpAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// Start the Prometheus metrics HTTP server.
/// Connects to the beanstalkd port as a client to gather stats.
pub async fn serve(listen_addr: IpAddr, port: u16, beanstalk_addr: String) -> io::Result<()> {
    let listener = TcpListener::bind((listen_addr, port)).await?;
    tracing::info!("metrics endpoint on {}:{}/metrics", listen_addr, port);

    loop {
        let (socket, _) = listener.accept().await?;
        let beanstalk_addr = beanstalk_addr.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_http(socket, &beanstalk_addr).await {
                tracing::debug!("metrics request error: {e}");
            }
        });
    }
}

fn http_response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    )
}

async fn handle_http(socket: tokio::net::TcpStream, beanstalk_addr: &str) -> io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut request_line = String::new();
    buf_reader.read_line(&mut request_line).await?;

    // Drain remaining headers
    loop {
        let mut header = String::new();
        buf_reader.read_line(&mut header).await?;
        if header.trim().is_empty() {
            break;
        }
    }

    if request_line.starts_with("GET /metrics") {
        let body = match gather_metrics(beanstalk_addr).await {
            Ok(b) => b,
            Err(e) => {
                let msg = format!("error gathering metrics: {e}");
                writer
                    .write_all(
                        http_response("503 Service Unavailable", "text/plain", &msg).as_bytes(),
                    )
                    .await?;
                return Ok(());
            }
        };
        writer
            .write_all(
                http_response("200 OK", "text/plain; version=0.0.4; charset=utf-8", &body)
                    .as_bytes(),
            )
            .await?;
    } else {
        writer
            .write_all(http_response("404 Not Found", "text/plain", "404 Not Found\n").as_bytes())
            .await?;
    }

    Ok(())
}

async fn gather_metrics(beanstalk_addr: &str) -> io::Result<String> {
    let mut client = TuberClient::connect(beanstalk_addr).await?;
    let mut out = String::new();

    // Global stats
    let stats_yaml = client.stats().await?;
    let stats = parse_yaml_map(&stats_yaml);

    // Info metric with instance name and version labels
    {
        let name = stats.get("name").unwrap_or(&"");
        let version = stats.get("version").unwrap_or(&"");
        let id = stats.get("id").unwrap_or(&"");
        out.push_str("# HELP tuber_info Tuber instance information\n");
        out.push_str("# TYPE tuber_info gauge\n");
        out.push_str(&format!(
            "tuber_info{{name=\"{name}\",version={version},id=\"{id}\"}} 1\n\n"
        ));
    }

    // Gauges
    prom_gauge(
        &mut out,
        "tuber_jobs_urgent",
        "Current urgent jobs",
        &stats,
        "current-jobs-urgent",
    );
    prom_gauge(
        &mut out,
        "tuber_jobs_ready",
        "Current ready jobs",
        &stats,
        "current-jobs-ready",
    );
    prom_gauge(
        &mut out,
        "tuber_jobs_reserved",
        "Current reserved jobs",
        &stats,
        "current-jobs-reserved",
    );
    prom_gauge(
        &mut out,
        "tuber_jobs_delayed",
        "Current delayed jobs",
        &stats,
        "current-jobs-delayed",
    );
    prom_gauge(
        &mut out,
        "tuber_jobs_buried",
        "Current buried jobs",
        &stats,
        "current-jobs-buried",
    );
    prom_gauge(
        &mut out,
        "tuber_connections_current",
        "Current connections",
        &stats,
        "current-connections",
    );
    prom_gauge(
        &mut out,
        "tuber_producers_current",
        "Current producers",
        &stats,
        "current-producers",
    );
    prom_gauge(
        &mut out,
        "tuber_workers_current",
        "Current workers",
        &stats,
        "current-workers",
    );
    prom_gauge(
        &mut out,
        "tuber_waiting_current",
        "Current waiting connections",
        &stats,
        "current-waiting",
    );
    prom_gauge(
        &mut out,
        "tuber_tubes_current",
        "Current number of tubes",
        &stats,
        "current-tubes",
    );
    prom_gauge(
        &mut out,
        "tuber_uptime_seconds",
        "Server uptime in seconds",
        &stats,
        "uptime",
    );
    prom_gauge(
        &mut out,
        "tuber_maxrss_bytes",
        "Peak resident set size in bytes",
        &stats,
        "rusage-maxrss",
    );
    prom_gauge(
        &mut out,
        "tuber_binlog_file_count",
        "Number of WAL files on disk",
        &stats,
        "binlog-file-count",
    );
    prom_gauge(
        &mut out,
        "tuber_binlog_total_bytes",
        "Total bytes written across all WAL files",
        &stats,
        "binlog-total-bytes",
    );
    prom_gauge(
        &mut out,
        "tuber_jobs_size_bytes",
        "Current in-memory size of all jobs (bodies + per-job overhead + tombstones)",
        &stats,
        "current-jobs-size",
    );
    prom_gauge(
        &mut out,
        "tuber_jobs_size_limit_bytes",
        "Configured --max-jobs-size limit (0 if unlimited)",
        &stats,
        "max-jobs-size",
    );

    // TOAST (external body store) gauges. Zero when persistence is off.
    prom_gauge(
        &mut out,
        "tuber_toast_total_bytes",
        "Total bytes used across all TOAST segment files",
        &stats,
        "toast-total-bytes",
    );
    prom_gauge(
        &mut out,
        "tuber_toast_live_bytes",
        "Body bytes still referenced by live BodyIds (drives compaction)",
        &stats,
        "toast-live-bytes",
    );
    prom_gauge(
        &mut out,
        "tuber_toast_segments",
        "Number of TOAST segment files on disk",
        &stats,
        "toast-segments",
    );
    prom_gauge(
        &mut out,
        "tuber_max_storage_bytes",
        "Configured --max-storage-bytes limit (0 if unlimited)",
        &stats,
        "max-storage-bytes",
    );

    // Counters
    prom_counter(
        &mut out,
        "tuber_jobs_total",
        "Total jobs created",
        &stats,
        "total-jobs",
    );
    prom_counter(
        &mut out,
        "tuber_job_timeouts_total",
        "Total job timeouts",
        &stats,
        "job-timeouts",
    );
    prom_counter(
        &mut out,
        "tuber_connections_total",
        "Total connections",
        &stats,
        "total-connections",
    );
    prom_counter(
        &mut out,
        "tuber_accounting_drift_events_total",
        "Total times the tick-time drift detector saw a non-zero \
         current-jobs-size with an empty live set (every increment is a bug)",
        &stats,
        "accounting-drift-events",
    );
    prom_counter(
        &mut out,
        "tuber_toast_compactions_total",
        "TOAST segment compactions completed since startup",
        &stats,
        "toast-compactions-total",
    );
    prom_counter(
        &mut out,
        "tuber_toast_bodies_migrated_total",
        "Bodies physically rewritten by TOAST compaction since startup",
        &stats,
        "toast-bodies-migrated-total",
    );
    prom_counter(
        &mut out,
        "tuber_toast_bodies_dropped_corrupted_total",
        "Bodies skipped (treated as deleted) during compaction because their \
         CRC check failed. Alert on rate>0 — every increment is bit-rot.",
        &stats,
        "toast-bodies-dropped-corrupted",
    );
    prom_counter(
        &mut out,
        "tuber_recovered_missing_bodies_total",
        "Jobs reaped at startup because their WAL FullJob referenced a \
         BodyId that wasn't in the TOAST index. Alert on rate>0 — every \
         increment is a TOAST integrity event.",
        &stats,
        "recovered-missing-bodies",
    );

    // Command counters (labeled)
    let cmd_keys = [
        "cmd-put",
        "cmd-peek",
        "cmd-peek-ready",
        "cmd-peek-delayed",
        "cmd-peek-buried",
        "cmd-peek-reserved",
        "cmd-reserve",
        "cmd-reserve-with-timeout",
        "cmd-reserve-mode",
        "cmd-delete",
        "cmd-release",
        "cmd-bury",
        "cmd-kick",
        "cmd-touch",
        "cmd-use",
        "cmd-watch",
        "cmd-ignore",
        "cmd-stats",
        "cmd-stats-job",
        "cmd-stats-tube",
        "cmd-list-tubes",
        "cmd-list-tube-used",
        "cmd-list-tubes-watched",
        "cmd-pause-tube",
    ];

    out.push_str("# HELP tuber_cmd_total Total commands by type\n");
    out.push_str("# TYPE tuber_cmd_total counter\n");
    for key in &cmd_keys {
        if let Some(val) = stats.get(*key) {
            let label = &key[4..]; // strip "cmd-" prefix
            out.push_str(&format!("tuber_cmd_total{{cmd=\"{label}\"}} {val}\n"));
        }
    }
    out.push('\n');

    // Per-tube stats
    let tubes_yaml = client.list_tubes().await?;
    let tube_names = parse_yaml_list(&tubes_yaml);

    if !tube_names.is_empty() {
        out.push_str("# HELP tuber_tube_ready_jobs Ready jobs per tube\n");
        out.push_str("# TYPE tuber_tube_ready_jobs gauge\n");
        out.push_str("# HELP tuber_tube_delayed_jobs Delayed jobs per tube\n");
        out.push_str("# TYPE tuber_tube_delayed_jobs gauge\n");
        out.push_str("# HELP tuber_tube_buried_jobs Buried jobs per tube\n");
        out.push_str("# TYPE tuber_tube_buried_jobs gauge\n");
        out.push_str("# HELP tuber_tube_reserved_jobs Reserved jobs per tube\n");
        out.push_str("# TYPE tuber_tube_reserved_jobs gauge\n");
        out.push_str("# HELP tuber_tube_waiting Waiting connections per tube\n");
        out.push_str("# TYPE tuber_tube_waiting gauge\n");
        out.push_str("# HELP tuber_tube_jobs_total Total jobs per tube\n");
        out.push_str("# TYPE tuber_tube_jobs_total counter\n");
        out.push_str("# HELP tuber_tube_deletes_total Total deletes per tube\n");
        out.push_str("# TYPE tuber_tube_deletes_total counter\n");
        out.push_str("# HELP tuber_tube_bury_rate Bury rate per tube\n");
        out.push_str("# TYPE tuber_tube_bury_rate gauge\n");
        out.push_str("# HELP tuber_tube_processing_time_ewma Processing time EWMA per tube\n");
        out.push_str("# TYPE tuber_tube_processing_time_ewma gauge\n");
        out.push_str("# HELP tuber_tube_processing_time_ewma_fast Processing time EWMA for fast jobs per tube\n");
        out.push_str("# TYPE tuber_tube_processing_time_ewma_fast gauge\n");
        out.push_str("# HELP tuber_tube_processing_time_ewma_slow Processing time EWMA for slow jobs per tube\n");
        out.push_str("# TYPE tuber_tube_processing_time_ewma_slow gauge\n");
        out.push_str("# HELP tuber_tube_processing_time_p50 Processing time p50 per tube\n");
        out.push_str("# TYPE tuber_tube_processing_time_p50 gauge\n");
        out.push_str("# HELP tuber_tube_processing_time_p95 Processing time p95 per tube\n");
        out.push_str("# TYPE tuber_tube_processing_time_p95 gauge\n");
        out.push_str("# HELP tuber_tube_processing_time_p99 Processing time p99 per tube\n");
        out.push_str("# TYPE tuber_tube_processing_time_p99 gauge\n");
        out.push_str("# HELP tuber_tube_queue_time_ewma Queue time EWMA per tube\n");
        out.push_str("# TYPE tuber_tube_queue_time_ewma gauge\n");

        for name in &tube_names {
            if let Ok(tube_yaml) = client.stats_tube(name).await {
                let ts = parse_yaml_map(&tube_yaml);
                tube_metric(
                    &mut out,
                    "tuber_tube_ready_jobs",
                    name,
                    &ts,
                    "current-jobs-ready",
                );
                tube_metric(
                    &mut out,
                    "tuber_tube_delayed_jobs",
                    name,
                    &ts,
                    "current-jobs-delayed",
                );
                tube_metric(
                    &mut out,
                    "tuber_tube_buried_jobs",
                    name,
                    &ts,
                    "current-jobs-buried",
                );
                tube_metric(
                    &mut out,
                    "tuber_tube_reserved_jobs",
                    name,
                    &ts,
                    "current-jobs-reserved",
                );
                tube_metric(&mut out, "tuber_tube_waiting", name, &ts, "current-waiting");
                tube_metric(&mut out, "tuber_tube_jobs_total", name, &ts, "total-jobs");
                tube_metric(
                    &mut out,
                    "tuber_tube_deletes_total",
                    name,
                    &ts,
                    "cmd-delete",
                );
                tube_metric(&mut out, "tuber_tube_bury_rate", name, &ts, "bury-rate");
                tube_metric(&mut out, "tuber_tube_processing_time_ewma", name, &ts, "processing-time-ewma");
                tube_metric(&mut out, "tuber_tube_processing_time_ewma_fast", name, &ts, "processing-time-ewma-fast");
                tube_metric(&mut out, "tuber_tube_processing_time_ewma_slow", name, &ts, "processing-time-ewma-slow");
                tube_metric(&mut out, "tuber_tube_processing_time_p50", name, &ts, "processing-time-p50");
                tube_metric(&mut out, "tuber_tube_processing_time_p95", name, &ts, "processing-time-p95");
                tube_metric(&mut out, "tuber_tube_processing_time_p99", name, &ts, "processing-time-p99");
                tube_metric(&mut out, "tuber_tube_queue_time_ewma", name, &ts, "queue-time-ewma");
            }
        }
        out.push('\n');
    }

    Ok(out)
}

fn prom_gauge(out: &mut String, name: &str, help: &str, stats: &HashMap<&str, &str>, key: &str) {
    if let Some(val) = stats.get(key) {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {val}\n\n"
        ));
    }
}

fn prom_counter(out: &mut String, name: &str, help: &str, stats: &HashMap<&str, &str>, key: &str) {
    if let Some(val) = stats.get(key) {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {val}\n\n"
        ));
    }
}

fn tube_metric(out: &mut String, name: &str, tube: &str, stats: &HashMap<&str, &str>, key: &str) {
    if let Some(val) = stats.get(key) {
        out.push_str(&format!("{name}{{tube=\"{tube}\"}} {val}\n"));
    }
}

/// Parse simple YAML `key: value` lines into a map.
fn parse_yaml_map(yaml: &str) -> HashMap<&str, &str> {
    let mut map = HashMap::new();
    for line in yaml.lines() {
        if line.starts_with("---") {
            continue;
        }
        if let Some((key, val)) = line.split_once(": ") {
            map.insert(key.trim(), val.trim().trim_matches('"'));
        }
    }
    map
}

/// Parse simple YAML list (`- item`) lines.
fn parse_yaml_list(yaml: &str) -> Vec<String> {
    yaml.lines()
        .filter_map(|line| line.strip_prefix("- ").map(|s| s.trim().to_string()))
        .collect()
}
