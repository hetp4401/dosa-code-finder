//! The work plane: everything not in the Raft log.
//!
//! The leader hands out chunks of the scan's shuffled codes (Assign, ephemeral); workers try them and report
//! Done (broadcast; the leader batches those into ChunksDone log entries). Progress telemetry (Prog) is broadcast
//! for the page. The Raft log in raft.rs is the only shared state.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::raft::{self, Command, Msg, Raft, StateMachine};

/// Codes per chunk.
pub const CHUNK: u32 = 250;
/// Chunks a worker holds at once.
const PER_WORKER: usize = 2;
/// A chunk not done this long after hand-out goes elsewhere.
const CHUNK_TIMEOUT: Duration = Duration::from_secs(90);
/// A slow worker rests this long (the site may be throttling it).
const REST: Duration = Duration::from_secs(180);
/// The leader batches Done reports into the log this often.
const BATCH_EVERY: Duration = Duration::from_secs(1);

pub fn now_unix() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

pub fn random_u64() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut h = RandomState::new().build_hasher();
    h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0));
    h.finish()
}

/// The range from..to in a random order, the same on every replica for the same seed.
pub fn permutation(from: u32, to: u32, seed: u64) -> Vec<u32> {
    let mut v: Vec<u32> = (from..to).collect();
    let mut s = seed;
    let mut next = || {
        s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    for i in (1..v.len()).rev() {
        let j = (next() % (i as u64 + 1)) as usize;
        v.swap(i, j);
    }
    v
}

/// "2026-10-05 20:30:00 UTC" for a unix time (civil-from-days, no date crate needed).
pub fn utc_text(secs: u64) -> String {
    let days = (secs / 86400) as i64;
    let rem = secs % 86400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02} UTC", rem / 3600, (rem % 3600) / 60, rem % 60)
}

/// A replica's answers and errors (since its scan began), how many codes wait for another try, and its pace.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Stats {
    pub ok: u64,
    pub e429: u64,
    pub e4: u64,
    pub e5: u64,
    pub net: u64,
    pub timeouts: u64,
    pub retrying: u64,
    pub pace: f64,
}

pub struct PeerProg {
    pub scan: String,
    pub tried: u64,
    pub rate: f64,
    pub queue: usize,
    pub last_error: String,
    pub stats: Stats,
    pub at: Instant,
}

pub struct Peer {
    #[allow(dead_code)]
    pub boot: String,
    pub tx: mpsc::UnboundedSender<String>,
    pub serial: u64,
}

/// This replica's share of the work: codes queued with their scan and chunk.
pub struct Worker {
    pub queue: Mutex<VecDeque<(Arc<str>, usize, String)>>,
    /// Codes left of each chunk queued here.
    pub left: Mutex<HashMap<usize, usize>>,
    /// The scan it works for; codes of any other are dropped.
    pub scan: Mutex<Arc<str>>,
    /// Working: false once the scan is found, stopped or done.
    pub active: AtomicBool,
    pub c: crate::Counters,
}

/// The leader's ephemeral books (rebuilt from the log's done-set on every leadership change).
struct AssignState {
    scan_id: String,
    perm: Vec<u32>,
    assigned: HashMap<usize, (u32, Instant)>,
    resting: HashMap<u32, Instant>,
    /// Chunks reported done but not yet committed (so they aren't handed out twice meanwhile).
    done_pending: HashSet<usize>,
    /// Done reports waiting to be batched into the log.
    batch: Vec<u32>,
    batch_scan: String,
    /// Workers allowed to hold chunks right now. Starts at 5 and grows in
    /// batches of 5 as they saturate, so a scan ramps up instead of bursting
    /// all replicas at checktodine.com at once.
    activated: HashSet<u32>,
}

/// How many workers get chunks at scan start; more join in batches of this size.
const RAMP_BATCH: usize = 5;

pub struct Node {
    pub me: u32,
    pub boot: String,
    pub n: u32,
    pub client: reqwest::Client,
    pub pacer: Arc<crate::Pacer>,
    pub peers: Mutex<HashMap<u32, Peer>>,
    pub progs: Mutex<HashMap<u32, PeerProg>>,
    pub raft: Mutex<Raft>,
    pub sm: Mutex<StateMachine>,
    pub worker: Worker,
    assign: Mutex<AssignState>,
    pub fwd: Mutex<HashMap<u64, tokio::sync::oneshot::Sender<raft::FwdResult>>>,
    pub req_id: AtomicU64,
    serial: AtomicU64,
}

impl Node {
    pub fn new(me: u32, n: u32, client: reqwest::Client, pacer: Arc<crate::Pacer>) -> Arc<Node> {
        Arc::new(Node {
            me,
            boot: format!("{:012x}", random_u64() & 0xffff_ffff_ffff),
            n,
            client,
            pacer,
            peers: Mutex::new(HashMap::new()),
            progs: Mutex::new(HashMap::new()),
            raft: Mutex::new(Raft::new()),
            sm: Mutex::new(StateMachine::new()),
            worker: Worker {
                queue: Mutex::new(VecDeque::new()),
                left: Mutex::new(HashMap::new()),
                scan: Mutex::new(Arc::from("")),
                active: AtomicBool::new(false),
                c: crate::Counters::default(),
            },
            assign: Mutex::new(AssignState {
                scan_id: String::new(), perm: Vec::new(), assigned: HashMap::new(),
                resting: HashMap::new(), done_pending: HashSet::new(), batch: Vec::new(), batch_scan: String::new(),
                activated: HashSet::new(),
            }),
            fwd: Mutex::new(HashMap::new()),
            req_id: AtomicU64::new(0),
            serial: AtomicU64::new(0),
        })
    }

    pub fn quorum(&self) -> usize {
        Raft::quorum(self.n)
    }

    pub fn is_leader(&self) -> bool {
        self.raft.lock().unwrap().role == raft::Role::Leader
    }

    // ---- mesh bookkeeping ----

    pub fn hello(&self) -> Msg {
        let r = self.raft.lock().unwrap();
        let sm = self.sm.lock().unwrap();
        Msg::Hello {
            id: self.me, boot: self.boot.clone(), term: r.term, commit: r.commit,
            scan: sm.scan.as_ref().map(|s| s.id.clone()),
        }
    }

    pub fn register(&self, id: u32, boot: String, tx: mpsc::UnboundedSender<String>) -> u64 {
        let serial = self.serial.fetch_add(1, Ordering::SeqCst) + 1;
        let old = self.peers.lock().unwrap().insert(id, Peer { boot: boot.clone(), tx, serial });
        if old.is_none() {
            println!("mesh: replica {id} connected ({} of {} others)", self.peers.lock().unwrap().len(), self.n.saturating_sub(1));
        }
        serial
    }

    pub fn unregister(&self, id: u32, serial: u64) {
        let mut peers = self.peers.lock().unwrap();
        if peers.get(&id).map(|p| p.serial) == Some(serial) {
            peers.remove(&id);
            println!("mesh: replica {id} disconnected ({} connected)", peers.len());
        }
    }

    pub fn live(&self) -> Vec<u32> {
        let mut v: Vec<u32> = self.peers.lock().unwrap().keys().copied().collect();
        v.push(self.me);
        v.sort();
        v
    }

    // ---- worker side ----

    /// This replica works for scan id from now on (a new scan: queue and counters start afresh).
    pub fn join_work(&self, id: &str) {
        {
            let mut scan = self.worker.scan.lock().unwrap();
            if &**scan != id {
                *scan = Arc::from(id);
                self.worker.queue.lock().unwrap().clear();
                self.worker.left.lock().unwrap().clear();
                self.worker.c.reset();
            }
        } // release scan lock before broadcast_prog (it locks scan again)
        self.worker.active.store(true, Ordering::SeqCst);
        // Tell everyone right away (don't wait for the 2s telemetry tick).
        self.broadcast_prog();
    }

    pub fn stop_work(&self) {
        self.worker.active.store(false, Ordering::SeqCst);
        self.worker.queue.lock().unwrap().clear();
        self.worker.left.lock().unwrap().clear();
    }

    pub fn take_codes(&self, id: &str, chunk: usize, codes: Vec<String>) {
        if &**self.worker.scan.lock().unwrap() != id || !self.worker.active.load(Ordering::SeqCst) {
            return;
        }
        let mut left = self.worker.left.lock().unwrap();
        if left.contains_key(&chunk) || codes.is_empty() {
            return;
        }
        left.insert(chunk, codes.len());
        let sid: Arc<str> = Arc::from(id);
        self.worker.queue.lock().unwrap().extend(codes.into_iter().map(|c| (sid.clone(), chunk, c)));
    }

    /// A code of chunk k was tried here: the chunk is done once its last one is.
    pub fn tried(&self, id: &Arc<str>, chunk: usize) {
        if &*self.worker.scan.lock().unwrap() != id {
            return;
        }
        let finished = {
            let mut left = self.worker.left.lock().unwrap();
            match left.get_mut(&chunk) {
                Some(n) => {
                    *n -= 1;
                    if *n == 0 {
                        left.remove(&chunk);
                        true
                    } else {
                        false
                    }
                }
                None => false,
            }
        };
        if finished && self.worker.active.load(Ordering::SeqCst) {
            self.worker.queue.lock().unwrap().retain(|(_, k, _)| *k != chunk);
            self.broadcast(&Msg::Done { id: id.to_string(), chunk: chunk as u32, by: self.me });
        }
    }

    /// A Done report arrived (broadcast). Only the leader acts: it batches them into the log.
    pub fn note_done_msg(&self, id: &str, chunk: usize, by: u32) {
        if !self.is_leader() {
            return;
        }
        let _ = by;
        let running_id = {
            let sm = self.sm.lock().unwrap();
            match &sm.scan {
                Some(s) if s.state == "running" => s.id.clone(),
                _ => return,
            }
        };
        if id != running_id {
            return;
        }
        let mut a = self.assign.lock().unwrap();
        if a.scan_id != running_id {
            return;
        }
        {
            let sm = self.sm.lock().unwrap();
            if sm.scan.as_ref().map(|s| s.done.get(chunk).copied().unwrap_or(false)).unwrap_or(false) {
                return;
            }
        }
        if !a.done_pending.insert(chunk) {
            return;
        }
        a.assigned.remove(&chunk);
        if a.batch_scan != running_id {
            a.batch.clear();
            a.batch_scan = running_id.clone();
        }
        a.batch.push(chunk as u32);
    }

    /// The leader appends its batched Dones as one ChunksDone entry, every second.
    pub async fn batch_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(BATCH_EVERY);
        loop {
            tick.tick().await;
            let batch = {
                if !self.is_leader() {
                    continue;
                }
                let mut a = self.assign.lock().unwrap();
                if a.batch.is_empty() {
                    continue;
                }
                let id = a.batch_scan.clone();
                let chunks = std::mem::take(&mut a.batch);
                (id, chunks)
            };
            let cmd = Command::ChunksDone { id: batch.0, chunks: batch.1 };
            let appended = {
                let mut r = self.raft.lock().unwrap();
                if r.role != raft::Role::Leader {
                    // Lost it between the check and the lock; put the batch back.
                    let mut a = self.assign.lock().unwrap();
                    if let Command::ChunksDone { id, chunks } = cmd {
                        if a.batch_scan == id {
                            a.batch.extend(chunks);
                        }
                    }
                    continue;
                }
                let index = r.last_index() + 1;
                let term = r.term;
                r.log.push(raft::Entry { term, index, cmd });
                true
            };
            if appended {
                self.replicate_all();
            }
        }
    }

    // ---- leading: handing out chunks ----

    /// The leader's loop: give every live worker chunks of the running scan, take back what's slow.
    pub async fn assign_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        loop {
            tick.tick().await;
            if !self.is_leader() {
                continue;
            }
            let (scan_id, from, to, seed, chunks) = {
                let sm = self.sm.lock().unwrap();
                match &sm.scan {
                    Some(s) if s.state == "running" => (s.id.clone(), s.from, s.to, s.seed, s.chunks()),
                    _ => continue,
                }
            };
            let mut a = self.assign.lock().unwrap();
            if a.scan_id != scan_id {
                // New scan: start with the first RAMP_BATCH live workers.
                let mut init: Vec<u32> = self.live();
                init.sort_unstable();
                init.truncate(RAMP_BATCH);
                *a = AssignState {
                    scan_id: scan_id.clone(), perm: permutation(from, to, seed),
                    assigned: HashMap::new(), resting: HashMap::new(),
                    done_pending: HashSet::new(), batch: Vec::new(), batch_scan: String::new(),
                    activated: init.into_iter().collect(),
                };
            }
            let now = Instant::now();
            let live: Vec<u32> = self.live();
            // Take back timed-out chunks; rest the slow worker.
            let mut back = Vec::new();
            {
                let sm = self.sm.lock().unwrap();
                let done = sm.scan.as_ref().map(|s| s.done.clone()).unwrap_or_default();
                for (&chunk, &(w, t)) in &a.assigned {
                    if done.get(chunk).copied().unwrap_or(false) {
                        back.push((chunk, w, 0));
                    } else if now.duration_since(t) > CHUNK_TIMEOUT {
                        back.push((chunk, w, 2));
                    }
                }
            }
            for (chunk, w, why) in back {
                a.assigned.remove(&chunk);
                if why == 2 {
                    println!("scan {scan_id}: replica {w}'s chunk {chunk} timed out, goes elsewhere; it rests");
                    a.resting.insert(w, now + REST);
                }
            }
            a.resting.retain(|_, until| *until > now);
            a.assigned.retain(|_, (w, _)| live.contains(w));
            let mut out: HashMap<u32, usize> = HashMap::new();
            for (w, _) in a.assigned.values() {
                *out.entry(*w).or_default() += 1;
            }
            let done = self.sm.lock().unwrap().scan.as_ref().map(|s| s.done.clone()).unwrap_or_default();
            let mut sends: Vec<(u32, Msg)> = Vec::new();
            'hand: loop {
                let mut any = false;
                for &w in &live {
                    // Only activated workers get chunks (ramp-up in batches).
                    if !a.activated.contains(&w) {
                        continue;
                    }
                    if a.resting.contains_key(&w) || out.get(&w).copied().unwrap_or(0) >= PER_WORKER {
                        continue;
                    }
                    let chunk = (0..chunks).find(|c| !done.get(*c).copied().unwrap_or(false) && !a.assigned.contains_key(c) && !a.done_pending.contains(c));
                    let Some(ch) = chunk else { break 'hand };
                    a.assigned.insert(ch, (w, now));
                    *out.entry(w).or_default() += 1;
                    any = true;
                    let s0 = ch * CHUNK as usize;
                    let s1 = (s0 + CHUNK as usize).min(a.perm.len());
                    let codes: Vec<String> = a.perm[s0..s1].iter().map(|c| format!("{c:05}")).collect();
                    if w == self.me {
                        self.take_codes(&scan_id, ch, codes);
                    } else {
                        sends.push((w, Msg::Assign { id: scan_id.clone(), chunk: ch as u32, codes }));
                    }
                }
                if !any {
                    break;
                }
            }
            // Ramp up: if every activated worker is saturated and chunks remain,
            // activate the next batch of live workers.
            let saturated = a.activated.iter().all(|w| {
                !live.contains(w) || a.resting.contains_key(w) || out.get(w).copied().unwrap_or(0) >= PER_WORKER
            });
            let chunks_left = (0..chunks).any(|c| !done.get(c).copied().unwrap_or(false) && !a.assigned.contains_key(&c) && !a.done_pending.contains(&c));
            if saturated && chunks_left {
                let mut more: Vec<u32> = live.iter().filter(|w| !a.activated.contains(w)).copied().collect();
                more.sort_unstable();
                for w in more.into_iter().take(RAMP_BATCH) {
                    println!("scan {scan_id}: activating replica {w} (ramp-up)");
                    a.activated.insert(w);
                }
            }
            drop(a);
            for (w, m) in sends {
                self.send_to(w, &m);
            }
        }
    }

    /// A code was found here: tell the leader until the log carries it (the scan stops on commit).
    pub async fn report_found(self: Arc<Self>, id: String, code: String) {
        println!("scan {id}: code {code} found here, reporting");
        loop {
            {
                let sm = self.sm.lock().unwrap();
                match &sm.scan {
                    Some(s) if s.id == id && s.state == "running" => {}
                    _ => return, // committed: every replica stops
                }
            }
            self.broadcast(&Msg::FoundCode { id: id.clone(), code: code.clone(), by: self.me });
            let _ = self.client_request(raft::ForwardCmd::Found { id: id.clone(), code: code.clone() }).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    // ---- telemetry ----

    /// Broadcast this replica's progress right now (for the page's replica cards).
    fn broadcast_prog(&self) {
        let id = self.worker.scan.lock().unwrap().to_string();
        let tried = self.worker.c.tried.load(Ordering::Relaxed);
        let queue = self.worker.queue.lock().unwrap().len();
        let last_error = self.worker.c.last_error.lock().unwrap().clone();
        let c = &self.worker.c;
        let stats = Stats {
            ok: c.ok_200.load(Ordering::Relaxed), e429: c.http_429.load(Ordering::Relaxed),
            e4: c.http_4xx.load(Ordering::Relaxed), e5: c.http_5xx.load(Ordering::Relaxed),
            net: c.net_errors.load(Ordering::Relaxed), timeouts: c.timeouts.load(Ordering::Relaxed),
            retrying: c.retrying.load(Ordering::Relaxed), pace: (self.pacer.rate() * 10.0).round() / 10.0,
        };
        // Instantaneous rate isn't known here; the loop fills it in.
        self.progs.lock().unwrap().insert(self.me, PeerProg { scan: id.clone(), tried, rate: 0.0, queue, last_error: last_error.clone(), stats: stats.clone(), at: Instant::now() });
        self.broadcast(&Msg::Prog { id, by: self.me, tried, rate: 0.0, queue, last_error, stats });
    }

    /// Workers the leader currently has chunks assigned to (so the page shows them working immediately).
    fn assigned_workers(&self) -> HashSet<u32> {
        let a = self.assign.lock().unwrap();
        a.assigned.values().map(|(w, _)| *w).collect()
    }

    /// Every 2 s: this replica's progress to every replica, for the page.
    pub async fn prog_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        let mut prev_tried = 0u64;
        let mut rate = 0.0f64;
        loop {
            tick.tick().await;
            let id = self.worker.scan.lock().unwrap().to_string();
            let active = self.worker.active.load(Ordering::SeqCst);
            let tried = self.worker.c.tried.load(Ordering::Relaxed);
            let inst = tried.saturating_sub(prev_tried) as f64 / 2.0;
            rate = if rate == 0.0 { inst } else { 0.6 * rate + 0.4 * inst };
            prev_tried = tried;
            if active || inst > 0.0 {
                let queue = self.worker.queue.lock().unwrap().len();
                let last_error = self.worker.c.last_error.lock().unwrap().clone();
                let r = (rate * 10.0).round() / 10.0;
                let c = &self.worker.c;
                let stats = Stats {
                    ok: c.ok_200.load(Ordering::Relaxed), e429: c.http_429.load(Ordering::Relaxed),
                    e4: c.http_4xx.load(Ordering::Relaxed), e5: c.http_5xx.load(Ordering::Relaxed),
                    net: c.net_errors.load(Ordering::Relaxed), timeouts: c.timeouts.load(Ordering::Relaxed),
                    retrying: c.retrying.load(Ordering::Relaxed), pace: (self.pacer.rate() * 10.0).round() / 10.0,
                };
                self.progs.lock().unwrap().insert(self.me, PeerProg { scan: id.clone(), tried, rate: r, queue, last_error: last_error.clone(), stats: stats.clone(), at: Instant::now() });
                self.broadcast(&Msg::Prog { id, by: self.me, tried, rate: r, queue, last_error, stats });
            }
        }
    }

    /// The scan as the page shows it.
    pub fn status(&self) -> serde_json::Value {
        let sm = self.sm.lock().unwrap();
        let r = self.raft.lock().unwrap();
        let peers = self.peers.lock().unwrap();
        let progs = self.progs.lock().unwrap();
        let now = Instant::now();
        let (running, state, found, valid, message, range, total, tried, sid) = match &sm.scan {
            Some(s) => {
                let tried: u64 = s.done.iter().enumerate().filter(|(_, d)| **d).map(|(k, _)| {
                    let total = (s.to - s.from) as u64;
                    let start = k as u64 * CHUNK as u64;
                    total.saturating_sub(start).min(CHUNK as u64)
                }).sum();
                let state = match s.state.as_str() {
                    "found" | "done" => "done",
                    other => other,
                };
                (s.state == "running", state.to_string(), s.state == "found",
                    if s.code.is_empty() { "—".to_string() } else { s.code.clone() },
                    s.message.clone(), s.range(), (s.to - s.from) as u64, tried, s.id.clone())
            }
            None => (false, "idle".into(), false, "—".into(), String::new(), "—".into(), 0, 0, String::new()),
        };
        let leader = r.leader;
        let term = r.term;
        let role = format!("{:?}", r.role).to_lowercase();
        let am_leader = r.role == raft::Role::Leader;
        drop(r);
        // If I'm the leader, I know exactly who has chunks right now (faster than telemetry).
        let assigned = if am_leader && running { self.assigned_workers() } else { HashSet::new() };
        // Codes handed to workers but not yet answered (fresh telemetry queues), and the codes not
        // handed out to anyone yet. The page shows `queued`; it used to go missing and break the UI.
        let in_queues: u64 = progs.values()
            .filter(|p| p.scan == sid && !sid.is_empty() && now.duration_since(p.at) < Duration::from_secs(6))
            .map(|p| p.queue as u64).sum();
        let queued = if running { total.saturating_sub(tried).saturating_sub(in_queues) } else { 0 };
        let replicas: Vec<serde_json::Value> = (1..=self.n)
            .map(|rk| {
                let up = rk == self.me || peers.contains_key(&rk);
                let p = progs.get(&rk).filter(|p| p.scan == sid && !sid.is_empty());
                let fresh = p.map(|p| now.duration_since(p.at) < Duration::from_secs(6)).unwrap_or(false);
                let busy = fresh && p.map(|p| p.queue > 0 || p.rate > 0.0).unwrap_or(false);
                let st = if !up {
                    "offline"
                } else if running && (busy || assigned.contains(&rk)) {
                    "working"
                } else {
                    "idle"
                };
                serde_json::json!({
                    "id": rk, "state": st, "leader": leader == Some(rk) && running,
                    "tried": p.map(|p| p.tried).unwrap_or(0), "buffer_len": p.map(|p| p.queue).unwrap_or(0),
                    "rate": if fresh { p.map(|p| p.rate).unwrap_or(0.0) } else { 0.0 },
                    "last_error": p.map(|p| p.last_error.clone()).unwrap_or_default(),
                    "stats": p.map(|p| p.stats.clone()).unwrap_or_default(),
                })
            })
            .collect();
        serde_json::json!({
            "running": running, "state": state, "found": found, "valid_code": valid, "message": message,
            "range": range, "tried": tried, "total": total, "queued": queued,
            "leader": leader, "term": term, "role": role, "me": self.me,
            "replica_count": self.n, "connected": peers.len() + 1, "quorum": self.quorum(),
            "history": sm.history, "last": sm.last, "replicas": replicas,
        })
    }
}
