//! The replicas as one: every replica keeps a WebSocket to every other one (see mesh.rs), and over it they agree
//! on the one scan that runs, share its progress, and stop it together.
//!
//!   - Starting a scan is a distributed lock: the replica it's started on proposes it, and it begins only once a
//!     majority of the fleet (REPLICA_COUNT / 2 + 1, itself included) grants it. A replica grants one proposal at a
//!     time, and none while another scan runs or within the cooldown of the last one, so two scans never run at
//!     once.
//!   - That replica coordinates: the range is shuffled (a permutation from the scan's seed), cut into chunks of
//!     CHUNK codes, and every replica connected gets PER_WORKER chunks at once, another each time one is done. A
//!     chunk not done in CHUNK_TIMEOUT goes to another replica, and the slow one rests for REST.
//!   - Every replica hears every finished chunk, every replica's progress, and the coordinator's heartbeat, so any
//!     replica's page shows the scan. If the coordinator goes silent for TAKEOVER_AFTER (its machine went, say), the
//!     lowest replica connected proposes itself with the next epoch, the same way, and goes on with the chunks not done.
//!   - A code found, or a stop from any replica, goes to every replica: they all stop.
//!   - The last scan (for the cooldown) and the codes found are passed on to every replica that connects, so they
//!     last as long as any replica is up.

use crate::{submit, Counters};
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::RateLimiter;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Codes per chunk: 2.5 s of one replica's work at 100/s.
pub const CHUNK: u32 = 250;
/// Chunks a replica holds at a time: the next one is queued while it works on one.
const PER_WORKER: usize = 2;
/// A chunk not done this long after it was handed out goes to another replica.
const CHUNK_TIMEOUT: Duration = Duration::from_secs(180);
/// And the replica that was slow gets nothing for this long (the site may be throttling it).
const REST: Duration = Duration::from_secs(180);
/// A coordinator not heard from this long is taken over.
const TAKEOVER_AFTER: Duration = Duration::from_secs(20);
/// A grant holds this long for its proposal (another proposal meanwhile is refused).
const GRANT_HOLD: Duration = Duration::from_secs(5);
/// How long a proposer waits for the answers.
const PROPOSE_WAIT: Duration = Duration::from_secs(4);
/// Found codes kept.
const HISTORY: usize = 20;

pub fn now_unix() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// A random u64 (the std hasher's random keys: no extra crate).
pub fn random_u64() -> u64 {
    let mut h = RandomState::new().build_hasher();
    h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0));
    h.finish()
}

/// The range from..to in a random order, the same on every replica for the same seed (Fisher-Yates over splitmix64).
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

/// The cooldown between scans: SCAN_COOLDOWN_HOURS (6 by default; 0 turns it off).
fn cooldown() -> u64 {
    let hours = std::env::var("SCAN_COOLDOWN_HOURS").ok().and_then(|h| h.parse::<f64>().ok()).unwrap_or(6.0);
    (hours * 3600.0) as u64
}

fn human_secs(s: u64) -> String {
    if s >= 3600 { format!("{}h {}m", s / 3600, (s % 3600) / 60) } else if s >= 60 { format!("{}m", s / 60) } else { format!("{s}s") }
}

/// "2026-10-05 20:30:00 UTC" for a unix time (civil-from-days, so no date crate is needed).
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

/// What names a scan: the same on every replica. epoch counts its coordinators (a takeover is the next one).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Header {
    pub id: String,
    pub from: u32,
    pub to: u32,
    pub seed: u64,
    pub chunk: u32,
    pub coord: u32,
    pub coord_boot: String,
    pub epoch: u32,
    pub started_at: u64,
}

impl Header {
    pub fn chunks(&self) -> usize {
        (self.to - self.from).div_ceil(self.chunk) as usize
    }
    pub fn range(&self) -> String {
        format!("{:05}..{:05}", self.from, self.to)
    }
    /// Codes in chunk k.
    fn chunk_len(&self, k: usize) -> u64 {
        let total = (self.to - self.from) as u64;
        let start = k as u64 * self.chunk as u64;
        total.saturating_sub(start).min(self.chunk as u64)
    }
}

/// The last scan started, for the cooldown.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Last {
    pub id: String,
    pub at: u64,
    pub from: u32,
    pub to: u32,
    pub status: String,
    pub code: String,
    pub by: u32,
    pub updated: u64,
}

/// A code found (the page's history).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Found {
    pub code: String,
    pub at: u64,
    pub found_at: String,
    pub range: String,
    pub replica: u32,
}

/// A scan as passed on to a replica that connects.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScanWire {
    pub h: Header,
    pub state: String,
    pub done: String,
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Msg {
    /// First thing on a connection, both ways.
    Hello { id: u32, boot: String, last: Option<Last>, history: Vec<Found>, scan: Option<ScanWire> },
    /// May this scan begin (or: may the sender coordinate it from now on, a higher epoch)?
    Propose { h: Header },
    Grant { id: String, epoch: u32, by: u32, ok: bool, why: String, done: String },
    /// A majority granted it: it runs.
    Begin { h: Header, done: String },
    /// It didn't get a majority: the grants are released.
    Release { id: String, epoch: u32 },
    /// The coordinator, every 2 s.
    Hb { h: Header, queued: u64, resting: Vec<u32>, done: String },
    /// Codes for one replica.
    Assign { id: String, epoch: u32, chunk: u32, codes: Vec<String> },
    /// A replica's progress, every 2 s while it works.
    Prog { id: String, by: u32, tried: u64, rate: f64, queue: usize, last_error: String },
    Done { id: String, chunk: u32, by: u32 },
    Found { id: String, code: String, by: u32 },
    Stop { id: Option<String>, by: u32 },
    End { id: String, state: String, code: String, message: String, by: u32 },
}

pub struct Scan {
    pub h: Header,
    /// running | found | stopped | done
    pub state: String,
    pub done: Vec<bool>,
    /// When its coordinator was last heard from.
    pub heard: Instant,
    pub code: String,
    pub message: String,
    pub queued: u64,
    pub resting: Vec<u32>,
}

/// The coordinator's own books (only on the replica that coordinates).
pub struct Coord {
    id: String,
    epoch: u32,
    perm: Vec<u32>,
    pending: VecDeque<usize>,
    assigned: HashMap<usize, (u32, Instant)>,
    resting: HashMap<u32, Instant>,
    last_hb: Instant,
}

pub struct PeerProg {
    pub scan: String,
    pub tried: u64,
    pub rate: f64,
    pub queue: usize,
    pub last_error: String,
    pub at: Instant,
}

#[derive(Default)]
pub struct Core {
    pub scan: Option<Scan>,
    pub coord: Option<Coord>,
    pub last: Option<Last>,
    pub history: Vec<Found>,
    /// The proposal this replica granted last: (scan id, epoch, who proposed it, when).
    pub grant: Option<(String, u32, u32, Instant)>,
    pub progs: HashMap<u32, PeerProg>,
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
    pub c: Counters,
}

pub struct Node {
    pub me: u32,
    pub boot: String,
    pub n: u32,
    pub client: Client,
    pub limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    pub peers: Mutex<HashMap<u32, Peer>>,
    pub core: Mutex<Core>,
    replies: Mutex<HashMap<String, mpsc::UnboundedSender<(u32, bool, String, String)>>>,
    pub w: Worker,
    serial: AtomicU64,
}

fn done_text(done: &[bool]) -> String {
    done.iter().map(|d| if *d { '1' } else { '0' }).collect()
}

fn merge_done(done: &mut [bool], text: &str) {
    for (d, c) in done.iter_mut().zip(text.chars()) {
        if c == '1' {
            *d = true;
        }
    }
}

impl Node {
    pub fn new(me: u32, n: u32, client: Client, limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>) -> Arc<Node> {
        Arc::new(Node {
            me,
            boot: format!("{:012x}", random_u64() & 0xffff_ffff_ffff),
            n,
            client,
            limiter,
            peers: Mutex::new(HashMap::new()),
            core: Mutex::new(Core::default()),
            replies: Mutex::new(HashMap::new()),
            w: Worker {
                queue: Mutex::new(VecDeque::new()),
                left: Mutex::new(HashMap::new()),
                scan: Mutex::new(Arc::from("")),
                active: AtomicBool::new(false),
                c: Counters::default(),
            },
            serial: AtomicU64::new(0),
        })
    }

    pub fn quorum(&self) -> usize {
        self.n as usize / 2 + 1
    }

    // ---- the connections (see mesh.rs) ----

    pub fn hello(&self) -> Msg {
        let c = self.core.lock().unwrap();
        Msg::Hello {
            id: self.me,
            boot: self.boot.clone(),
            last: c.last.clone(),
            history: c.history.clone(),
            scan: c.scan.as_ref().map(|s| ScanWire { h: s.h.clone(), state: s.state.clone(), done: done_text(&s.done), code: s.code.clone(), message: s.message.clone() }),
        }
    }

    /// A connection to replica id is up: its messages go out on tx from now on (a newer connection to the same
    /// replica replaces an older one, which then closes). Its serial, to unregister it by.
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

    pub fn broadcast(&self, m: &Msg) {
        let text = serde_json::to_string(m).unwrap();
        for p in self.peers.lock().unwrap().values() {
            let _ = p.tx.send(text.clone());
        }
    }

    fn send_to(&self, id: u32, m: &Msg) {
        if let Some(p) = self.peers.lock().unwrap().get(&id) {
            let _ = p.tx.send(serde_json::to_string(m).unwrap());
        }
    }

    pub fn handle(self: &Arc<Self>, from: u32, m: Msg) {
        match m {
            Msg::Hello { last, history, scan, .. } => self.merge_hello(last, history, scan),
            Msg::Propose { h } => {
                let (ok, why, done) = self.decide(&h, from);
                self.send_to(from, &Msg::Grant { id: h.id, epoch: h.epoch, by: self.me, ok, why, done });
            }
            Msg::Grant { id, epoch, by, ok, why, done } => {
                if let Some(tx) = self.replies.lock().unwrap().get(&format!("{id}/{epoch}")) {
                    let _ = tx.send((by, ok, why, done));
                }
            }
            Msg::Begin { h, done } => self.begin(h, &done),
            Msg::Release { id, epoch } => {
                let mut c = self.core.lock().unwrap();
                if matches!(&c.grant, Some((g, e, _, _)) if *g == id && *e == epoch) {
                    c.grant = None;
                }
            }
            Msg::Hb { h, queued, resting, done } => self.heartbeat(from, h, queued, resting, &done),
            Msg::Assign { id, epoch, chunk, codes } => {
                let ok = matches!(&self.core.lock().unwrap().scan, Some(s) if s.h.id == id && s.state == "running" && epoch >= s.h.epoch);
                if ok {
                    self.take_codes(&id, chunk as usize, codes);
                }
            }
            Msg::Prog { id, by, tried, rate, queue, last_error } => {
                self.core.lock().unwrap().progs.insert(by, PeerProg { scan: id, tried, rate, queue, last_error, at: Instant::now() });
            }
            Msg::Done { id, chunk, .. } => self.note_done(&id, chunk as usize),
            Msg::Found { id, code, by } => self.note_found(&id, code, by, false),
            Msg::Stop { id, by } => self.note_stop(id.as_deref(), by, false),
            Msg::End { id, state, code, message, by } => self.note_end(&id, &state, &code, &message, by),
        }
    }

    /// What a replica that connected knows: the newer last scan, the codes found, and the scan (a running one it
    /// knows of joins this replica to it; one it knows has ended ends it here).
    fn merge_hello(self: &Arc<Self>, last: Option<Last>, history: Vec<Found>, scan: Option<ScanWire>) {
        let mut begin = None;
        let mut end = None;
        {
            let mut c = self.core.lock().unwrap();
            if let Some(l) = last {
                let newer = match &c.last {
                    None => true,
                    Some(m) => l.at > m.at || (l.id == m.id && l.updated > m.updated),
                };
                if newer {
                    c.last = Some(l);
                }
            }
            for f in history {
                if !c.history.iter().any(|x| x.code == f.code && x.at == f.at) {
                    c.history.push(f);
                }
            }
            c.history.sort_by_key(|f| f.at);
            let extra = c.history.len().saturating_sub(HISTORY);
            c.history.drain(..extra);
            if let Some(sw) = scan {
                match &mut c.scan {
                    Some(s) if s.h.id == sw.h.id => {
                        merge_done(&mut s.done, &sw.done);
                        if sw.h.epoch > s.h.epoch {
                            s.h = sw.h.clone();
                            s.heard = Instant::now();
                        }
                        if sw.state != "running" && s.state == "running" {
                            end = Some(sw);
                        }
                    }
                    Some(s) if s.h.started_at >= sw.h.started_at => {}
                    _ => {
                        if sw.state == "running" {
                            begin = Some(sw);
                        } else {
                            c.scan = Some(Scan { done: vec![false; sw.h.chunks()], h: sw.h, state: sw.state, heard: Instant::now(), code: sw.code,
                                message: sw.message, queued: 0, resting: vec![] });
                        }
                    }
                }
            }
        }
        if let Some(sw) = begin {
            self.begin(sw.h, &sw.done);
        }
        if let Some(sw) = end {
            self.note_end(&sw.h.id, &sw.state, &sw.code, &sw.message, sw.h.coord);
        }
    }

    // ---- the lock: proposals, grants, a majority ----

    /// Whether this replica grants a proposal (and records the grant): (granted, why not, the chunks it knows are
    /// done of that scan).
    fn decide(&self, h: &Header, from: u32) -> (bool, String, String) {
        let mut c = self.core.lock().unwrap();
        let now = Instant::now();
        let mut known = String::new();
        match &c.scan {
            Some(s) if s.h.id == h.id => {
                if h.epoch <= s.h.epoch {
                    return (false, format!("epoch {} is already taken", s.h.epoch), String::new());
                }
                if s.state != "running" {
                    return (false, format!("that scan has {}", s.state), String::new());
                }
                if now.duration_since(s.heard) < TAKEOVER_AFTER / 2 && s.h.coord != h.coord {
                    return (false, format!("replica {} still coordinates it", s.h.coord), String::new());
                }
                known = done_text(&s.done);
            }
            Some(s) if s.state == "running" && now.duration_since(s.heard) < TAKEOVER_AFTER * 3 => {
                return (false, format!("{} is running, coordinated by replica {}", s.h.range(), s.h.coord), String::new());
            }
            _ => {
                if let Some(l) = &c.last {
                    let cd = cooldown();
                    if l.id != h.id && h.started_at < l.at + cd {
                        let age = h.started_at.saturating_sub(l.at);
                        return (false, format!("{:05}..{:05} was started {} ago ({}); the next scan is allowed in {}", l.from, l.to,
                            human_secs(age), l.status, human_secs(cd - age)), String::new());
                    }
                }
            }
        }
        if let Some((g, e, by, t)) = &c.grant {
            if now.duration_since(*t) < GRANT_HOLD && !(g == &h.id && *e == h.epoch) {
                return (false, format!("replica {by} is starting a scan"), String::new());
            }
        }
        c.grant = Some((h.id.clone(), h.epoch, from, now));
        (true, String::new(), known)
    }

    /// Proposes h to every replica connected; with a majority it begins (and this replica coordinates it).
    pub async fn propose(self: &Arc<Self>, h: Header) -> Result<(), String> {
        let key = format!("{}/{}", h.id, h.epoch);
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.replies.lock().unwrap().insert(key.clone(), tx);
        let (ok, why, mut done) = self.decide(&h, self.me);
        if !ok {
            self.replies.lock().unwrap().remove(&key);
            return Err(why);
        }
        let asked = self.peers.lock().unwrap().len();
        self.broadcast(&Msg::Propose { h: h.clone() });
        let deadline = Instant::now() + PROPOSE_WAIT;
        let (mut grants, mut answered) = (1usize, 0usize);
        let mut why_not: Vec<String> = Vec::new();
        while answered < asked {
            let left = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(left, rx.recv()).await {
                Ok(Some((by, ok, why, d))) => {
                    answered += 1;
                    if ok {
                        grants += 1;
                        let mut merged: Vec<bool> = done.chars().map(|c| c == '1').collect();
                        if merged.len() < d.len() {
                            merged.resize(d.len(), false);
                        }
                        merge_done(&mut merged, &d);
                        done = done_text(&merged);
                    } else {
                        why_not.push(format!("#{by}: {why}"));
                    }
                }
                _ => break,
            }
        }
        self.replies.lock().unwrap().remove(&key);
        if grants >= self.quorum() {
            self.begin(h.clone(), &done);
            self.broadcast(&Msg::Begin { h: h.clone(), done: done.clone() });
            println!("scan {} ({}): {} by replica {} (epoch {}), {grants} of {} granted", h.id, h.range(),
                if h.epoch == 1 { "started" } else { "taken over" }, self.me, h.epoch, self.n);
            return Ok(());
        }
        self.broadcast(&Msg::Release { id: h.id.clone(), epoch: h.epoch });
        {
            let mut c = self.core.lock().unwrap();
            if matches!(&c.grant, Some((g, e, _, _)) if *g == h.id && *e == h.epoch) {
                c.grant = None;
            }
        }
        // The most common refusal says it best.
        let mut counts: HashMap<String, usize> = HashMap::new();
        for w in &why_not {
            *counts.entry(w.split_once(": ").map(|x| x.1).unwrap_or(w).to_string()).or_default() += 1;
        }
        let top = counts.into_iter().max_by_key(|x| x.1).map(|x| x.0);
        Err(match top {
            Some(why) => why,
            None => format!("only {grants} of {} replicas could be reached (need {}); try again in a moment", self.n, self.quorum()),
        })
    }

    /// Starts a scan of from..to from this replica.
    pub async fn start(self: &Arc<Self>, from: u32, to: u32) -> Result<String, String> {
        let h = Header {
            id: format!("{:08x}", random_u64() as u32),
            from,
            to,
            seed: random_u64(),
            chunk: CHUNK,
            coord: self.me,
            coord_boot: self.boot.clone(),
            epoch: 1,
            started_at: now_unix(),
        };
        let range = h.range();
        self.propose(h).await?;
        Ok(format!("started {range} over {} replicas", self.live().len()))
    }

    // ---- a scan's life ----

    /// A scan granted by a majority runs: this replica works for it (and coordinates it, if it's this one's).
    fn begin(self: &Arc<Self>, h: Header, done_in: &str) {
        let coordinate;
        {
            let mut c = self.core.lock().unwrap();
            if let Some(s) = &c.scan {
                if s.h.id == h.id && (s.h.epoch > h.epoch || s.state != "running") {
                    return;
                }
            }
            let mut done = match &c.scan {
                Some(s) if s.h.id == h.id => s.done.clone(),
                _ => vec![false; h.chunks()],
            };
            merge_done(&mut done, done_in);
            coordinate = h.coord == self.me && h.coord_boot == self.boot;
            if !coordinate || c.coord.as_ref().map(|k| k.id != h.id || k.epoch != h.epoch).unwrap_or(false) {
                c.coord = None;
            }
            if coordinate && c.coord.is_none() {
                let perm = permutation(h.from, h.to, h.seed);
                let pending = (0..h.chunks()).filter(|k| !done[*k]).collect();
                c.coord = Some(Coord { id: h.id.clone(), epoch: h.epoch, perm, pending, assigned: HashMap::new(), resting: HashMap::new(),
                    last_hb: Instant::now() - Duration::from_secs(10) });
            }
            let newer = c.last.as_ref().map(|l| l.id == h.id || h.started_at >= l.at).unwrap_or(true);
            if newer && c.last.as_ref().map(|l| l.id != h.id).unwrap_or(true) {
                c.last = Some(Last { id: h.id.clone(), at: h.started_at, from: h.from, to: h.to, status: "running".into(), code: String::new(),
                    by: h.coord, updated: now_unix() });
            }
            if matches!(&c.grant, Some((g, _, _, _)) if *g == h.id) {
                c.grant = None;
            }
            c.scan = Some(Scan { h: h.clone(), state: "running".into(), done, heard: Instant::now(), code: String::new(), message: String::new(),
                queued: 0, resting: vec![] });
        }
        self.join_work(&h.id);
        if coordinate {
            let node = self.clone();
            let (id, epoch) = (h.id.clone(), h.epoch);
            tokio::spawn(async move { node.coordinate(id, epoch).await });
        }
    }

    /// This replica's worker serves scan id from now on (a new scan: its queue and counters start afresh).
    fn join_work(&self, id: &str) {
        let mut scan = self.w.scan.lock().unwrap();
        if &**scan != id {
            *scan = Arc::from(id);
            self.w.queue.lock().unwrap().clear();
            self.w.left.lock().unwrap().clear();
            self.w.c.reset();
        }
        self.w.active.store(true, Ordering::SeqCst);
    }

    /// The worker stops: what's queued is dropped.
    fn stop_work(&self) {
        self.w.active.store(false, Ordering::SeqCst);
        self.w.queue.lock().unwrap().clear();
        self.w.left.lock().unwrap().clear();
    }

    fn heartbeat(self: &Arc<Self>, from: u32, h: Header, queued: u64, resting: Vec<u32>, done: &str) {
        let mut begin = false;
        let mut tell = None;
        {
            let mut c = self.core.lock().unwrap();
            match &mut c.scan {
                Some(s) if s.h.id == h.id => {
                    if s.state != "running" {
                        // A coordinator that missed the end: told again.
                        tell = Some(Msg::End { id: s.h.id.clone(), state: s.state.clone(), code: s.code.clone(), message: s.message.clone(), by: self.me });
                    } else if h.epoch >= s.h.epoch {
                        if h.epoch > s.h.epoch {
                            s.h = h.clone();
                        }
                        s.heard = Instant::now();
                        s.queued = queued;
                        s.resting = resting;
                        merge_done(&mut s.done, done);
                        if h.coord != self.me && c.coord.is_some() {
                            c.coord = None;
                        }
                    }
                }
                Some(s) if s.h.started_at > h.started_at => {}
                _ => begin = true,
            }
        }
        if let Some(m) = tell {
            self.send_to(from, &m);
        }
        if begin {
            self.begin(h, done);
        }
    }

    pub fn take_codes(&self, id: &str, chunk: usize, codes: Vec<String>) {
        if &**self.w.scan.lock().unwrap() != id || !self.w.active.load(Ordering::SeqCst) {
            return;
        }
        let mut left = self.w.left.lock().unwrap();
        if left.contains_key(&chunk) || codes.is_empty() {
            return;
        }
        left.insert(chunk, codes.len());
        let sid: Arc<str> = Arc::from(id);
        self.w.queue.lock().unwrap().extend(codes.into_iter().map(|c| (sid.clone(), chunk, c)));
    }

    /// A code of chunk k was tried here: the chunk is done once its last one is.
    pub fn tried(self: &Arc<Self>, id: &Arc<str>, chunk: usize) {
        if &*self.w.scan.lock().unwrap() != id {
            return;
        }
        let finished = {
            let mut left = self.w.left.lock().unwrap();
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
        if finished && self.w.active.load(Ordering::SeqCst) {
            self.note_done(id, chunk);
            self.broadcast(&Msg::Done { id: id.to_string(), chunk: chunk as u32, by: self.me });
        }
    }

    fn note_done(&self, id: &str, chunk: usize) {
        let mut c = self.core.lock().unwrap();
        let Core { scan, coord, .. } = &mut *c;
        if let Some(s) = scan {
            if s.h.id == id && chunk < s.done.len() {
                s.done[chunk] = true;
            }
        }
        if let Some(k) = coord {
            if k.id == id {
                k.assigned.remove(&chunk);
            }
        }
    }

    /// A code was found (here, mine: told to every replica).
    pub fn note_found(&self, id: &str, code: String, by: u32, mine: bool) {
        let coordinating;
        {
            let mut c = self.core.lock().unwrap();
            let Some(s) = &mut c.scan else { return };
            if s.h.id != id || s.state != "running" {
                return;
            }
            s.state = "found".into();
            s.code = code.clone();
            s.message = format!("code {code} found by replica {by}");
            let range = s.h.range();
            coordinating = c.coord.take().is_some();
            let at = now_unix();
            if !c.history.iter().any(|f| f.code == code) {
                c.history.push(Found { code: code.clone(), at, found_at: utc_text(at), range, replica: by });
                let extra = c.history.len().saturating_sub(HISTORY);
                c.history.drain(..extra);
            }
            if let Some(l) = &mut c.last {
                if l.id == id {
                    l.status = "found".into();
                    l.code = code.clone();
                    l.updated = at;
                }
            }
        }
        self.stop_work();
        println!("scan {id}: code {code} found by replica {by}");
        if mine {
            self.broadcast(&Msg::Found { id: id.to_string(), code: code.clone(), by });
        }
        if coordinating {
            self.broadcast(&Msg::End { id: id.to_string(), state: "found".into(), code: code.clone(), message: format!("code {code} found by replica {by}"), by: self.me });
        }
    }

    /// A stop (of scan id, or whatever runs), from replica by (here, mine: told to every replica).
    pub fn note_stop(&self, id: Option<&str>, by: u32, mine: bool) {
        let stopped = {
            let mut c = self.core.lock().unwrap();
            match &mut c.scan {
                Some(s) if s.state == "running" && id.map(|i| i == s.h.id).unwrap_or(true) => {
                    s.state = "stopped".into();
                    s.message = format!("stopped from replica {by}");
                    let sid = s.h.id.clone();
                    c.coord = None;
                    if let Some(l) = &mut c.last {
                        if l.id == sid {
                            l.status = "stopped".into();
                            l.updated = now_unix();
                        }
                    }
                    Some(sid)
                }
                _ => None,
            }
        };
        self.stop_work();
        if let Some(sid) = &stopped {
            println!("scan {sid}: stopped from replica {by}");
        }
        if mine {
            self.broadcast(&Msg::Stop { id: stopped.or(id.map(String::from)), by });
        }
    }

    fn note_end(&self, id: &str, state: &str, code: &str, message: &str, by: u32) {
        {
            let mut c = self.core.lock().unwrap();
            let Some(s) = &mut c.scan else { return };
            if s.h.id != id || s.state != "running" {
                return;
            }
            s.state = state.to_string();
            s.code = code.to_string();
            s.message = message.to_string();
            let range = s.h.range();
            c.coord = None;
            let at = now_unix();
            if state == "found" && !code.is_empty() && !c.history.iter().any(|f| f.code == code) {
                c.history.push(Found { code: code.to_string(), at, found_at: utc_text(at), range, replica: by });
            }
            if let Some(l) = &mut c.last {
                if l.id == id {
                    l.status = state.to_string();
                    l.code = code.to_string();
                    l.updated = at;
                }
            }
        }
        self.stop_work();
    }

    // ---- coordinating ----

    /// Hands the scan's chunks out to every replica connected, takes back what's lost or slow, and says how it's
    /// going every 2 s, while this replica coordinates scan id at this epoch.
    async fn coordinate(self: Arc<Self>, id: String, epoch: u32) {
        println!("scan {id}: coordinating (epoch {epoch})");
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        loop {
            tick.tick().await;
            let live = self.live();
            let mut sends: Vec<(u32, Msg)> = Vec::new();
            let mut mine: Vec<(usize, Vec<String>)> = Vec::new();
            let mut hb = None;
            let mut complete = false;
            {
                let mut c = self.core.lock().unwrap();
                let Core { scan, coord, .. } = &mut *c;
                let (Some(s), Some(k)) = (scan.as_mut(), coord.as_mut()) else { break };
                if s.h.id != id || k.id != id || k.epoch != epoch || s.state != "running" {
                    break;
                }
                let now = Instant::now();
                let mut back = Vec::new();
                for (&chunk, &(w, t)) in &k.assigned {
                    if s.done[chunk] {
                        back.push((chunk, w, 0));
                    } else if !live.contains(&w) {
                        back.push((chunk, w, 1));
                    } else if now.duration_since(t) > CHUNK_TIMEOUT {
                        back.push((chunk, w, 2));
                    }
                }
                for (chunk, w, why) in back {
                    k.assigned.remove(&chunk);
                    if why > 0 {
                        k.pending.push_front(chunk);
                    }
                    if why == 2 {
                        println!("scan {id}: replica {w} is slow, its chunk goes elsewhere and it rests");
                        k.resting.insert(w, now + REST);
                    }
                }
                k.resting.retain(|_, until| *until > now);
                let mut out: HashMap<u32, usize> = HashMap::new();
                for (w, _) in k.assigned.values() {
                    *out.entry(*w).or_default() += 1;
                }
                // Round by round, a chunk to every replica that has room, until none has or nothing's left.
                'hand: loop {
                    let mut any = false;
                    for &w in &live {
                        if k.resting.contains_key(&w) || out.get(&w).copied().unwrap_or(0) >= PER_WORKER {
                            continue;
                        }
                        let chunk = loop {
                            match k.pending.pop_front() {
                                None => break 'hand,
                                Some(ch) if s.done[ch] => continue,
                                Some(ch) => break ch,
                            }
                        };
                        k.assigned.insert(chunk, (w, now));
                        *out.entry(w).or_default() += 1;
                        any = true;
                        let a = chunk * s.h.chunk as usize;
                        let b = (a + s.h.chunk as usize).min(k.perm.len());
                        let codes: Vec<String> = k.perm[a..b].iter().map(|c| format!("{c:05}")).collect();
                        if w == self.me {
                            mine.push((chunk, codes));
                        } else {
                            sends.push((w, Msg::Assign { id: id.clone(), epoch, chunk: chunk as u32, codes }));
                        }
                    }
                    if !any {
                        break;
                    }
                }
                if k.pending.iter().all(|ch| s.done[*ch]) && k.assigned.is_empty() && s.done.iter().all(|d| *d) {
                    complete = true;
                }
                if now.duration_since(k.last_hb) >= Duration::from_secs(2) {
                    k.last_hb = now;
                    let queued: u64 = k.pending.iter().filter(|ch| !s.done[**ch]).map(|ch| s.h.chunk_len(*ch)).sum();
                    let resting: Vec<u32> = k.resting.keys().copied().collect();
                    s.heard = now;
                    s.queued = queued;
                    s.resting = resting.clone();
                    hb = Some(Msg::Hb { h: s.h.clone(), queued, resting, done: done_text(&s.done) });
                }
            }
            for (w, m) in sends {
                self.send_to(w, &m);
            }
            for (chunk, codes) in mine {
                self.take_codes(&id, chunk, codes);
            }
            if let Some(m) = hb {
                self.broadcast(&m);
            }
            if complete {
                let message = "scan complete, no code found".to_string();
                self.note_end(&id, "done", "", &message, self.me);
                self.broadcast(&Msg::End { id: id.clone(), state: "done".into(), code: String::new(), message, by: self.me });
                println!("scan {id}: complete, no code found");
                break;
            }
        }
        println!("scan {id}: no longer coordinating (epoch {epoch})");
    }

    /// Every 2 s: this replica's progress to every replica while it works; and a running scan whose coordinator
    /// went silent is taken over by the lowest replica connected (this one, if it's that).
    pub async fn watch(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        let mut prev_tried = 0u64;
        let mut rate = 0.0f64;
        loop {
            tick.tick().await;
            let id = self.w.scan.lock().unwrap().to_string();
            let active = self.w.active.load(Ordering::SeqCst);
            let tried = self.w.c.tried.load(Ordering::Relaxed);
            let inst = tried.saturating_sub(prev_tried) as f64 / 2.0;
            rate = if rate == 0.0 { inst } else { 0.6 * rate + 0.4 * inst };
            prev_tried = tried;
            if active || inst > 0.0 {
                let queue = self.w.queue.lock().unwrap().len();
                let last_error = self.w.c.last_error.lock().unwrap().clone();
                let r = (rate * 10.0).round() / 10.0;
                self.core.lock().unwrap().progs.insert(self.me, PeerProg { scan: id.clone(), tried, rate: r, queue, last_error: last_error.clone(), at: Instant::now() });
                self.broadcast(&Msg::Prog { id, by: self.me, tried, rate: r, queue, last_error });
            }
            let takeover = {
                let c = self.core.lock().unwrap();
                match &c.scan {
                    Some(s) if s.state == "running" && s.heard.elapsed() > TAKEOVER_AFTER && c.coord.is_none() => {
                        let lowest = self.live().into_iter().find(|r| *r != s.h.coord || s.h.coord == self.me);
                        (lowest == Some(self.me)).then(|| s.h.clone())
                    }
                    _ => None,
                }
            };
            if let Some(h) = takeover {
                println!("scan {}: replica {} silent for {}s, taking over", h.id, h.coord, TAKEOVER_AFTER.as_secs());
                let next = Header { coord: self.me, coord_boot: self.boot.clone(), epoch: h.epoch + 1, ..h };
                if let Err(e) = self.propose(next).await {
                    println!("takeover refused: {e}");
                }
            }
        }
    }

    /// The scan as the page shows it.
    pub fn status(&self) -> serde_json::Value {
        let c = self.core.lock().unwrap();
        let peers = self.peers.lock().unwrap();
        let now = Instant::now();
        let (running, state, found, valid, message, range, total, tried, queued, coord, resting, sid) = match &c.scan {
            Some(s) => {
                let tried: u64 = s.done.iter().enumerate().filter(|(_, d)| **d).map(|(k, _)| s.h.chunk_len(k)).sum();
                let state = match s.state.as_str() {
                    "found" | "done" => "done",
                    other => other,
                };
                (s.state == "running", state.to_string(), s.state == "found", if s.code.is_empty() { "—".to_string() } else { s.code.clone() },
                    s.message.clone(), s.h.range(), (s.h.to - s.h.from) as u64, tried, s.queued, Some(s.h.coord), s.resting.clone(), s.h.id.clone())
            }
            None => (false, "idle".into(), false, "—".into(), String::new(), "—".into(), 0, 0, 0, None, vec![], String::new()),
        };
        let replicas: Vec<serde_json::Value> = (1..=self.n)
            .map(|r| {
                let up = r == self.me || peers.contains_key(&r);
                let p = c.progs.get(&r).filter(|p| p.scan == sid && !sid.is_empty());
                let fresh = p.map(|p| now.duration_since(p.at) < Duration::from_secs(6)).unwrap_or(false);
                let st = if !up {
                    "offline"
                } else if resting.contains(&r) {
                    "cooldown"
                } else if running && fresh && p.map(|p| p.queue > 0 || p.rate > 0.0).unwrap_or(false) {
                    "working"
                } else {
                    "idle"
                };
                serde_json::json!({
                    "id": r, "state": st, "coordinator": coord == Some(r) && running,
                    "tried": p.map(|p| p.tried).unwrap_or(0), "buffer_len": p.map(|p| p.queue).unwrap_or(0),
                    "rate": if fresh { p.map(|p| p.rate).unwrap_or(0.0) } else { 0.0 }, "last_error": p.map(|p| p.last_error.clone()).unwrap_or_default(),
                })
            })
            .collect();
        serde_json::json!({
            "running": running, "state": state, "found": found, "valid_code": valid, "message": message, "range": range,
            "tried": tried, "total": total, "queued": queued, "coordinator": coord, "replica_count": self.n,
            "connected": peers.len() + 1, "quorum": self.quorum(), "me": self.me,
            "history": c.history, "last": c.last, "replicas": replicas,
        })
    }
}

/// Every BUFFER_WORKERS of these drain the queue through the rate limiter.
pub async fn work(node: Arc<Node>) {
    loop {
        let item = node.w.queue.lock().unwrap().pop_front();
        let Some((sid, chunk, code)) = item else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        node.limiter.until_ready().await;
        if !node.w.active.load(Ordering::SeqCst) || *node.w.scan.lock().unwrap() != sid {
            continue;
        }
        let hit = submit(&node.client, &node.w.c, &node.w.active, &code).await;
        node.w.c.tried.fetch_add(1, Ordering::Relaxed);
        if hit {
            node.note_found(&sid, code, node.me, true);
            continue;
        }
        node.tried(&sid, chunk);
    }
}
